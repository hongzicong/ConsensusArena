package bodega

// Recovery coordination, evidence collection, and log-gap repair.
import (
	"slices"
	"time"

	"github.com/hongzicong/ConsensusArena/replica/defs"
)

func (e *engine) proposeRoster(leader int, responders uint64, now time.Time) {
	ranges := e.current.Ranges
	if e.current.Ballot == 0 {
		ranges = e.opt.Ranges
	}
	e.proposeRosterRanges(leader, responders, ranges, now)
}

func (e *engine) validRoster(r roster) bool {
	return r.Ballot > 0 && r.Leader >= 0 && r.Leader < e.n && r.Responders&bit(r.Leader) != 0 && r.Responders>>uint(e.n) == 0 && defs.ValidBodegaRanges(r.Ranges, e.n)
}

func (e *engine) observe(r roster, now time.Time) {
	if !e.validRoster(r) || r.Ballot <= e.current.Ballot || r.Ballot <= e.pending.Ballot {
		return
	}
	r.Ranges = slices.Clone(r.Ranges)
	e.pending = r
	// Stop read authority immediately; do not change the Paxos promise yet.
	e.incoming = map[int]grant{}
	e.requests = map[uint64]time.Time{}
	e.prepared = false
	e.revokeLeases(now)
	e.install(now)
}

func (e *engine) install(now time.Time) {
	if e.pending.Ballot == 0 {
		return
	}
	for _, until := range e.outgoing {
		if now.Before(until) {
			return
		}
	}
	e.current = e.pending
	e.pending = roster{}
	// A new roster starts a fresh failure-detection window. This does not
	// renew grants or authorize reads; those still require actual lease replies.
	e.tracePeerAges("BODEGA_TIMER_RESET", now, 0)
	for i := range e.seen {
		e.refreshPeer(i, now)
	}
	e.threshold = e.high
	e.outgoing = map[int]time.Time{}
	e.incoming = map[int]grant{}
	e.snapshots = map[int]*snapshot{}
	e.ownSnapshot = nil
	e.snapshotChunks = nil
	e.snapshotCursor = map[int]int{}
	e.snapshotTaken = false
	e.snapshotStart, e.prepareStart = 0, e.prefix+1
	e.prepareSince = now
	e.votes = map[uint64]uint64{}
	// Executed history is already irrevocable; only the suffix needs a new vote.
	e.acceptedPrefix, e.readPrefix = e.prefix, e.prefix
	e.acceptedProgress = make([]uint64, e.n)
	e.acceptedProgress[e.id] = e.prefix
	e.noticePrefix = 0
	e.noticeHigh = 0
	e.noticeSlots = map[uint64]bool{}
	e.lastRepair = time.Time{}
	e.stats.RosterChanges++
	// An old held slot can be overwritten during recovery: re-route these reads.
	for id, h := range e.held {
		e.enqueue(h.Request)
		delete(e.held, id)
	}
	e.holdSlots = nil
	e.fastSlots = nil
	e.fastQueued = map[uint64]bool{}
	e.holdWaiters = map[uint64][]requestID{}
	e.holdCounts = map[uint64]int{}
	for _, r := range e.batch {
		e.enqueue(r)
	}
	e.batch, e.batchBytes = nil, 0
	e.batchIDs = map[requestID]bool{}
	if e.current.Leader == e.id {
		e.startPrepare(now)
	}
}

func (e *engine) startPrepare(now time.Time) {
	e.takeSnapshot(e.prepareStart)
	e.addSnapshot(e.id, e.prepareStart, 0, 1, e.ownSnapshot, now)
	e.broadcast(message{Kind: prepare, PrepareStart: e.prepareStart})
}

// Freeze accepted evidence once per ballot and requested suffix, like upstream's
// trigger_slot. Executed history is not part of a new leader's Prepare.
func (e *engine) takeSnapshot(start uint64) bool {
	if start == 0 || start <= e.compacted {
		return false
	}
	if e.snapshotTaken {
		return start == e.snapshotStart
	}
	e.snapshotTaken, e.snapshotStart = true, start
	e.ownSnapshot = make([]entry, 0)
	for s := start; s <= e.high; s++ {
		if v, ok := e.log[s]; ok {
			e.ownSnapshot = append(e.ownSnapshot, v)
		}
	}
	first, size := 0, 0
	for i, v := range e.ownSnapshot {
		if i > first && (i-first >= 64 || size+entryBytes(v) > maxBatchBytes) {
			e.snapshotChunks = append(e.snapshotChunks, e.ownSnapshot[first:i])
			first, size = i, 0
		}
		size += entryBytes(v)
	}
	e.snapshotChunks = append(e.snapshotChunks, e.ownSnapshot[first:])
	return true
}

func (e *engine) sendSnapshot(to int, start uint64) {
	if !e.takeSnapshot(start) {
		return
	}
	if e.snapshotCursor == nil {
		e.snapshotCursor = map[int]int{}
	}
	if e.snapshotCursor[to] == len(e.snapshotChunks) {
		e.snapshotCursor[to] = 0 // retry a lost reply, not a transfer pacing credit
	}
	e.flushPromises(to)
}

// The writer wakes this pump as queue space becomes available. Heartbeats only
// retry lost requests/replies; they impose no slots-per-second transfer budget.
func (e *engine) flushPromises(to int) {
	if !e.active() || to != e.current.Leader || !e.snapshotTaken {
		return
	}
	for p := e.snapshotCursor[to]; p < len(e.snapshotChunks); p++ {
		m := message{Kind: promise, PrepareStart: e.snapshotStart,
			Part: p, Parts: len(e.snapshotChunks), Entries: e.snapshotChunks[p]}
		if !e.emit(to, m) {
			return
		}
		e.snapshotCursor[to] = p + 1
	}
}

func (e *engine) addSnapshot(from int, start uint64, part, parts int, entries []entry, now time.Time) {
	if e.prepared || start != e.prepareStart || parts <= 0 || part < 0 || part >= parts {
		return
	}
	for _, v := range entries {
		if v.Slot < start || v.Ballot > e.current.Ballot {
			return
		}
	}
	s := e.snapshots[from]
	if s == nil {
		s = &snapshot{parts, map[int][]entry{}}
		e.snapshots[from] = s
	}
	if s.Parts != parts {
		return
	}
	s.Chunks[part] = entries
	count := 0
	for _, s := range e.snapshots {
		if len(s.Chunks) == s.Parts {
			count++
		}
	}
	if count < e.majority {
		return
	}
	selected := map[uint64]entry{}
	high := start - 1
	for _, s := range e.snapshots {
		if len(s.Chunks) != s.Parts {
			continue
		}
		for _, vs := range s.Chunks {
			for _, v := range vs {
				if old, ok := selected[v.Slot]; !ok || v.Ballot > old.Ballot {
					selected[v.Slot] = v
				}
				high = maxSlot(high, v.Slot)
			}
		}
	}
	e.next = high
	e.inflight = map[requestID]uint64{}
	e.prepared = true
	for slot := start; slot <= high; slot++ {
		v, ok := selected[slot]
		if !ok {
			v = entry{Slot: slot, Request: request{Origin: -1}}
		}
		v.Ballot, v.ReadFresh = e.current.Ballot, false
		e.acceptEntry(v)
		for _, r := range v.requests() {
			e.inflight[r.id()] = slot
		}
		e.votes[slot] = bit(e.id)
		// Already committed entries are fixed; others must cover the new roster.
		e.stats.RecoveredSlots++
	}
	e.stats.PrepareEntries += high - start + 1
	e.stats.PrepareCompleted++
	e.stats.PrepareNanos += uint64(now.Sub(e.prepareSince))
	e.reindex()
	for slot := start; slot <= high; slot++ {
		e.broadcast(message{Kind: accept, Entry: e.log[slot]})
		if e.committed[slot] {
			e.broadcast(message{Kind: commit, CommitSlot: slot, CommitPrefix: e.prefix})
		}
	}
	for p := 0; p < e.n; p++ {
		if p != e.id {
			e.repairPeer(p)
		}
	}
	// Values remain in the log; release the quorum's duplicate payloads.
	e.snapshots = map[int]*snapshot{}
}

func (e *engine) proposeRosterRanges(leader int, responders uint64, ranges []defs.BodegaResponderRange, now time.Time) {
	b := maxSlot(e.current.Ballot, e.pending.Ballot)
	r := roster{Ballot: (b/uint64(e.n)+1)*uint64(e.n) + uint64(e.id) + 1,
		Leader: leader, Responders: responders | bit(leader), Ranges: slices.Clone(ranges)}
	e.observe(r, now)
	e.broadcast(message{Kind: heartbeat})
}

func (e *engine) proposeFilteredRoster(leader int, healthy uint64, now time.Time) {
	e.tracePeerAges("BODEGA_ROSTER_FILTER", now, healthy)
	mask, ranges := e.current.Responders, e.current.Ranges
	if e.current.Ballot == 0 {
		mask, ranges = e.opt.Responders, e.opt.Ranges
	}
	ranges = slices.Clone(ranges)
	for i := range ranges {
		ranges[i].Responders &= healthy
	}
	e.proposeRosterRanges(leader, mask&healthy, ranges, now)
}

// Log-gap repair and replica catch-up.
const repairWindow = 512

func (e *engine) requestCommitRepair(now time.Time) {
	start := maxSlot(e.prefix, e.acceptedPrefix) + 1
	if !e.active() || e.id == e.current.Leader || start > e.noticeHigh {
		return
	}
	if !e.lastRepair.IsZero() && now.Sub(e.lastRepair) < e.opt.Heartbeat {
		return
	}
	e.lastRepair = now
	e.stats.CommitRepairRequests++
	e.emit(e.current.Leader, message{Kind: repairRequest, RepairStart: start})
}

func (e *engine) repairPeer(peer int) {
	start := minSlot(e.progress[peer], e.prefix) + 1
	end := e.sendCommitRepair(peer, start)
	// A lagging follower must not prevent solicitation of its vote for the
	// leader's first unexecuted slot, even when that slot is far ahead.
	if end <= e.prefix {
		e.sendCommitRepair(peer, e.prefix+1)
	}
	// Preserve the stronger current-ballot accepted-prefix evidence used by
	// out-of-order reads, but repair it separately from execution progress.
	if e.acceptedProgress[peer]+1 < start {
		e.sendRepairWindow(peer, e.acceptedProgress[peer]+1, 64)
	}
}

func (e *engine) sendCommitRepair(peer int, start uint64) uint64 {
	return e.sendRepairWindow(peer, start, repairWindow)
}

func (e *engine) sendRepairWindow(peer int, start, window uint64) uint64 {
	if start == 0 || start > e.next {
		return start
	}
	end := e.next
	if end-start >= window {
		end = start + window - 1
	}
	slot, size := start, 0
	for ; slot <= end; slot++ {
		v, ok := e.log[slot]
		if !ok {
			continue
		}
		// History before the Prepare suffix is already chosen, and must not be
		// re-voted merely to catch up a lagging follower in a newer ballot.
		if slot <= e.prefix && v.Ballot != e.current.Ballot {
			bytes := entryBytes(v)
			if size > 0 && size+bytes > maxBatchBytes {
				break
			}
			size += bytes
			e.stats.CommitRepairEntries++
			e.emit(peer, message{Kind: committedEntry, Entry: v})
			continue
		}
		if v.Ballot != e.current.Ballot {
			continue
		}
		// An acknowledged value survives for this ballot in the crash-stop
		// model. Do not flood the data queue with copies of it on every tick.
		if e.votes[slot]&bit(peer) == 0 {
			bytes := entryBytes(v)
			if size > 0 && size+bytes > maxBatchBytes {
				break
			}
			size += bytes
			e.stats.CommitRepairEntries++
			e.emit(peer, message{Kind: accept, Entry: v})
		}
		if e.committed[slot] && slot > e.prefix {
			e.emit(peer, message{Kind: commit, CommitSlot: slot, CommitPrefix: e.prefix})
		}
	}
	e.emit(peer, message{Kind: commit, CommitPrefix: e.prefix})
	return slot
}

func (e *engine) learnCommittedEntry(v entry) {
	if v.Slot <= e.prefix {
		return
	}
	// The installed leader supplies a chosen value from its executed history.
	// Give it current-ballot provenance so compact commit notices can repair gaps.
	v.Ballot, v.ReadFresh = e.current.Ballot, false
	e.acceptEntry(v)
	e.markCommitted(v.Slot)
	e.apply()
}
