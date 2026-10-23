package bodega

// Recovery coordination, evidence collection, and log-gap repair.
import (
	"slices"
	"time"

	"github.com/hongzicong/ConsensusArena/replica/defs"
	"github.com/hongzicong/ConsensusArena/replicaset"
)

func (c *core) handleLeaderCall(now time.Time) int {
	leader := c.current.Leader
	if c.current.Ballot == 0 || !c.peerHealthy(leader, now) {
		mask := (uint64(1) << uint(c.n)) - 1
		for p := range c.seen {
			if !c.peerHealthy(p, now) {
				mask &^= bit(p)
			}
		}
		c.proposeFilteredRoster(c.id, mask, now)
		leader = c.id
	}
	return leader
}

func (c *core) proposeRoster(leader int, responders uint64, now time.Time) {
	ranges := c.current.Ranges
	if c.current.Ballot == 0 {
		ranges = c.opt.ranges
	}
	c.proposeRosterRanges(leader, responders, ranges, now)
}

func (c *core) validRoster(r roster) bool {
	c.stats.RosterValidations++
	return r.Ballot > 0 && r.Leader >= 0 && r.Leader < c.n && r.Responders&bit(r.Leader) != 0 && r.Responders>>uint(c.n) == 0 && ValidBodegaRanges(r.Ranges, c.n)
}

func (c *core) observe(r roster, now time.Time) {
	if !c.validRoster(r) {
		return
	}
	c.observeValidatedRoster(r, now)
}

// observeValidatedRoster accepts only a roster that the caller fully checked.
// Clone its ranges before retaining it: current/pending are immutable validated
// configurations, reused by compact messages without rescanning their ranges.
func (c *core) observeValidatedRoster(r roster, now time.Time) {
	if r.Ballot <= c.current.Ballot || r.Ballot <= c.pending.Ballot {
		return
	}
	r.Ranges = slices.Clone(r.Ranges)
	c.pending = r
	// Stop read authority immediately; do not change the Paxos promise yet.
	c.incoming = map[int]grant{}
	c.requests = map[uint64]time.Time{}
	c.prepared = false
	c.revokeLeases(now)
	c.install(now)
}

func (c *core) install(now time.Time) {
	if c.pending.Ballot == 0 {
		return
	}
	for _, until := range c.outgoing {
		if now.Before(until) {
			return
		}
	}
	c.current = c.pending
	c.pending = roster{}
	// A new roster starts a fresh failure-detection window. This does not
	// renew grants or authorize reads; those still require actual lease replies.
	c.tracePeerAges("BODEGA_TIMER_RESET", now, 0)
	for i := range c.seen {
		c.refreshPeer(i, now)
	}
	c.threshold = c.high
	c.outgoing = map[int]time.Time{}
	c.incoming = map[int]grant{}
	c.snapshots = map[int]*snapshot{}
	c.ownSnapshot = nil
	c.snapshotChunks = nil
	c.snapshotCursor = map[int]int{}
	c.snapshotTaken = false
	c.snapshotStart, c.prepareStart = 0, c.prefix+1
	c.prepareSince = now
	c.votes = map[uint64]replicaset.Set{}
	// Executed history is already irrevocable; only the suffix needs a new vote.
	c.acceptedPrefix, c.readPrefix = c.prefix, c.prefix
	c.acceptedProgress = make([]uint64, c.n)
	c.acceptedProgress[c.id] = c.prefix
	c.noticePrefix = 0
	c.noticeHigh = 0
	c.noticeSlots = map[uint64]bool{}
	c.lastRepair = time.Time{}
	c.stats.RosterChanges++
	// An old held slot can be overwritten during recovery: re-route these reads.
	for id, h := range c.held {
		c.enqueue(h.request)
		delete(c.held, id)
	}
	c.holdSlots = nil
	c.fastSlots = nil
	c.fastQueued = map[uint64]bool{}
	c.holdWaiters = map[uint64][]defs.RequestID{}
	c.holdCounts = map[uint64]int{}
	for _, r := range c.batch {
		c.enqueue(r)
	}
	c.batch, c.batchBytes = nil, 0
	c.batchIDs = map[defs.RequestID]bool{}
	if c.current.Leader == c.id {
		c.startPrepare(now)
	}
}

func (c *core) startPrepare(now time.Time) {
	c.takeSnapshot(c.prepareStart)
	c.addSnapshot(c.id, c.prepareStart, 0, 1, c.ownSnapshot, now)
	c.broadcast(message{Kind: prepare, PrepareStart: c.prepareStart})
}

// Freeze accepted evidence once per ballot and requested suffix, like upstream's
// trigger_slot. Executed history is not part of a new leader's Prepare.
func (c *core) takeSnapshot(start uint64) bool {
	if start == 0 || start <= c.compacted {
		return false
	}
	if c.snapshotTaken {
		return start == c.snapshotStart
	}
	c.snapshotTaken, c.snapshotStart = true, start
	c.ownSnapshot = make([]entry, 0)
	for s := start; s <= c.high; s++ {
		if v, ok := c.log[s]; ok {
			c.ownSnapshot = append(c.ownSnapshot, v)
		}
	}
	first, size := 0, 0
	for i, v := range c.ownSnapshot {
		if i > first && (i-first >= 64 || size+entryBytes(v) > maxBatchBytes) {
			c.snapshotChunks = append(c.snapshotChunks, c.ownSnapshot[first:i])
			first, size = i, 0
		}
		size += entryBytes(v)
	}
	c.snapshotChunks = append(c.snapshotChunks, c.ownSnapshot[first:])
	return true
}

func (c *core) sendSnapshot(to int, start uint64) {
	if !c.takeSnapshot(start) {
		return
	}
	if c.snapshotCursor == nil {
		c.snapshotCursor = map[int]int{}
	}
	if c.snapshotCursor[to] == len(c.snapshotChunks) {
		c.snapshotCursor[to] = 0 // retry a lost reply, not a transfer pacing credit
	}
	c.flushPromises(to)
}

// The writer wakes this pump as queue space becomes available. Heartbeats only
// retry lost requests/replies; they impose no slots-per-second transfer budget.
func (c *core) flushPromises(to int) {
	if !c.active() || to != c.current.Leader || !c.snapshotTaken {
		return
	}
	for p := c.snapshotCursor[to]; p < len(c.snapshotChunks); p++ {
		m := message{Kind: promise, PrepareStart: c.snapshotStart,
			Part: p, Parts: len(c.snapshotChunks), Entries: c.snapshotChunks[p]}
		if !c.emit(to, m) {
			return
		}
		c.snapshotCursor[to] = p + 1
	}
}

func (c *core) addSnapshot(from int, start uint64, part, parts int, entries []entry, now time.Time) {
	if c.prepared || start != c.prepareStart || parts <= 0 || part < 0 || part >= parts {
		return
	}
	for _, v := range entries {
		if v.Slot < start || v.Ballot > c.current.Ballot {
			return
		}
	}
	s := c.snapshots[from]
	if s == nil {
		s = &snapshot{parts, map[int][]entry{}}
		c.snapshots[from] = s
	}
	if s.parts != parts {
		return
	}
	s.chunks[part] = entries
	count := 0
	for _, s := range c.snapshots {
		if len(s.chunks) == s.parts {
			count++
		}
	}
	if count < c.majority {
		return
	}
	selected := map[uint64]entry{}
	high := start - 1
	for _, s := range c.snapshots {
		if len(s.chunks) != s.parts {
			continue
		}
		for _, vs := range s.chunks {
			for _, v := range vs {
				if old, ok := selected[v.Slot]; !ok || v.Ballot > old.Ballot {
					selected[v.Slot] = v
				}
				high = maxSlot(high, v.Slot)
			}
		}
	}
	c.next = high
	c.inflight = map[defs.RequestID]uint64{}
	c.prepared = true
	for slot := start; slot <= high; slot++ {
		v, ok := selected[slot]
		if !ok {
			v = entry{Slot: slot, Request: request{Origin: -1}}
		}
		v.Ballot, v.ReadFresh = c.current.Ballot, false
		c.acceptEntry(v)
		for _, r := range v.requests() {
			c.inflight[r.id()] = slot
		}
		c.votes[slot] = replicaset.Set(0).With(c.id)
		// Already committed entries are fixed; others must cover the new roster.
		c.stats.RecoveredSlots++
	}
	c.stats.PrepareEntries += high - start + 1
	c.stats.PrepareCompleted++
	c.stats.PrepareNanos += uint64(now.Sub(c.prepareSince))
	c.reindex()
	for slot := start; slot <= high; slot++ {
		c.broadcast(message{Kind: accept, Entry: c.log[slot]})
		if c.committed[slot] {
			c.broadcast(message{Kind: commit, CommitSlot: slot, CommitPrefix: c.prefix})
		}
	}
	for p := 0; p < c.n; p++ {
		if p != c.id {
			c.repairPeer(p)
		}
	}
	// Values remain in the log; release the quorum's duplicate payloads.
	c.snapshots = map[int]*snapshot{}
}

func (c *core) proposeRosterRanges(leader int, responders uint64, ranges []BodegaResponderRange, now time.Time) {
	b := maxSlot(c.current.Ballot, c.pending.Ballot)
	r := roster{Ballot: (b/uint64(c.n)+1)*uint64(c.n) + uint64(c.id) + 1,
		Leader: leader, Responders: responders | bit(leader), Ranges: slices.Clone(ranges)}
	c.observe(r, now)
	c.broadcast(message{Kind: heartbeat})
}

func (c *core) proposeFilteredRoster(leader int, healthy uint64, now time.Time) {
	c.tracePeerAges("BODEGA_ROSTER_FILTER", now, healthy)
	mask, ranges := c.current.Responders, c.current.Ranges
	if c.current.Ballot == 0 {
		mask, ranges = c.opt.responders, c.opt.ranges
	}
	ranges = slices.Clone(ranges)
	for i := range ranges {
		ranges[i].Responders &= healthy
	}
	c.proposeRosterRanges(leader, mask&healthy, ranges, now)
}

// Log-gap repair and replica catch-up.
const repairWindow = 512

func (c *core) requestCommitRepair(now time.Time) {
	start := maxSlot(c.prefix, c.acceptedPrefix) + 1
	if !c.active() || c.id == c.current.Leader || start > c.noticeHigh {
		return
	}
	if !c.lastRepair.IsZero() && now.Sub(c.lastRepair) < c.opt.heartbeat {
		return
	}
	c.lastRepair = now
	c.stats.CommitRepairRequests++
	c.emit(c.current.Leader, message{Kind: repairRequest, RepairStart: start})
}

func (c *core) repairPeer(peer int) {
	start := minSlot(c.progress[peer], c.prefix) + 1
	end := c.sendCommitRepair(peer, start)
	// A lagging follower must not prevent solicitation of its vote for the
	// leader's first unexecuted slot, even when that slot is far ahead.
	if end <= c.prefix {
		c.sendCommitRepair(peer, c.prefix+1)
	}
	// Preserve the stronger current-ballot accepted-prefix evidence used by
	// out-of-order reads, but repair it separately from execution progress.
	if c.acceptedProgress[peer]+1 < start {
		c.sendRepairWindow(peer, c.acceptedProgress[peer]+1, 64)
	}
}

func (c *core) sendCommitRepair(peer int, start uint64) uint64 {
	return c.sendRepairWindow(peer, start, repairWindow)
}

func (c *core) sendRepairWindow(peer int, start, window uint64) uint64 {
	if start == 0 || start > c.next {
		return start
	}
	end := c.next
	if end-start >= window {
		end = start + window - 1
	}
	slot, size := start, 0
	for ; slot <= end; slot++ {
		v, ok := c.log[slot]
		if !ok {
			continue
		}
		// History before the Prepare suffix is already chosen, and must not be
		// re-voted merely to catch up a lagging follower in a newer ballot.
		if slot <= c.prefix && v.Ballot != c.current.Ballot {
			bytes := entryBytes(v)
			if size > 0 && size+bytes > maxBatchBytes {
				break
			}
			size += bytes
			c.stats.CommitRepairEntries++
			c.emit(peer, message{Kind: committedEntry, Entry: v})
			continue
		}
		if v.Ballot != c.current.Ballot {
			continue
		}
		// An acknowledged value survives for this ballot in the crash-stop
		// model. Do not flood the data queue with copies of it on every tick.
		if !c.votes[slot].Contains(peer) {
			bytes := entryBytes(v)
			if size > 0 && size+bytes > maxBatchBytes {
				break
			}
			size += bytes
			c.stats.CommitRepairEntries++
			c.emit(peer, message{Kind: accept, Entry: v})
		}
		if c.committed[slot] && slot > c.prefix {
			c.emit(peer, message{Kind: commit, CommitSlot: slot, CommitPrefix: c.prefix})
		}
	}
	c.emit(peer, message{Kind: commit, CommitPrefix: c.prefix})
	return slot
}

func (c *core) learnCommittedEntry(v entry) {
	if v.Slot <= c.prefix {
		return
	}
	// The installed leader supplies a chosen value from its executed history.
	// Give it current-ballot provenance so compact commit notices can repair gaps.
	v.Ballot, v.ReadFresh = c.current.Ballot, false
	c.acceptEntry(v)
	c.markCommitted(v.Slot)
	c.apply()
}
