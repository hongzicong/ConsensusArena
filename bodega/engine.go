package bodega

import (
	"bytes"
	"container/heap"
	"math/bits"
	"slices"
	"sort"
	"time"

	"github.com/hongzicong/ConsensusArena/replica/defs"
	"github.com/hongzicong/ConsensusArena/state"
)

type options struct {
	Lease, Margin, Heartbeat, Failure time.Duration
	Responders                        uint64
	Ranges                            []defs.BodegaResponderRange
}
type grant struct {
	Until     time.Time
	Threshold uint64
}
type pendingRead struct {
	Request request
	Slot    uint64
	Since   time.Time
}
type snapshot struct {
	Parts  int
	Chunks map[int][]entry
}
type statistics struct {
	LocalReads, HeldReads, FallbackReads, Forwarded, Writes, Commits, RosterChanges uint64
	LeaseMisses, CoverageWaits, RecoveredSlots, Messages                            uint64
	RetriedRequests, DroppedMessages                                                uint64
	OutOfOrderReads                                                                 uint64
	StableLeaderReads, LeaderPendingWriteBypasses                                   uint64
	CommitNotices, CommitRepairRequests, CommitRepairEntries                        uint64
	Batches, BatchCommands, MaxBatchCommands                                        uint64
}

// engine has no goroutines. All callbacks and transitions are serialized.
type engine struct {
	id, n, majority               int
	opt                           options
	current, pending              roster
	log                           map[uint64]entry
	committed                     map[uint64]bool
	votes                         map[uint64]uint64
	latest                        map[state.Key]uint64
	latestCommitted               map[state.Key]uint64
	prefix, high, threshold, next uint64
	acceptedPrefix, readPrefix    uint64
	acceptedProgress              []uint64
	prepared                      bool
	noticePrefix                  uint64
	noticeHigh                    uint64
	noticeSlots                   map[uint64]bool
	lastRepair                    time.Time
	snapshots                     map[int]*snapshot
	ownSnapshot                   []entry
	snapshotChunks                [][]entry
	batching                      bool
	batch                         []request
	batchIDs                      map[requestID]bool
	batchBytes                    int
	snapshotCursor                map[int]int
	snapshotTaken                 bool
	incoming                      map[int]grant
	outgoing                      map[int]time.Time
	requests                      map[uint64]time.Time
	sequence                      uint64
	seen                          []time.Time
	progress                      []uint64
	held                          map[requestID]pendingRead
	holdSlots                     readSlots
	fastSlots                     readSlots
	fastQueued                    map[uint64]bool
	holdWaiters                   map[uint64][]requestID
	holdCounts                    map[uint64]int
	queued                        map[requestID]request
	retryQueue                    []requestID
	retryHead                     int
	inflight                      map[requestID]uint64
	completed                     map[requestID]state.Value
	stats                         statistics
	send                          func(int, message)
	reply                         func(request, state.Value)
	execute                       func(state.Command) state.Value
	clock                         func() time.Time
	trace                         func(string, ...interface{})
}

func newEngine(id, n int, opt options, now time.Time) *engine {
	e := &engine{id: id, n: n, majority: n/2 + 1, opt: opt,
		log: map[uint64]entry{}, committed: map[uint64]bool{}, votes: map[uint64]uint64{}, latest: map[state.Key]uint64{},
		latestCommitted: map[state.Key]uint64{},
		snapshots:       map[int]*snapshot{}, incoming: map[int]grant{}, outgoing: map[int]time.Time{}, requests: map[uint64]time.Time{},
		seen: make([]time.Time, n), progress: make([]uint64, n), held: map[requestID]pendingRead{}, queued: map[requestID]request{},
		inflight: map[requestID]uint64{}, completed: map[requestID]state.Value{}, holdWaiters: map[uint64][]requestID{}, holdCounts: map[uint64]int{},
		acceptedProgress: make([]uint64, n), fastQueued: map[uint64]bool{}, noticeSlots: map[uint64]bool{}, batchIDs: map[requestID]bool{}}
	for i := range e.seen {
		e.seen[i] = now
	}
	return e
}
func bit(id int) uint64        { return uint64(1) << uint(id) }
func (e *engine) active() bool { return e.current.Ballot != 0 && e.pending.Ballot == 0 }
func (e *engine) emit(to int, m message) {
	m.From = e.id
	m.Roster = e.current
	if e.pending.Ballot > m.Roster.Ballot {
		m.Roster = e.pending
	}
	if e.active() && e.id == e.current.Leader {
		m.ReadPrefix = e.readPrefix
	}
	e.stats.Messages++
	e.send(to, m)
}
func (e *engine) broadcast(m message) {
	for i := 0; i < e.n; i++ {
		if i != e.id {
			e.emit(i, m)
		}
	}
}
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
		e.seen[i] = now
	}
	e.threshold = e.high
	e.outgoing = map[int]time.Time{}
	e.incoming = map[int]grant{}
	e.snapshots = map[int]*snapshot{}
	e.ownSnapshot = nil
	e.snapshotChunks = nil
	e.snapshotCursor = map[int]int{}
	e.snapshotTaken = false
	e.votes = map[uint64]uint64{}
	e.acceptedPrefix, e.readPrefix = 0, 0
	e.acceptedProgress = make([]uint64, e.n)
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
func (e *engine) tracePeerAges(event string, now time.Time, healthy uint64) {
	if e.trace == nil {
		return
	}
	ages := make([]int64, e.n)
	for i, seen := range e.seen {
		ages[i] = now.Sub(seen).Milliseconds()
	}
	e.trace("%s replica=%d ballot=%d responders=%x healthy=%x peer_ages_ms=%v", event, e.id, e.current.Ballot, e.current.Responders, healthy, ages)
}
func (e *engine) stable(now time.Time) bool {
	if e.clock != nil {
		now = e.clock()
	}
	if !e.active() {
		return false
	}
	count := 0
	if e.prefix >= e.threshold {
		count++
	}
	for p, g := range e.incoming {
		if p != e.id && now.Before(g.Until) && e.prefix >= g.Threshold {
			count++
		}
	}
	return count >= e.majority
}
func (e *engine) tick(now time.Time) {
	e.seen[e.id] = now
	e.install(now)
	hb := message{Kind: heartbeat, Prefix: e.prefix}
	if e.active() && e.current.Leader == e.id && e.prepared {
		hb.CommitPrefix = e.prefix
	}
	e.broadcast(hb)
	if !e.active() {
		return
	}
	e.sequence++
	e.requests[e.sequence] = now.Add(e.opt.Lease)
	for s, until := range e.requests {
		if !now.Before(until) {
			delete(e.requests, s)
		}
	}
	e.broadcast(message{Kind: leaseRequest, Sequence: e.sequence})
	healthy := uint64(0)
	first := -1
	for i, t := range e.seen {
		if now.Sub(t) < e.opt.Failure {
			healthy |= bit(i)
			if first < 0 {
				first = i
			}
		}
	}
	if bits.OnesCount64(healthy) >= e.majority {
		if healthy&bit(e.current.Leader) == 0 && first == e.id {
			e.proposeFilteredRoster(e.id, healthy, now)
			return
		}
		if e.current.Leader == e.id && e.current.allResponders() & ^healthy != 0 {
			e.proposeFilteredRoster(e.id, healthy, now)
			return
		}
	}
	if e.current.Leader == e.id {
		if !e.prepared {
			e.startPrepare(now)
		} else {
			// Retry pending slots and catch lagging peers up in bounded batches.
			for p := 0; p < e.n; p++ {
				if p == e.id {
					continue
				}
				// A peer may have learned more old commits than the new leader.
				// Still solicit its votes for the leader's uncommitted prefix.
				start := e.progress[p]
				if e.prefix < start {
					start = e.prefix
				}
				end := start + 64
				if end > e.next {
					end = e.next
				}
				for s := start + 1; s <= end; s++ {
					v, ok := e.log[s]
					if !ok {
						continue
					}
					if e.committed[s] {
						if s > e.prefix {
							e.emit(p, message{Kind: commit, CommitSlot: s, CommitPrefix: e.prefix})
						}
					} else if e.votes[s]&bit(p) == 0 {
						e.emit(p, message{Kind: accept, Entry: v})
					}
				}
			}
		}
	}
	if e.current.Leader != e.id {
		e.requestCommitRepair(now)
	}
	// Bound retry work independently of backlog size. FIFO rotation prevents
	// loss repair from flooding the connection or starving later requests.
	budget := len(e.retryQueue) - e.retryHead
	if budget > 256 {
		budget = 256
	}
	for i := 0; i < budget; i++ {
		id := e.retryQueue[e.retryHead]
		e.retryHead++
		if r, ok := e.queued[id]; ok {
			delete(e.queued, id)
			e.stats.RetriedRequests++
			e.submit(r, now)
		}
	}
	if e.retryHead > 0 && e.retryHead >= len(e.retryQueue)/2 {
		e.retryQueue = append([]requestID(nil), e.retryQueue[e.retryHead:]...)
		e.retryHead = 0
	}
	e.release(now)
}

func (e *engine) enqueue(r request) {
	id := r.id()
	if _, ok := e.queued[id]; !ok {
		e.retryQueue = append(e.retryQueue, id)
	}
	e.queued[id] = r
}
func (e *engine) receive(m message, now time.Time) {
	if m.From < 0 || m.From >= e.n || m.From == e.id {
		return
	}
	e.seen[m.From] = now
	if m.Kind == commit && m.From != e.current.Leader {
		return
	}
	// Results retain their ingress identity across ballot changes.
	if m.Kind == result {
		if m.Request.Origin == e.id {
			e.finish(m.Request, m.Value)
		}
		return
	}
	// Only a heartbeat carries the complete configuration on the wire.
	if m.Kind != heartbeat && m.Roster.Responders == 0 {
		if !e.active() || m.Roster.Ballot != e.current.Ballot {
			return
		}
		m.Roster = e.current
	}
	if !e.validRoster(m.Roster) {
		return
	}
	e.observe(m.Roster, now)
	if !e.active() || !sameRoster(m.Roster, e.current) {
		return
	}
	if m.From == e.current.Leader && m.ReadPrefix > e.readPrefix {
		e.readPrefix = m.ReadPrefix
	}
	switch m.Kind {
	case heartbeat:
		e.progress[m.From] = m.Prefix
		if m.From == e.current.Leader {
			e.learnCommitNotice(m.CommitPrefix, 0, now)
		}
	case leaseRequest:
		e.outgoing[m.From] = now.Add(e.opt.Lease + e.opt.Margin)
		e.emit(m.From, message{Kind: leaseReply, Sequence: m.Sequence, Threshold: e.threshold})
	case leaseReply:
		until, ok := e.requests[m.Sequence]
		if ok && now.Before(until) && until.After(e.incoming[m.From].Until) {
			e.incoming[m.From] = grant{until, m.Threshold}
		}
	case prepare:
		if m.From == e.current.Leader {
			e.sendSnapshot(m.From)
		}
	case promise:
		if e.id == e.current.Leader && !e.prepared {
			e.addSnapshot(m.From, m.Part, m.Parts, m.Entries, now)
		}
	case accept:
		if m.From == e.current.Leader && m.Entry.Ballot == e.current.Ballot {
			e.acceptEntry(m.Entry)
			e.emit(m.From, message{Kind: accepted, Prefix: e.acceptedPrefix, Entry: entry{Slot: m.Entry.Slot, Ballot: m.Entry.Ballot}})
			e.applyNoticedSlot(m.Entry.Slot)
			e.learnCommitNotice(0, 0, now)
		}
	case accepted:
		if e.id == e.current.Leader && e.prepared && m.Entry.Ballot == e.current.Ballot {
			if v, ok := e.log[m.Entry.Slot]; ok && v.Ballot == m.Entry.Ballot {
				if m.Prefix > e.acceptedProgress[m.From] {
					e.acceptedProgress[m.From] = m.Prefix
					e.updateReadPrefix()
				}
				e.votes[v.Slot] |= bit(m.From)
				e.tryCommit(v.Slot, now)
			}
		}
	case commit:
		e.stats.CommitNotices++
		e.learnCommitNotice(m.CommitPrefix, m.CommitSlot, now)
	case repairRequest:
		if e.id == e.current.Leader && e.prepared {
			e.sendCommitRepair(m.From, m.RepairStart)
		}
	case forward:
		if e.id == e.current.Leader {
			e.submit(m.Request, now)
		}
	}
	e.release(now)
}
func (e *engine) startPrepare(now time.Time) {
	e.takeSnapshot()
	e.addSnapshot(e.id, 0, 1, e.ownSnapshot, now)
	e.broadcast(message{Kind: prepare})
}
func (e *engine) takeSnapshot() {
	if e.snapshotTaken {
		return
	}
	e.snapshotTaken = true
	e.ownSnapshot = make([]entry, 0, len(e.log))
	for _, v := range e.log {
		e.ownSnapshot = append(e.ownSnapshot, v)
	}
	sort.Slice(e.ownSnapshot, func(i, j int) bool { return e.ownSnapshot[i].Slot < e.ownSnapshot[j].Slot })
	start, size := 0, 0
	for i, v := range e.ownSnapshot {
		if i > start && (i-start >= 64 || size+entryBytes(v) > maxBatchBytes) {
			e.snapshotChunks = append(e.snapshotChunks, e.ownSnapshot[start:i])
			start, size = i, 0
		}
		size += entryBytes(v)
	}
	e.snapshotChunks = append(e.snapshotChunks, e.ownSnapshot[start:])
}
func (e *engine) sendSnapshot(to int) {
	e.takeSnapshot()
	parts := len(e.snapshotChunks)
	if parts == 0 {
		parts = 1
	}
	// Stream a bounded window per prepare retry, cycling to repair lost chunks.
	// A full-log burst can otherwise repeatedly overflow a bounded send queue.
	window := parts
	if window > 8 {
		window = 8
	}
	for i := 0; i < window; i++ {
		p := e.snapshotCursor[to] % parts
		e.snapshotCursor[to]++
		e.emit(to, message{Kind: promise, Part: p, Parts: parts, Entries: e.snapshotChunks[p]})
	}
}
func (e *engine) addSnapshot(from, part, parts int, entries []entry, now time.Time) {
	if e.prepared || parts <= 0 || part < 0 || part >= parts {
		return
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
	high := uint64(0)
	for _, s := range e.snapshots {
		if len(s.Chunks) != s.Parts {
			continue
		}
		for _, vs := range s.Chunks {
			for _, v := range vs {
				if old, ok := selected[v.Slot]; !ok || v.Ballot > old.Ballot {
					selected[v.Slot] = v
				}
				if v.Slot > high {
					high = v.Slot
				}
			}
		}
	}
	e.next = high
	e.inflight = map[requestID]uint64{}
	e.prepared = true
	for s := uint64(1); s <= high; s++ {
		v, ok := selected[s]
		if !ok {
			v = entry{Slot: s, Request: request{Origin: -1}}
		}
		v.Ballot = e.current.Ballot
		v.ReadFresh = false
		e.acceptEntry(v)
		for _, r := range v.requests() {
			if r.Proposal.Command.Op != state.NONE {
				e.inflight[r.id()] = s
			}
		}
		e.votes[s] = bit(e.id)
		// Re-cover even already learned values under the new roster.
		if s > e.prefix {
			delete(e.committed, s)
		}
		e.broadcast(message{Kind: accept, Entry: v})
		e.stats.RecoveredSlots++
	}
	e.reindex()
}
func sameEntry(a, b entry) bool {
	xs, ys := a.requests(), b.requests()
	if len(xs) != len(ys) {
		return false
	}
	for i := range xs {
		x, y := xs[i].Proposal, ys[i].Proposal
		if x.ClientId != y.ClientId || x.CommandId != y.CommandId || x.Command.Op != y.Command.Op || x.Command.K != y.Command.K || !bytes.Equal(x.Command.V, y.Command.V) {
			return false
		}
	}
	return true
}
func (e *engine) acceptEntry(v entry) {
	if v.Slot == 0 {
		return
	}
	old, ok := e.log[v.Slot]
	if ok && (old.Ballot > v.Ballot || (old.Ballot == v.Ballot && !sameEntry(old, v))) {
		panic("bodega: conflicting accepted value")
	}
	if ok && e.committed[v.Slot] && !sameEntry(old, v) {
		panic("bodega: recovery changed a committed value")
	}
	e.log[v.Slot] = v
	for {
		next, ok := e.log[e.acceptedPrefix+1]
		if !ok || next.Ballot != e.current.Ballot {
			break
		}
		e.acceptedPrefix++
	}
	e.acceptedProgress[e.id] = e.acceptedPrefix
	if v.Slot > e.high {
		e.high = v.Slot
	}
	if v.Slot > e.next {
		e.next = v.Slot
	}
	if ok && !sameEntry(old, v) {
		delete(e.committed, v.Slot)
		e.reindex()
	} else {
		for _, r := range v.requests() {
			if r.Proposal.Command.Op == state.PUT && v.Slot > e.latest[r.Proposal.Command.K] {
				e.latest[r.Proposal.Command.K] = v.Slot
			}
		}
	}
}
func (e *engine) reindex() {
	e.latest = map[state.Key]uint64{}
	e.latestCommitted = map[state.Key]uint64{}
	for s, v := range e.log {
		for _, r := range v.requests() {
			if r.Proposal.Command.Op != state.PUT {
				continue
			}
			k := r.Proposal.Command.K
			if s > e.latest[k] {
				e.latest[k] = s
			}
			if e.committed[s] && s > e.latestCommitted[k] {
				e.latestCommitted[k] = s
			}
		}
	}
}
func (e *engine) submit(r request, now time.Time) {
	id := r.id()
	if value, ok := e.completed[id]; ok {
		e.finish(r, value)
		return
	}
	if !e.active() {
		e.enqueue(r)
		return
	}
	if e.current.Leader == e.id && !e.prepared {
		e.enqueue(r)
		return
	}
	if r.Proposal.Command.Op == state.GET && e.current.respondersFor(r.Proposal.Command.K)&bit(e.id) != 0 {
		if e.localReadAuthority(now) {
			s := e.latest[r.Proposal.Command.K]
			if e.current.Leader == e.id {
				// A stable leader reads committed state, ignoring concurrent
				// uncommitted writes. Followers must still inspect accepted writes.
				s = e.latestCommitted[r.Proposal.Command.K]
			}
			if value, ok := e.readValue(s, r); ok {
				e.finishLocalRead(r, value)
				return
			}
			if _, ok := e.held[id]; !ok {
				e.held[id] = pendingRead{r, s, now}
				if _, exists := e.holdWaiters[s]; !exists {
					heap.Push(&e.holdSlots, s)
				}
				e.holdWaiters[s] = append(e.holdWaiters[s], id)
				e.holdCounts[s]++
				e.queueFastSlot(s)
				e.stats.HeldReads++
			}
			delete(e.queued, id)
			return
		}
		e.stats.LeaseMisses++
	}
	if e.current.Leader != e.id {
		e.enqueue(r)
		e.stats.Forwarded++
		e.emit(e.current.Leader, message{Kind: forward, Request: r})
		return
	}
	if _, ok := e.inflight[id]; ok {
		return
	}
	delete(e.queued, id)
	e.admit(r, now)
}
func (e *engine) tryCommit(s uint64, now time.Time) {
	if !e.active() || !e.prepared || e.current.Leader != e.id || e.committed[s] {
		return
	}
	v := e.log[s]
	mask := e.votes[s]
	if bits.OnesCount64(mask) < e.majority {
		return
	}
	responders := uint64(0)
	for _, r := range v.requests() {
		if r.Proposal.Command.Op == state.PUT {
			responders |= e.current.respondersFor(r.Proposal.Command.K)
		}
	}
	if mask&responders != responders {
		e.stats.CoverageWaits++
		return
	}
	e.markCommitted(s)
	e.stats.Commits++
	e.apply()
	e.broadcast(message{Kind: commit, CommitPrefix: e.prefix, CommitSlot: s})
	e.release(now)
}
func (e *engine) apply() {
	for e.committed[e.prefix+1] {
		e.prefix++
		v := e.log[e.prefix]
		for _, r := range v.requests() {
			if r.Proposal.Command.Op == state.NONE {
				continue
			}
			value, ok := e.completed[r.id()]
			if !ok {
				value = e.execute(r.Proposal.Command)
				e.completed[r.id()] = value
			}
			if e.id == e.current.Leader {
				e.finish(r, value)
			}
		}
	}
}
func (e *engine) finish(r request, value state.Value) {
	delete(e.queued, r.id())
	e.dropHeld(r.id())
	if r.Origin == e.id {
		e.reply(r, value)
	} else if r.Origin >= 0 && r.Origin < e.n {
		e.emit(r.Origin, message{Kind: result, Request: r, Value: value})
	}
}
func (e *engine) release(now time.Time) {
	if !e.stable(now) {
		return
	}
	for len(e.holdSlots) > 0 && e.holdSlots[0] <= e.prefix {
		s := e.holdSlots[0]
		if !e.releaseSlot(s, now) {
			return
		}
		heap.Pop(&e.holdSlots)
	}
	for len(e.fastSlots) > 0 && e.fastSlots[0] <= e.readPrefix {
		s := e.fastSlots[0]
		if !e.releaseSlot(s, now) {
			return
		}
		heap.Pop(&e.fastSlots)
		delete(e.fastQueued, s)
	}
}
