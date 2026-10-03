package bodega

// Protocol state and normal-case transitions.
import (
	"bytes"
	"container/heap"
	"math/bits"
	"math/rand"
	"time"

	"github.com/hongzicong/ConsensusArena/replica/defs"
	"github.com/hongzicong/ConsensusArena/replicaset"
	"github.com/hongzicong/ConsensusArena/state"
)

type options struct {
	Lease, Margin, Heartbeat, Failure, FailureMax time.Duration
	Responders                                    uint64
	Ranges                                        []defs.BodegaResponderRange
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
	StableLeaderReads, UncommittedReadHolds                                         uint64
	CommitNotices, CommitRepairRequests, CommitRepairEntries                        uint64
	Batches, BatchCommands, MaxBatchCommands                                        uint64
	PrepareEntries, PrepareCompleted, PrepareNanos, Revokes, RevokeAcks             uint64
	CompactedSlots                                                                  uint64
}

// engine has no goroutines. All callbacks and transitions are serialized.
type engine struct {
	id, n, majority               int
	opt                           options
	current, pending              roster
	log                           map[uint64]entry
	committed                     map[uint64]bool
	votes                         map[uint64]replicaset.Set
	latest                        map[state.Key]uint64
	latestCommitted               map[state.Key]uint64
	prefix, high, threshold, next uint64
	acceptedPrefix, readPrefix    uint64
	acceptedProgress              []uint64
	prepared                      bool
	prepareStart, snapshotStart   uint64
	prepareSince                  time.Time
	compacted                     uint64
	revoked                       []uint64
	noticePrefix                  uint64
	noticeHigh                    uint64
	noticeSlots                   map[uint64]bool
	lastRepair                    time.Time
	snapshots                     map[int]*snapshot
	ownSnapshot                   []entry
	snapshotChunks                [][]entry
	batching                      bool
	batch                         []request
	batchIDs                      map[defs.RequestID]bool
	batchBytes                    int
	snapshotCursor                map[int]int
	snapshotTaken                 bool
	incoming                      map[int]grant
	outgoing                      map[int]time.Time
	requests                      map[uint64]time.Time
	sequence                      uint64
	seen                          []time.Time
	failureAt                     []time.Time
	progress                      []uint64
	held                          map[defs.RequestID]pendingRead
	holdSlots                     readSlots
	fastSlots                     readSlots
	fastQueued                    map[uint64]bool
	holdWaiters                   map[uint64][]defs.RequestID
	holdCounts                    map[uint64]int
	queued                        map[defs.RequestID]request
	retryQueue                    []defs.RequestID
	retryHead                     int
	inflight                      map[defs.RequestID]uint64
	completed                     map[defs.RequestID]state.Value
	stats                         statistics
	send                          func(int, message)
	trySend                       func(int, message) bool
	reply                         func(request, state.Value)
	execute                       func(state.Command) state.Value
	clock                         func() time.Time
	trace                         func(string, ...interface{})
}

func newEngine(id, n int, opt options, now time.Time) *engine {
	e := &engine{id: id, n: n, majority: n/2 + 1, opt: opt,
		log: map[uint64]entry{}, committed: map[uint64]bool{}, votes: map[uint64]replicaset.Set{}, latest: map[state.Key]uint64{},
		latestCommitted: map[state.Key]uint64{},
		snapshots:       map[int]*snapshot{}, incoming: map[int]grant{}, outgoing: map[int]time.Time{}, requests: map[uint64]time.Time{},
		seen: make([]time.Time, n), failureAt: make([]time.Time, n), progress: make([]uint64, n), held: map[defs.RequestID]pendingRead{}, queued: map[defs.RequestID]request{},
		inflight: map[defs.RequestID]uint64{}, completed: map[defs.RequestID]state.Value{}, holdWaiters: map[uint64][]defs.RequestID{}, holdCounts: map[uint64]int{},
		acceptedProgress: make([]uint64, n), fastQueued: map[uint64]bool{}, noticeSlots: map[uint64]bool{}, batchIDs: map[defs.RequestID]bool{},
		revoked: make([]uint64, n)}
	for i := range e.seen {
		e.refreshPeer(i, now)
	}
	return e
}

// Sample once per timer refresh, not on each failure check. These deadlines
// affect failure suspicion only; they never extend a roster lease.
func (e *engine) refreshPeer(peer int, now time.Time) {
	timeout := e.opt.Failure
	if e.opt.FailureMax > timeout {
		timeout += time.Duration(rand.Int63n(int64(e.opt.FailureMax-timeout) + 1))
	}
	e.seen[peer] = now
	e.failureAt[peer] = now.Add(timeout)
}

func (e *engine) peerHealthy(peer int, now time.Time) bool {
	return now.Before(e.failureAt[peer])
}

func bit(id int) uint64 { return uint64(1) << uint(id) }

func (e *engine) active() bool { return e.current.Ballot != 0 && e.pending.Ballot == 0 }

func (e *engine) emit(to int, m message) bool {
	m.From = e.id
	m.Roster = e.current
	if e.pending.Ballot > m.Roster.Ballot {
		m.Roster = e.pending
	}
	if e.active() && e.id == e.current.Leader {
		m.ReadPrefix = e.readPrefix
	}
	e.stats.Messages++
	if e.trySend != nil {
		return e.trySend(to, m)
	}
	e.send(to, m)
	return true
}

func (e *engine) broadcast(m message) {
	for i := 0; i < e.n; i++ {
		if i != e.id {
			e.emit(i, m)
		}
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
	e.refreshPeer(e.id, now)
	e.install(now)
	hb := message{Kind: heartbeat, Prefix: e.prefix}
	if e.active() && e.current.Leader == e.id && e.prepared {
		hb.CommitPrefix = e.prefix
	}
	e.broadcast(hb)
	if !e.active() {
		e.revokeLeases(now)
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
	for i := range e.seen {
		if e.peerHealthy(i, now) {
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
			// Repair execution gaps independently of current-ballot acceptance.
			for p := 0; p < e.n; p++ {
				if p == e.id {
					continue
				}
				e.repairPeer(p)
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
		e.retryQueue = append([]defs.RequestID(nil), e.retryQueue[e.retryHead:]...)
		e.retryHead = 0
	}
	e.release(now)
	e.compactLog()
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
	e.refreshPeer(m.From, now)
	// Revocation must work while either party is waiting to install a roster.
	if e.receiveRevocation(m, now) {
		return
	}
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
		e.progress[m.From] = maxSlot(e.progress[m.From], m.Prefix)
		if m.From == e.current.Leader {
			e.learnCommitNotice(m.CommitPrefix, 0, now)
		}
	case leaseRequest:
		e.outgoing[m.From] = now.Add(e.opt.Lease + e.opt.Margin)
		e.emit(m.From, message{Kind: leaseReply, Sequence: m.Sequence, Threshold: e.threshold})
	case leaseReply:
		until, ok := e.requests[m.Sequence]
		if ok && m.Roster.Ballot > e.revoked[m.From] && now.Before(until) && until.After(e.incoming[m.From].Until) {
			e.incoming[m.From] = grant{until, m.Threshold}
		}
	case prepare:
		if m.From == e.current.Leader {
			e.sendSnapshot(m.From, m.PrepareStart)
		}
	case promise:
		if e.id == e.current.Leader && !e.prepared {
			e.addSnapshot(m.From, m.PrepareStart, m.Part, m.Parts, m.Entries, now)
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
				e.votes[v.Slot] = e.votes[v.Slot].With(m.From)
				e.tryCommit(v.Slot, now)
			}
		}
	case commit:
		e.stats.CommitNotices++
		e.learnCommitNotice(m.CommitPrefix, m.CommitSlot, now)
	case committedEntry:
		if m.From == e.current.Leader {
			e.learnCommittedEntry(m.Entry)
		}
	case acceptNote:
		// Reserved wire kind: optional pre-commit read notifications are disabled.
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
	if v.Slot == 0 || v.Slot <= e.compacted {
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
				// Figure 7: the stable leader reads the latest committed value.
				// A commit can precede prefix execution; use the same slot-value
				// lookup as responders, never an older state-machine value.
				s = e.latestCommitted[r.Proposal.Command.K]
			}
			if value, ok := e.readValue(s, r); ok {
				e.finishLocalRead(r, value)
				return
			}
			if _, ok := e.held[id]; !ok {
				if s > e.prefix && !e.committed[s] {
					e.stats.UncommittedReadHolds++
				}
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

// Notice evidence never supplies a value: only an Accept in the same ballot
// can satisfy it. In particular, recovery cannot commit a stale local tail.
func (e *engine) applyNoticedSlot(slot uint64) {
	if slot == 0 || (slot > e.noticePrefix && !e.noticeSlots[slot]) {
		return
	}
	v, ok := e.log[slot]
	if !ok || v.Ballot != e.current.Ballot {
		return
	}
	if !e.committed[slot] {
		e.markCommitted(slot)
	}
	delete(e.noticeSlots, slot)
}

func (e *engine) learnCommitNotice(prefix, slot uint64, now time.Time) {
	e.noticePrefix = maxSlot(e.noticePrefix, prefix)
	e.noticeHigh = maxSlot(e.noticeHigh, maxSlot(prefix, slot))
	if slot > e.prefix {
		e.noticeSlots[slot] = true
		e.applyNoticedSlot(slot)
	}
	// The executed prefix is already fixed. Check the unexecuted suffix
	// directly, without waiting to re-accept all earlier slots in this ballot.
	for s := e.prefix + 1; s <= e.noticePrefix; s++ {
		v, ok := e.log[s]
		if !ok || v.Ballot != e.current.Ballot {
			break
		}
		e.applyNoticedSlot(s)
	}
	e.apply()
	e.requestCommitRepair(now)
}

func (e *engine) tryCommit(s uint64, now time.Time) {
	if !e.active() || !e.prepared || e.current.Leader != e.id || e.committed[s] {
		return
	}
	v := e.log[s]
	mask := e.votes[s]
	if mask.Size() < e.majority {
		return
	}
	responders := uint64(0)
	for _, r := range v.requests() {
		if r.Proposal.Command.Op == state.PUT {
			responders |= e.current.respondersFor(r.Proposal.Command.K)
		}
	}
	if !mask.Covers(replicaset.Set(responders)) {
		e.stats.CoverageWaits++
		return
	}
	e.markCommitted(s)
	e.stats.Commits++
	e.apply()
	e.broadcast(message{Kind: commit, CommitPrefix: e.prefix, CommitSlot: s})
	e.release(now)
}

func (e *engine) markCommitted(s uint64) {
	e.committed[s] = true
	for _, r := range e.log[s].requests() {
		cmd := r.Proposal.Command
		if cmd.Op == state.PUT && s > e.latestCommitted[cmd.K] {
			e.latestCommitted[cmd.K] = s
		}
	}
	e.queueFastSlot(s)
}
