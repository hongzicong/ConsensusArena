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
	lease, margin, heartbeat, failure, failureMax time.Duration
	responders                                    uint64
	ranges                                        []BodegaResponderRange
}

type grant struct {
	until     time.Time
	threshold uint64
}

type pendingRead struct {
	request request
	slot    uint64
	since   time.Time
}

type snapshot struct {
	parts  int
	chunks map[int][]entry
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
	RosterValidations, RosterReuses                                                 uint64
}

// core has no goroutines. All callbacks and transitions are serialized.
// core owns protocol state, ordering, recovery decisions and completion semantics.
type core struct {
	id, n, majority               int
	opt                           options
	current, pending              roster
	heartbeatBallot               uint64 // ballot of the last full-roster broadcast attempt
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
	trySend                       func(int, message) bool
	tryBroadcast                  func(message) int
	reply                         func(request, state.Value) bool
	execute                       func(state.Command) state.Value
	clock                         func() time.Time
	trace                         func(string, ...interface{})
}

func newCore(id, n int, opt options) *core {
	c := &core{id: id, n: n, majority: n/2 + 1, opt: opt,
		log: map[uint64]entry{}, committed: map[uint64]bool{}, votes: map[uint64]replicaset.Set{}, latest: map[state.Key]uint64{},
		latestCommitted: map[state.Key]uint64{},
		snapshots:       map[int]*snapshot{}, incoming: map[int]grant{}, outgoing: map[int]time.Time{}, requests: map[uint64]time.Time{},
		seen: make([]time.Time, n), failureAt: make([]time.Time, n), progress: make([]uint64, n), held: map[defs.RequestID]pendingRead{}, queued: map[defs.RequestID]request{},
		inflight: map[defs.RequestID]uint64{}, completed: map[defs.RequestID]state.Value{}, holdWaiters: map[uint64][]defs.RequestID{}, holdCounts: map[uint64]int{},
		acceptedProgress: make([]uint64, n), fastQueued: map[uint64]bool{}, noticeSlots: map[uint64]bool{}, batchIDs: map[defs.RequestID]bool{},
		revoked: make([]uint64, n)}
	return c
}

// Sample once per timer refresh, not on each failure check. These deadlines
// affect failure suspicion only; they never extend a roster lease.
func (c *core) refreshPeer(peer int, now time.Time) {
	timeout := c.opt.failure
	if c.opt.failureMax > timeout {
		timeout += time.Duration(rand.Int63n(int64(c.opt.failureMax-timeout) + 1))
	}
	c.seen[peer] = now
	c.failureAt[peer] = now.Add(timeout)
}

func (c *core) peerHealthy(peer int, now time.Time) bool {
	return now.Before(c.failureAt[peer])
}

func bit(id int) uint64 { return uint64(1) << uint(id) }

func (c *core) active() bool { return c.current.Ballot != 0 && c.pending.Ballot == 0 }

func (c *core) prepareMessage(m message) message {
	m.From = c.id
	m.Roster = c.current
	if c.pending.Ballot > m.Roster.Ballot {
		m.Roster = c.pending
	}
	if c.active() && c.id == c.current.Leader {
		m.ReadPrefix = c.readPrefix
	}
	return m
}

func (c *core) emit(to int, m message) bool {
	m = c.prepareMessage(m)
	c.stats.Messages++
	sent := c.trySend(to, m)
	if !sent && m.Kind != promise {
		c.stats.DroppedMessages++
	}
	return sent
}

func (c *core) broadcast(m message) {
	c.stats.Messages += uint64(c.n - 1)
	m = c.prepareMessage(m)
	drops := c.tryBroadcast(m)
	if m.Kind != promise {
		c.stats.DroppedMessages += uint64(drops)
	}
	if m.Kind == heartbeat {
		c.heartbeatBallot = m.Roster.Ballot
	}
}

func (c *core) heartbeatMessage() message {
	m := message{Kind: heartbeat, Prefix: c.prefix}
	if c.active() && c.current.Leader == c.id && c.prepared {
		m.CommitPrefix = c.prefix
	}
	return m
}

func (c *core) tracePeerAges(event string, now time.Time, healthy uint64) {
	if c.trace == nil {
		return
	}
	ages := make([]int64, c.n)
	for i, seen := range c.seen {
		ages[i] = now.Sub(seen).Milliseconds()
	}
	c.trace("%s replica=%d ballot=%d responders=%x healthy=%x peer_ages_ms=%v", event, c.id, c.current.Ballot, c.current.Responders, healthy, ages)
}

func (c *core) stable(now time.Time) bool {
	if c.clock != nil {
		now = c.clock()
	}
	if !c.active() {
		return false
	}
	count := 0
	if c.prefix >= c.threshold {
		count++
	}
	for p, g := range c.incoming {
		if p != c.id && now.Before(g.until) && c.prefix >= g.threshold {
			count++
		}
	}
	return count >= c.majority
}

func (c *core) tick(now time.Time) {
	c.refreshPeer(c.id, now)
	c.install(now)
	hb := c.prepareMessage(c.heartbeatMessage())
	// Paper §5.2: unchanged rosters need only their ballot on the wire.
	// Peers missing a full roster request it again; a failed peer must not
	// force full-roster broadcasts to healthy peers indefinitely.
	if hb.Roster.Ballot != 0 && hb.Roster.Ballot == c.heartbeatBallot {
		hb.Kind = lightHeartbeat
	}
	c.broadcast(hb)
	if !c.active() {
		c.revokeLeases(now)
		return
	}
	c.sequence++
	c.requests[c.sequence] = now.Add(c.opt.lease)
	for s, until := range c.requests {
		if !now.Before(until) {
			delete(c.requests, s)
		}
	}
	c.broadcast(message{Kind: leaseRequest, Sequence: c.sequence})
	healthy := uint64(0)
	first := -1
	for i := range c.seen {
		if c.peerHealthy(i, now) {
			healthy |= bit(i)
			if first < 0 {
				first = i
			}
		}
	}
	if bits.OnesCount64(healthy) >= c.majority {
		if healthy&bit(c.current.Leader) == 0 && first == c.id {
			c.proposeFilteredRoster(c.id, healthy, now)
			return
		}
		if c.current.Leader == c.id && c.current.allResponders() & ^healthy != 0 {
			c.proposeFilteredRoster(c.id, healthy, now)
			return
		}
	}
	if c.current.Leader == c.id {
		if !c.prepared {
			c.startPrepare(now)
		} else {
			// Repair execution gaps independently of current-ballot acceptance.
			for p := 0; p < c.n; p++ {
				if p == c.id || !c.peerHealthy(p, now) {
					continue
				}
				c.repairPeer(p)
			}
		}
	}
	if c.current.Leader != c.id {
		c.requestCommitRepair(now)
	}
	// Bound retry work independently of backlog size. FIFO rotation prevents
	// loss repair from flooding the connection or starving later requests.
	budget := len(c.retryQueue) - c.retryHead
	if budget > 256 {
		budget = 256
	}
	for i := 0; i < budget; i++ {
		id := c.retryQueue[c.retryHead]
		c.retryHead++
		if r, ok := c.queued[id]; ok {
			delete(c.queued, id)
			c.stats.RetriedRequests++
			c.submit(r, now)
		}
	}
	if c.retryHead > 0 && c.retryHead >= len(c.retryQueue)/2 {
		c.retryQueue = append([]defs.RequestID(nil), c.retryQueue[c.retryHead:]...)
		c.retryHead = 0
	}
	c.release(now)
	c.compactLog()
}

func (c *core) enqueue(r request) {
	id := r.id()
	if _, ok := c.queued[id]; !ok {
		c.retryQueue = append(c.retryQueue, id)
	}
	c.queued[id] = r
}

func (c *core) receive(m message, now time.Time) {
	if m.From < 0 || m.From >= c.n || m.From == c.id {
		return
	}
	c.refreshPeer(m.From, now)
	if m.Kind == rosterRequest {
		// Reply with the latest complete roster, even while installation waits
		// for old leases. A compact heartbeat alone never installs a roster.
		if c.current.Ballot != 0 || c.pending.Ballot != 0 {
			c.emit(m.From, c.heartbeatMessage())
		}
		return
	}
	// Only local substitution can mark a roster as known. current and pending
	// were fully validated and cloned by observeValidatedRoster; their ranges
	// remain immutable until a different roster replaces them.
	knownRoster := false
	if m.Kind == lightHeartbeat {
		switch {
		case m.Roster.Ballot != 0 && m.Roster.Ballot == c.current.Ballot:
			m.Roster = c.current
		case m.Roster.Ballot != 0 && m.Roster.Ballot == c.pending.Ballot:
			m.Roster = c.pending
		default:
			if m.Roster.Ballot > c.current.Ballot && m.Roster.Ballot > c.pending.Ballot {
				c.emit(m.From, message{Kind: rosterRequest})
			}
			return
		}
		knownRoster = true
		m.Kind = heartbeat
	}
	// Revocation must work while either party is waiting to install a roster.
	if c.receiveRevocation(m, now) {
		return
	}
	if m.Kind == commit && m.From != c.current.Leader {
		return
	}
	// Results retain their ingress identity across ballot changes.
	if m.Kind == result {
		if m.Request.Origin == c.id {
			c.finish(m.Request, m.Value)
		}
		return
	}
	// Only a heartbeat carries the complete configuration on the wire.
	if m.Kind != heartbeat && m.Roster.Responders == 0 {
		if !c.active() || m.Roster.Ballot != c.current.Ballot {
			return
		}
		m.Roster = c.current
		knownRoster = true
	}
	if knownRoster {
		c.stats.RosterReuses++
	} else {
		// Full external configurations must still be checked, even when their
		// ballot matches an installed configuration. Never trust a ballot alone.
		if !c.validRoster(m.Roster) {
			return
		}
		c.observeValidatedRoster(m.Roster, now)
	}
	if !c.active() || (!knownRoster && !sameRoster(m.Roster, c.current)) {
		return
	}
	if m.From == c.current.Leader && m.ReadPrefix > c.readPrefix {
		c.readPrefix = m.ReadPrefix
	}
	switch m.Kind {
	case heartbeat:
		c.progress[m.From] = maxSlot(c.progress[m.From], m.Prefix)
		if m.From == c.current.Leader {
			c.learnCommitNotice(m.CommitPrefix, 0, now)
		}
	case leaseRequest:
		c.outgoing[m.From] = now.Add(c.opt.lease + c.opt.margin)
		c.emit(m.From, message{Kind: leaseReply, Sequence: m.Sequence, Threshold: c.threshold})
	case leaseReply:
		until, ok := c.requests[m.Sequence]
		if ok && m.Roster.Ballot > c.revoked[m.From] && now.Before(until) && until.After(c.incoming[m.From].until) {
			c.incoming[m.From] = grant{until, m.Threshold}
		}
	case prepare:
		if m.From == c.current.Leader {
			c.sendSnapshot(m.From, m.PrepareStart)
		}
	case promise:
		if c.id == c.current.Leader && !c.prepared {
			c.addSnapshot(m.From, m.PrepareStart, m.Part, m.Parts, m.Entries, now)
		}
	case accept:
		if m.From == c.current.Leader && m.Entry.Ballot == c.current.Ballot {
			c.acceptEntry(m.Entry)
			c.emit(m.From, message{Kind: accepted, Prefix: c.acceptedPrefix, Entry: entry{Slot: m.Entry.Slot, Ballot: m.Entry.Ballot}})
			c.applyNoticedSlot(m.Entry.Slot)
			c.learnCommitNotice(0, 0, now)
		}
	case accepted:
		if c.id == c.current.Leader && c.prepared && m.Entry.Ballot == c.current.Ballot {
			if v, ok := c.log[m.Entry.Slot]; ok && v.Ballot == m.Entry.Ballot {
				if m.Prefix > c.acceptedProgress[m.From] {
					c.acceptedProgress[m.From] = m.Prefix
					c.updateReadPrefix()
				}
				c.votes[v.Slot] = c.votes[v.Slot].With(m.From)
				c.tryCommit(v.Slot, now)
			}
		}
	case commit:
		c.stats.CommitNotices++
		c.learnCommitNotice(m.CommitPrefix, m.CommitSlot, now)
	case committedEntry:
		if m.From == c.current.Leader {
			c.learnCommittedEntry(m.Entry)
		}
	case acceptNote:
		// Reserved wire kind: optional pre-commit read notifications are disabled.
	case repairRequest:
		if c.id == c.current.Leader && c.prepared {
			c.sendCommitRepair(m.From, m.RepairStart)
		}
	case forward:
		if c.id == c.current.Leader {
			c.submit(m.Request, now)
		}
	}
	c.release(now)
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

func (c *core) acceptEntry(v entry) {
	if v.Slot == 0 || v.Slot <= c.compacted {
		return
	}
	old, ok := c.log[v.Slot]
	if ok && (old.Ballot > v.Ballot || (old.Ballot == v.Ballot && !sameEntry(old, v))) {
		panic("bodega: conflicting accepted value")
	}
	if ok && c.committed[v.Slot] && !sameEntry(old, v) {
		panic("bodega: recovery changed a committed value")
	}
	c.log[v.Slot] = v
	for {
		next, ok := c.log[c.acceptedPrefix+1]
		if !ok || next.Ballot != c.current.Ballot {
			break
		}
		c.acceptedPrefix++
	}
	c.acceptedProgress[c.id] = c.acceptedPrefix
	if v.Slot > c.high {
		c.high = v.Slot
	}
	if v.Slot > c.next {
		c.next = v.Slot
	}
	if ok && !sameEntry(old, v) {
		delete(c.committed, v.Slot)
		c.reindex()
	} else {
		for _, r := range v.requests() {
			if r.Proposal.Command.Op == state.PUT && v.Slot > c.latest[r.Proposal.Command.K] {
				c.latest[r.Proposal.Command.K] = v.Slot
			}
		}
	}
}

func (c *core) reindex() {
	c.latest = map[state.Key]uint64{}
	c.latestCommitted = map[state.Key]uint64{}
	for s, v := range c.log {
		for _, r := range v.requests() {
			if r.Proposal.Command.Op != state.PUT {
				continue
			}
			k := r.Proposal.Command.K
			if s > c.latest[k] {
				c.latest[k] = s
			}
			if c.committed[s] && s > c.latestCommitted[k] {
				c.latestCommitted[k] = s
			}
		}
	}
}

func (c *core) submit(r request, now time.Time) {
	id := r.id()
	if value, ok := c.completed[id]; ok {
		c.finish(r, value)
		return
	}
	if !c.active() {
		c.enqueue(r)
		return
	}
	if c.current.Leader == c.id && !c.prepared {
		c.enqueue(r)
		return
	}
	if r.Proposal.Command.Op == state.GET && c.current.respondersFor(r.Proposal.Command.K)&bit(c.id) != 0 {
		if c.localReadAuthority(now) {
			s := c.latest[r.Proposal.Command.K]
			if c.current.Leader == c.id {
				// Figure 7: the stable leader reads the latest committed value.
				// A commit can precede prefix execution; use the same slot-value
				// lookup as responders, never an older state-machine value.
				s = c.latestCommitted[r.Proposal.Command.K]
			}
			if value, ok := c.readValue(s, r); ok {
				c.finishLocalRead(r, value)
				return
			}
			if _, ok := c.held[id]; !ok {
				if s > c.prefix && !c.committed[s] {
					c.stats.UncommittedReadHolds++
				}
				c.held[id] = pendingRead{r, s, now}
				if _, exists := c.holdWaiters[s]; !exists {
					heap.Push(&c.holdSlots, s)
				}
				c.holdWaiters[s] = append(c.holdWaiters[s], id)
				c.holdCounts[s]++
				c.queueFastSlot(s)
				c.stats.HeldReads++
			}
			delete(c.queued, id)
			return
		}
		c.stats.LeaseMisses++
	}
	if c.current.Leader != c.id {
		c.enqueue(r)
		c.stats.Forwarded++
		c.emit(c.current.Leader, message{Kind: forward, Request: r})
		return
	}
	if _, ok := c.inflight[id]; ok {
		return
	}
	delete(c.queued, id)
	c.admit(r, now)
}

// Notice evidence never supplies a value: only an Accept in the same ballot
// can satisfy it. In particular, recovery cannot commit a stale local tail.
func (c *core) applyNoticedSlot(slot uint64) {
	if slot == 0 || (slot > c.noticePrefix && !c.noticeSlots[slot]) {
		return
	}
	v, ok := c.log[slot]
	if !ok || v.Ballot != c.current.Ballot {
		return
	}
	if !c.committed[slot] {
		c.markCommitted(slot)
	}
	delete(c.noticeSlots, slot)
}

func (c *core) learnCommitNotice(prefix, slot uint64, now time.Time) {
	c.noticePrefix = maxSlot(c.noticePrefix, prefix)
	c.noticeHigh = maxSlot(c.noticeHigh, maxSlot(prefix, slot))
	if slot > c.prefix {
		c.noticeSlots[slot] = true
		c.applyNoticedSlot(slot)
	}
	// The executed prefix is already fixed. Check the unexecuted suffix
	// directly, without waiting to re-accept all earlier slots in this ballot.
	for s := c.prefix + 1; s <= c.noticePrefix; s++ {
		v, ok := c.log[s]
		if !ok || v.Ballot != c.current.Ballot {
			break
		}
		c.applyNoticedSlot(s)
	}
	c.apply()
	c.requestCommitRepair(now)
}

func (c *core) tryCommit(s uint64, now time.Time) {
	if !c.active() || !c.prepared || c.current.Leader != c.id || c.committed[s] {
		return
	}
	v := c.log[s]
	mask := c.votes[s]
	if mask.Size() < c.majority {
		return
	}
	responders := uint64(0)
	for _, r := range v.requests() {
		if r.Proposal.Command.Op == state.PUT {
			responders |= c.current.respondersFor(r.Proposal.Command.K)
		}
	}
	if !mask.Covers(replicaset.Set(responders)) {
		c.stats.CoverageWaits++
		return
	}
	c.markCommitted(s)
	c.stats.Commits++
	c.apply()
	c.broadcast(message{Kind: commit, CommitPrefix: c.prefix, CommitSlot: s})
	c.release(now)
}

func (c *core) markCommitted(s uint64) {
	c.committed[s] = true
	for _, r := range c.log[s].requests() {
		cmd := r.Proposal.Command
		if cmd.Op == state.PUT && s > c.latestCommitted[cmd.K] {
			c.latestCommitted[cmd.K] = s
		}
	}
	c.queueFastSlot(s)
}

func (c *core) activate(now time.Time) {
	for i := range c.seen {
		c.refreshPeer(i, now)
	}
}
