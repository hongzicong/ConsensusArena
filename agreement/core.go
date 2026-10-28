package agreement

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
	KeyPolicy                                     *keyPolicy
	AllKeyLearning                                bool
	ClientLearning                                bool
	Learners                                      uint64
	Completion                                    []completionSpan
	Lease, Margin, Heartbeat, Failure, FailureMax time.Duration
	Responders                                    uint64
	Ranges                                        []ResponderRange
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
	RepairDataSent, RepairDataSuppressed                                                                                        uint64
	AllKeyVoteCommands, HotCertificateVoteCommands, CertificateDispatchSamples, CertificateDispatchNanos, HotCertifiedReadHolds uint64
	ClientVoteFrames, ClientVoteBytes, ClientVoteErrors, ClientVoteCommands                                                     uint64
	DirectAdmissionArrivals, DirectFreshAdmissions, DirectRepeatedArrivals, DirectForwardAdmissions                             uint64
	ResultHints, ResultLocalExecuted, DeferredLoggedResults                                                                     uint64
	VoteRecipientSamples, VoteRecipientCount, OmittedVoteRecipients                                                             uint64
	CoalescedAttempts, FanoutAttempts, FanoutEnqueued                                                                           uint64
	OwnerShortcutReads, OwnPendingBypassReads, OwnerRiskHolds, OwnerPrefixHolds                                                 uint64
	DelegatedWrites, OwnerIngressWrites, WrongOriginWrites, DeferredOwnerResults                                                uint64
	LearnerCoverageWaits                                                                                                        uint64
	DirectVotesSent, OrphanVotes, LearnerCommits                                                                                uint64
	LearnerWaitSamples, LearnerWaitNanos, IngressSamples, IngressNanos                                                          uint64
	HeldReadSamples, HeldReadNanos                                                                                              uint64
	IngressHistogram                                                                                                            [32]uint64
	MajoritySamples, MajorityNanos, CoverageSamples, CoverageNanos                                                              uint64
	MajorityHistogram, CoverageHistogram                                                                                        [32]uint64
	LocalReads, HeldReads, FallbackReads, Forwarded, Writes, Commits, RosterChanges                                             uint64
	LeaseMisses, CoverageWaits, RecoveredSlots, Messages                                                                        uint64
	RetriedRequests, DroppedMessages                                                                                            uint64
	OutOfOrderReads                                                                                                             uint64
	StableLeaderReads, UncommittedReadHolds                                                                                     uint64
	CommitNotices, CommitRepairRequests, CommitRepairEntries                                                                    uint64
	Batches, BatchCommands, MaxBatchCommands                                                                                    uint64
	PrepareEntries, PrepareCompleted, PrepareNanos, Revokes, RevokeAcks                                                         uint64
	CompactedSlots                                                                                                              uint64
}

// engine has no goroutines. All callbacks and transitions are serialized.
type engine struct {
	repairSent                    map[int]map[uint64]time.Time
	auxRoles                      func() uint64
	intercept                     func(request, time.Time) bool
	applySpecial                  func(request, uint64) bool
	strictPrefix                  bool
	bridgeSlot                    uint64
	beforeApply                   func(entry) bool
	finishSpecial                 func(request)
	admissionPaused               bool
	clientVote                    func(entry)
	observedLearners              uint64
	latestRemote                  map[state.Key]uint64 // writes that may complete away from the current leader
	phaseTimes                    map[uint64]phaseTiming
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
		latestRemote:    map[state.Key]uint64{},
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
		if e.current.Leader == e.id && (e.current.allResponders()|e.additionalRoles()) & ^healthy != 0 {
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
				if p == e.id || !e.peerHealthy(p, now) {
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
			e.stats.ResultHints++
			if m.Request.Proposal.Command.Op == state.PUT || m.Request.Proposal.Command.Op == state.SCAN {
				if value, ok := e.completed[m.Request.id()]; ok {
					e.stats.ResultLocalExecuted++
					e.finish(m.Request, value)
				} else {
					e.stats.DeferredOwnerResults++
					e.stats.DeferredLoggedResults++
				}
			} else {
				e.finish(m.Request, m.Value)
			}
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
			e.votes[m.Entry.Slot] = e.votes[m.Entry.Slot].With(e.id)
			if !e.committed[m.Entry.Slot] {
				if e.phaseTimes == nil {
					e.phaseTimes = map[uint64]phaseTiming{}
				}
				if _, ok := e.phaseTimes[m.Entry.Slot]; !ok {
					e.phaseTimes[m.Entry.Slot] = phaseTiming{Start: now}
				}
			}
			e.updateReadPrefix()
			e.broadcastVote(m.Entry.Slot)
			e.tryCommit(m.Entry.Slot, now)
			e.applyNoticedSlot(m.Entry.Slot)
			e.learnCommitNotice(0, 0, now)
		}
	case accepted:
		if m.Entry.Ballot == e.current.Ballot && m.Entry.Slot > e.compacted && m.Entry.Slot <= e.high+4096 {
			if m.Prefix > e.acceptedProgress[m.From] {
				e.acceptedProgress[m.From] = m.Prefix
				e.updateReadPrefix()
			}
			if _, ok := e.log[m.Entry.Slot]; !ok {
				e.stats.OrphanVotes++
			}
			e.votes[m.Entry.Slot] = e.votes[m.Entry.Slot].With(m.From)
			e.tryCommit(m.Entry.Slot, now)
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
		if xs[i].Stage != ys[i].Stage || xs[i].Scan != ys[i].Scan || xs[i].Origin != ys[i].Origin || x.ClientId != y.ClientId || x.CommandId != y.CommandId || x.Command.Op != y.Command.Op || x.Command.K != y.Command.K || !bytes.Equal(x.Command.V, y.Command.V) {
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
	for _, req := range v.requests() {
		if req.Proposal.Command.Op == bridgeOp {
			e.bridgeSlot = v.Slot
		}
	}
	for _, req := range v.requests() {
		if req.Origin >= 0 && req.Origin < e.n {
			e.observedLearners |= bit(req.Origin)
		}
	}
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
		gate := e.completionGate(v)
		for _, r := range v.requests() {
			if r.Proposal.Command.Op == state.PUT && v.Slot > e.latest[r.Proposal.Command.K] {
				e.latest[r.Proposal.Command.K] = v.Slot
			}
			if r.Proposal.Command.Op == state.PUT && e.remoteCompletionRisk(v, r, gate) && v.Slot > e.latestRemote[r.Proposal.Command.K] {
				e.latestRemote[r.Proposal.Command.K] = v.Slot
			}
		}
	}
}

func (e *engine) reindex() {
	e.bridgeSlot = 0
	for s, v := range e.log {
		for _, req := range v.requests() {
			if req.Proposal.Command.Op == bridgeOp {
				e.bridgeSlot = s
			}
		}
	}
	e.latest = map[state.Key]uint64{}
	e.latestCommitted = map[state.Key]uint64{}
	e.latestRemote = map[state.Key]uint64{}
	for s, v := range e.log {
		gate := e.completionGate(v)
		for _, r := range v.requests() {
			if r.Proposal.Command.Op != state.PUT {
				continue
			}
			k := r.Proposal.Command.K
			if s > e.latest[k] {
				e.latest[k] = s
			}
			if e.remoteCompletionRisk(v, r, gate) && s > e.latestRemote[k] {
				e.latestRemote[k] = s
			}
			if e.committed[s] && s > e.latestCommitted[k] {
				e.latestCommitted[k] = s
			}
		}
	}
}

func (e *engine) submit(r request, now time.Time) {
	id := r.id()
	if e.intercept != nil && e.intercept(r, now) {
		return
	}
	if e.admissionPaused && r.Stage == 0 {
		e.enqueue(r)
		return
	}
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
			ownerRead := e.completionOwner(r.Proposal.Command.K) == e.id
			delegated := e.delegatedKey(r.Proposal.Command.K)
			if ownerRead {
				// The owner withholds its ACCEPTED until local commitment, so no
				// other responder can expose a bypassed write first. Other writes
				// (including recovery) must block the committed-value shortcut.
				s = e.latestCommitted[r.Proposal.Command.K]
				if pending := e.latestRemote[r.Proposal.Command.K]; pending > s {
					s = pending
				}
			}
			if value, ok := e.readValue(s, r); ok {
				if ownerRead && delegated {
					e.stats.OwnerShortcutReads++
					if e.latest[r.Proposal.Command.K] > s {
						e.stats.OwnPendingBypassReads++
					}
				}
				e.finishLocalRead(r, value, now)
				return
			}
			if _, ok := e.held[id]; !ok {
				if ownerRead && delegated {
					if e.latestRemote[r.Proposal.Command.K] > e.latestCommitted[r.Proposal.Command.K] {
						e.stats.OwnerRiskHolds++
					} else {
						e.stats.OwnerPrefixHolds++
					}
				}
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

func (e *engine) remoteCompletionRisk(v entry, r request, gate int) bool {
	return !e.ownerMayBypass(v, r) || gate != e.completionOwner(r.Proposal.Command.K)
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
	if !e.active() || (e.current.Leader == e.id && !e.prepared) || e.committed[s] {
		return
	}
	v, ok := e.log[s]
	if !ok || v.Ballot != e.current.Ballot {
		return
	}
	mask := e.votes[s]
	if mask.Size() < e.majority {
		return
	}
	e.observeMajority(s, now)
	responders := uint64(0)
	for _, r := range v.requests() {
		if r.Proposal.Command.Op == state.PUT {
			responders |= e.current.respondersFor(r.Proposal.Command.K)
		}
	}
	if !mask.Covers(replicaset.Set(responders)) {
		if e.id == e.current.Leader {
			e.stats.CoverageWaits++
		} else {
			e.stats.LearnerCoverageWaits++
		}
		return
	}
	e.markCommitted(s)
	e.observeCoverage(s, now)
	if e.id == e.current.Leader {
		e.stats.Commits++
	} else {
		e.stats.LearnerCommits++
	}
	e.apply()
	if e.completionGate(v) == e.id {
		// markCommitted advanced the owner's read target before this vote can
		// let any other replica (or client learner) expose the new value.
		e.broadcastVote(s)
	}
	if e.id == e.current.Leader {
		e.broadcast(message{Kind: commit, CommitPrefix: e.prefix, CommitSlot: s})
	}
	e.release(now)
}

func (e *engine) broadcastVote(slot uint64) {
	v, ok := e.log[slot]
	if !ok || v.Ballot != e.current.Ballot {
		return
	}
	if e.completionGate(v) == e.id && !e.committed[slot] {
		return
	}
	if e.clientVote != nil {
		e.clientVote(v)
	}
	mask := e.opt.Learners | e.observedLearners | e.current.allResponders() | bit(e.current.Leader)
	if e.opt.Learners == 0 || !v.ReadFresh {
		mask = (uint64(1) << uint(e.n)) - 1
	}
	for _, req := range v.requests() {
		if req.Origin >= 0 && req.Origin < e.n {
			mask |= bit(req.Origin)
		}
	}
	mask &^= bit(e.id)
	count := uint64(bits.OnesCount64(mask))
	e.stats.VoteRecipientSamples++
	e.stats.VoteRecipientCount += count
	e.stats.OmittedVoteRecipients += uint64(e.n-1) - count
	e.stats.DirectVotesSent += count
	for to := 0; to < e.n; to++ {
		if mask&bit(to) != 0 {
			e.emit(to, message{Kind: accepted, Prefix: e.acceptedPrefix, Entry: entry{Slot: slot, Ballot: v.Ballot}})
		}
	}
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

func (e *engine) additionalRoles() uint64 {
	if e.auxRoles != nil {
		return e.auxRoles()
	}
	return 0
}
