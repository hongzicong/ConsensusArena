package kcensus

// Protocol state and normal-case transitions.
import (
	"fmt"
	"math/bits"
	"time"

	"github.com/hongzicong/ConsensusArena/replica/defs"
	"github.com/hongzicong/ConsensusArena/replicaset"
	"github.com/hongzicong/ConsensusArena/state"
)

type slotID struct {
	Key  state.Key
	Slot uint64
}

type slotState struct {
	fast                                *Value
	values                              map[int]*Value
	graphs                              map[int]*graphRun
	nodeReports                         map[int]nodeReport
	classicValues                       map[uint64]*Value
	payloadRelayed                      map[graphEdge]bool
	conflictAnnounced                   bool
	fetchSent                           bool
	fetchTarget                         int
	nextRetry, retryDelay               time.Duration
	knowledge                           []replicaset.Set
	knowledgeTime                       time.Duration
	frozen                              bool
	participants, announcedParticipants uint64
	promised, acceptedBallot            uint64
	classic, decided                    *Value
	local                               *Value
	ballot                              uint64
	phase                               uint8 // 0: evidence; 1: census/prepare; 2: accept
	reports                             map[int]message
	selected                            *Value
	acks                                uint64
	prepareSent, acceptSent             replicaset.Set
	followedBallot                      uint64
	created, lastSend                   time.Duration
}

type shard struct {
	high, executed uint64
	slots          map[uint64]*slotState
	pending        []defs.RequestID
	queued         map[defs.RequestID]bool
	shared         map[defs.RequestID]bool
	objects        map[uint64]*Value
	nextUID        uint64
	pendingValue   *Value
	scheduled      bool
	gapScheduled   bool
	nextOffer      time.Duration
}

type Stats struct {
	FastCommits, FallbackCommits, Conflicts, Prepares, Retries, Fetches   uint64
	SpreadMessages, KnowledgeUpdates, Executed, Deduplicated              uint64
	Reads, ReadWaits, ReadWaitNanos, Decisions                            uint64
	AcceptanceEdges, WitnessEdges, RetryDeferred                          uint64
	OfferedCommands, BatchCommands, MaxBatchCommands                      uint64
	LocalWrites, LocalWriteNanos, LocalWriteMaxNanos, LocalSlotWaits      uint64
	LocalWriteLatencyBuckets                                              [9]uint64
	Reproposals, InitialCensuses, TimeoutCensuses                         uint64
	OriginalProposals, BusyProposalSlots, SharedProposalDeferrals         uint64
	GraphMessages, GraphPayloads, GraphStates, FrozenRelays               uint64
	ReadyReadReplies, BufferedReadReplies                                 uint64
	ReferenceMessages, PayloadQueries, PayloadRepairs, PayloadWaits       uint64
	NodeStateRelays                                                       uint64
	HeartbeatReplies, StaleHeartbeatReplies, FailedPeers, WrongBatchSlots uint64
	ReadTimerVisits, GapTimerVisits, TickNanos, TickMaxNanos              uint64
	FailedGraphBypasses, RecoveryResultReplies                            uint64
	SuppressedRecoveryRequests, RetiredRecoveryReplies                    uint64
	SuppressedReadRequests, DuplicateReadReplies                          uint64
	SuppressedFetches, SuppressedRepairOffers                             uint64
	ImmediateFetches, DeadlineFetches, DeferredInFlightFetches            uint64
	RecoveryAdmissionWaits                                                uint64
	EncodedFrames, EncodedBytes, EncodingSamples, EncodingNanos           uint64
	ReusedEncodings                                                       uint64
	InputMessages, InputSamples, InputSampleNanos                         [resultQuery + 1]uint64
}

type core struct {
	id, n, m, f         int
	plan                Plan
	priority            []int
	shards              map[state.Key]*shard
	active              map[slotID]*slotState
	retryQueue          []slotID
	retryHead           int
	retryBudget         int
	activeCensuses      int
	known               map[defs.RequestID]Record
	commandUIDs         map[defs.RequestID]uint64
	results             map[defs.RequestID]state.Value
	ordered             map[defs.RequestID]bool
	reads               map[defs.RequestID]*pendingRead
	readQueue           []defs.RequestID
	readHead            int
	readsByKey          map[state.Key]map[defs.RequestID]*pendingRead
	gaps                map[state.Key]bool
	gapQueue            []state.Key
	gapHead             int
	out                 []envelope
	now, lastBeat       time.Duration
	heard               []time.Duration
	failed              []bool
	failedGraphs        []bool
	connected           []bool
	probeSequence       uint64
	probes              [8]heartbeatProbe
	failure, retry      time.Duration
	execute             func(Record) state.Value
	complete            func(defs.RequestID, state.Value)
	stats               Stats
	localWrites         map[defs.RequestID]writeArrival
	pendingKeys         []state.Key
	pendingHead         int
	future              map[state.Key][]message
	payloadWaiting      map[payloadWaitKey]*payloadWait
	payloadDependencies map[payloadObject]map[payloadWaitKey]bool
	payloadReady        []payloadObject
	payloadRotation     []*payloadWait
	payloadHead         int
	payloadRetry        time.Duration
	payloadDraining     bool
	payloadSeen         map[payloadEdgeID]bool
	repairHandoffs      map[payloadObject]uint64 // key/Single UID -> reliable-stream destinations
}

func newCore(id int, plan Plan, failure time.Duration) *core {
	if err := validateRequirements(plan.Requirements, plan.Voters); err != nil {
		panic(err)
	}
	m := len(plan.Requirements)
	n := plan.Voters
	if len(plan.Graphs) != m {
		panic("KCensus requires a synthesized propagation graph")
	}
	if id < 0 || id >= m {
		panic("invalid replica ID")
	}
	if failure <= 0 {
		failure = 1500 * time.Millisecond
	}
	return &core{id: id, n: n, m: m, f: n / 2, plan: plan, priority: plan.leaderPriority(), shards: make(map[state.Key]*shard), active: make(map[slotID]*slotState), known: make(map[defs.RequestID]Record), commandUIDs: make(map[defs.RequestID]uint64), results: make(map[defs.RequestID]state.Value), ordered: make(map[defs.RequestID]bool), reads: make(map[defs.RequestID]*pendingRead), readsByKey: make(map[state.Key]map[defs.RequestID]*pendingRead), gaps: make(map[state.Key]bool), heard: make([]time.Duration, m), failure: failure, retry: failure / 3, localWrites: make(map[defs.RequestID]writeArrival), future: make(map[state.Key][]message)}
}

func (c *core) shard(k state.Key) *shard {
	s := c.shards[k]
	if s == nil {
		s = &shard{objects: make(map[uint64]*Value), nextUID: uint64(2 * c.id), slots: make(map[uint64]*slotState), queued: make(map[defs.RequestID]bool), shared: make(map[defs.RequestID]bool)}
		c.shards[k] = s
	}
	return s
}

func (c *core) slot(k state.Key, i uint64) *slotState {
	if i == 0 {
		panic("slot zero is reserved")
	}
	s := c.shard(k)
	if i > s.high {
		s.high = i
	}
	if s.executed < s.high {
		c.markGap(k)
	}
	x := s.slots[i]
	if x == nil {
		x = &slotState{knowledge: make([]replicaset.Set, c.m), created: c.now, nextRetry: c.now + c.retry, retryDelay: c.retry}
		s.slots[i] = x
		c.active[slotID{k, i}] = x
		c.retryQueue = append(c.retryQueue, slotID{k, i})
	}
	return x
}

func (c *core) send(to int, m message) {
	c.sendShared(to, m, nil)
}

func (c *core) sendShared(to int, m message, encoded *[]byte) {
	c.emit(envelope{To: to, Message: m, Encoded: encoded})
}

func (c *core) emit(e envelope) {
	e.Message.From = c.id
	count := e.Count
	if count == 0 {
		count = 1
	}
	if e.Message.References {
		c.stats.ReferenceMessages += uint64(count)
	}
	c.out = append(c.out, e)
	if e.Message.Kind == spread {
		c.stats.SpreadMessages += uint64(count)
	}
}

func (c *core) broadcast(m message) {
	c.emit(envelope{Count: c.n, Message: m})
}

func (c *core) broadcastAll(m message) {
	c.emit(envelope{Count: c.m, Message: m})
}

func (c *core) broadcastCommit(m message) {
	var encoded, fullEncoded []byte
	full := m
	if m.Ballot != 0 {
		full = c.valuePayload(m)
	}
	send := func(to int) {
		packet := m
		image := &encoded
		if to >= c.n {
			packet = full
			image = &fullEncoded
		}
		c.sendShared(to, packet, image)
	}
	// Native prioritizes a Single's requester on commit dissemination.
	first := -1
	if m.Value != nil && m.Value.UID&1 == 0 {
		first = int((m.Value.UID >> 1) % uint64(c.m))
		send(first)
	}
	for i := 0; i < c.m; i++ {
		if i != first {
			send(i)
		}
	}
}

func (c *core) remember(r Record) {
	if old, ok := c.known[r.ID]; ok {
		if !sameRecord(old, r) {
			panic("client request ID reused")
		}
	} else {
		r.Command.V = append(state.Value(nil), r.Command.V...)
		c.known[r.ID] = r
	}
}

func (c *core) submit(r Record) error {
	if err := commandPolicy.Validate(r.Command); err != nil {
		return err
	}
	c.remember(r)
	if v, ok := c.results[r.ID]; ok {
		if c.complete != nil {
			c.complete(r.ID, v)
		}
		return nil
	}
	if r.Command.Op == state.GET {
		if c.id >= c.n {
			return fmt.Errorf("non-voting GET requires a delegate")
		}
		if c.reads[r.ID] == nil {
			q := &pendingRead{record: r, started: c.now, lastSend: c.now, fences: make(map[int]uint64)}
			c.reads[r.ID] = q
			c.readQueue = append(c.readQueue, r.ID)
			if c.readsByKey[r.Command.K] == nil {
				c.readsByKey[r.Command.K] = make(map[defs.RequestID]*pendingRead)
			}
			c.readsByKey[r.Command.K][r.ID] = q
			c.stats.Reads++
			c.sendMissingReadQueries(q)
		}
		return nil
	}
	single := c.singleValue(r)
	s := c.shard(r.Command.K)
	if _, exists := c.localWrites[r.ID]; !exists {
		c.localWrites[r.ID] = writeArrival{c.now, s.executed}
	}
	c.enqueue(r, false)
	c.propose(r.Command.K)
	if !s.shared[r.ID] {
		s.shared[r.ID] = true
		c.offerValue(r.Command.K, single, !c.graphUsable(c.id))
	}
	return nil
}

func (c *core) propose(k state.Key) {
	s := c.shard(k)
	for len(s.pending) > 0 {
		if !c.ordered[s.pending[0]] {
			break
		}
		delete(s.queued, s.pending[0])
		s.pending = s.pending[1:]
	}
	if len(s.pending) == 0 {
		return
	}
	usable := c.graphUsable(c.id)
	if !usable && c.coordinator() != c.id {
		return
	}
	if !usable && c.activeCensuses >= recoveryPipeline {
		c.stats.RecoveryAdmissionWaits++
		return
	}
	i := s.executed + 1
	x := s.slots[i]
	if x != nil && (x.local != nil || x.fast != nil || len(x.values) > 0 || x.frozen || x.decided != nil) {
		c.stats.BusyProposalSlots++
		return
	}
	isShared := true
	// Only the selected reproposer may propose shared backlog. Avoid sorting
	// that backlog at every non-owner before making this same scheduling check.
	if len(s.shared) > 0 && c.reproposer() != c.id {
		c.stats.SharedProposalDeferrals++
		return
	}
	shared := c.pendingRecords(k, &isShared)
	var v *Value
	if len(shared) > 0 {
		if c.reproposer() != c.id {
			return
		}
		v = c.makePendingValue(k, shared)
		c.stats.Reproposals++
	} else {
		v = c.pendingBatch(k, false)
		if len(v.Records) > 0 {
			c.stats.OriginalProposals++
		}
	}
	if len(v.Records) == 0 {
		return
	}
	for _, r := range v.Records {
		s.shared[r.ID] = true
	}
	x = c.slot(k, i)
	x.local = v
	x.participants |= 1 << c.id
	if !usable {
		// Freeze the original instance at a majority before choosing any value.
		c.stats.FailedGraphBypasses++
		c.startRecovery(k, i)
		return
	}
	// A Batch descriptor is new even when every member Single is already known.
	c.startGraph(k, i, x, v, len(shared) == 0 || v.Batch != nil)
}

func (c *core) validValue(k state.Key, v *Value, fast bool) bool {
	if v == nil || v.Proposer < 0 || v.Proposer >= c.m || len(v.Records) > maxBatch || (fast && len(v.Records) == 0) {
		return false
	}
	seen := map[defs.RequestID]bool{}
	bytes := 0
	for _, r := range v.Records {
		bytes += recordBytes(r)
		if bytes > maxBatchBytes {
			return false
		}
		if r.Command.K != k || r.Command.Op != state.PUT || len(r.Command.V) > state.MaxValueBytes || seen[r.ID] {
			return false
		}
		seen[r.ID] = true
	}
	return true
}

func (c *core) canCommit(x *slotState) bool {
	if x.fast == nil || c.plan.Leaders[x.fast.Proposer] != c.id || x.decided != nil {
		return false
	}
	for w, req := range c.plan.Requirements[x.fast.Proposer] {
		if !x.knowledge[w].Covers(req) {
			return false
		}
	}
	return true
}

func (c *core) learn(k state.Key, i uint64, v *Value) {
	x := c.slot(k, i)
	if x.decided != nil {
		if !sameValue(x.decided, v) {
			panic(fmt.Sprintf("conflicting decisions key=%d slot=%d", k, i))
		}
		return
	}
	x.decided = v
	if x.phase != 0 {
		c.activeCensuses--
	}
	// A different value may win while this node is waiting for the remaining
	// dependencies of a payload-arrival state. Preserve the losing input's
	// value-only forwarding even though this slot's evidence stops progressing.
	for p := 0; p < c.m; p++ {
		r := x.graphs[p]
		if r == nil || !r.NewValue || x.values[p] == nil || x.values[p].Reference {
			continue
		}
		for edge := range r.Received {
			if c.plan.Graphs[p].Payload[edge] {
				c.relayPayload(message{From: edge.From, Proposer: p, Key: k, Slot: i, GraphTime: int64(edge.Time), Value: x.values[p]})
			}
		}
	}
	c.stats.Decisions++
	c.stats.BatchCommands += uint64(len(v.Records))
	if uint64(len(v.Records)) > c.stats.MaxBatchCommands {
		c.stats.MaxBatchCommands = uint64(len(v.Records))
	}
	delete(c.active, slotID{k, i})
	for _, r := range v.Records {
		c.remember(r)
	}
	c.apply(k)
}

func (c *core) step(m message) {
	c.stepMessage(m, false)
}

func (c *core) stepMessage(m message, replayed bool) {
	if m.From < 0 || m.From >= c.m {
		return
	}
	if m.Kind == heartbeat {
		c.receiveHeartbeat(m)
		return
	}
	if m.Kind == delegateRead {
		if c.id >= c.n || m.From < c.n || m.Request == nil || m.Request.ID.Client != int32(m.From) || m.Request.Command.Op != state.GET || m.Request.Command.K != m.Key {
			return
		}
		if err := c.submit(*m.Request); err != nil {
			panic(err)
		}
		return
	}
	if m.Kind == resultQuery {
		if c.id >= c.n || m.ID.Client != int32(m.From) {
			return
		}
		if v, ok := c.results[m.ID]; ok {
			c.send(m.From, message{Kind: executedResult, Key: m.Key, ID: m.ID, Result: v})
		}
		return
	}
	if m.Kind == executedResult {
		return
	}
	if m.Kind == readReply && m.Value == nil && m.Fast == nil && len(m.Values) == 0 && len(m.Reports) == 0 && len(m.Knowledge) == 0 && len(m.UIDs) == 0 {
		q := c.reads[m.ID]
		if q == nil || q.record.Command.K != m.Key {
			return
		}
		if _, seen := q.fences[m.From]; seen {
			c.stats.DuplicateReadReplies++
			return
		}
	}
	// A learned old Commit needs neither its payload nor another slot transition.
	if m.Kind == commit && m.Slot > 0 && m.Slot <= c.executed(m.Key) {
		return
	}
	if m.Slot > 0 && m.Slot <= c.executed(m.Key) && (m.Kind == promise || m.Kind == accepted || m.Kind == nack) {
		c.stats.RetiredRecoveryReplies++
		return
	}
	if m.Kind == payloadQuery {
		c.answerPayload(m)
		return
	}
	if m.Kind == payloadReply {
		for _, v := range m.Values {
			if err := c.storeValue(m.Key, v); err != nil {
				panic(err)
			}
			resolved, ready := c.materialize(m.Key, v)
			if ready {
				c.enqueueValue(resolved)
			}
		}
		c.stats.PayloadRepairs++
		c.drainPayload()
		c.propose(m.Key)
		return
	}
	if m.Kind == prepare || m.Kind == promise || m.Kind == accept || m.Kind == commit {
		for _, v := range m.Values {
			if v == nil || v.Batch != nil || v.Reference {
				return
			}
			if err := c.storeValue(m.Key, v); err != nil {
				panic(err)
			}
		}
	}
	if !c.resolveMessage(&m) {
		c.waitPayload(m)
		return
	}
	defer c.drainPayload()
	for _, r := range m.Reports {
		if r.Value != nil && !r.Value.Reference && !c.validValue(m.Key, r.Value, false) {
			return
		}
	}
	if m.Kind == readQuery {
		if c.id >= c.n {
			return
		}
		reply := message{Kind: readReply, Key: m.Key, ID: m.ID}
		reply.High = c.readFence(m.Key)
		c.send(m.From, reply)
		return
	}
	if m.Kind == readReply {
		if m.From >= c.n {
			return
		}
		q := c.reads[m.ID]
		if q == nil || q.record.Command.K != m.Key {
			return
		}
		if m.Value != nil && m.Slot > 0 && c.validValue(m.Key, m.Value, false) {
			c.learn(m.Key, m.Slot, m.Value)
			q = c.reads[m.ID]
			if q == nil {
				return
			}
		}
		// The first response from a voter describes that read's observation.
		// Retransmissions cannot raise its fence or count it a second time.
		if _, seen := q.fences[m.From]; !seen {
			q.fences[m.From] = m.High
		}
		c.finishRead(m.ID, q)
		if c.reads[m.ID] != nil && q.target > c.executed(m.Key) {
			c.repairPrefix(m.Key, q.target, true)
		}
		return
	}
	if m.Kind == offer {
		if !c.validValue(m.Key, m.Value, true) {
			return
		}
		c.enqueueValue(m.Value)
		c.relayPayload(m)
		c.propose(m.Key)
		return
	}
	if m.Value != nil && m.Value.Batch != nil && m.Value.Batch.Slot != m.Slot {
		c.stats.WrongBatchSlots++
		return
	}
	if m.Slot == 0 {
		return
	}
	if m.Value != nil && !c.validValue(m.Key, m.Value, m.Kind == spread) {
		return
	}
	if m.Fast != nil && !c.validValue(m.Key, m.Fast, true) {
		return
	}
	// As in upstream's slot layer, future-slot messages cannot produce an
	// acceptance before the preceding prefix executes. GET fences can then
	// refer to the current acceptance rather than metadata's highest slot.
	if m.Slot > c.executed(m.Key)+1 {
		c.slot(m.Key, m.Slot)
		c.future[m.Key] = append(c.future[m.Key], m)
		return
	}
	if m.Kind == spread || m.Kind == promise || m.Kind == accept {
		c.enqueueValue(m.Value)
		c.enqueueValue(m.Fast)
	}
	x := c.slot(m.Key, m.Slot)
	c.cacheClassic(x, m)
	if m.Kind == commit {
		if m.Value != nil {
			c.learn(m.Key, m.Slot, m.Value)
		}
		return
	}
	if x.decided != nil {
		if m.Kind == spread && m.Value != nil {
			c.relayPayload(m)
		}
		if m.Kind == fetch || m.Kind == prepare || m.Kind == accept || m.Kind == spread {
			// Prefix catch-up must not depend on the bounded UID cache repair
			// timer when a survivor never received the original phase-2 body.
			c.send(m.From, c.valuePayload(message{Kind: commit, Key: m.Key, Slot: m.Slot, Value: x.decided}))
		}
		c.propose(m.Key)
		return
	}
	switch m.Kind {
	case resumeCensus:
		c.completeCensus(m.Key, m.Slot, x)
	case spread:
		c.receiveGraph(m, x)
	case abandon:
		x.participants |= m.Mask & ((uint64(1) << c.m) - 1)
		c.freezeConflict(m.Key, m.Slot, x)
	case prepare:
		if c.id >= c.n || m.From >= c.n {
			return
		}
		if m.Ballot == 0 || int(m.Ballot%uint64(c.n)) != m.From {
			return
		}
		if m.Ballot < x.promised {
			c.send(m.From, message{Kind: nack, Key: m.Key, Slot: m.Slot, Ballot: x.promised})
			return
		}
		x.promised = m.Ballot
		c.freezeFor(x, m.Ballot)
		x.followedBallot = m.Ballot
		if !x.conflictAnnounced {
			x.conflictAnnounced = true
			x.announcedParticipants = x.participants
			c.broadcast(message{Kind: abandon, Key: m.Key, Slot: m.Slot, Mask: x.participants})
		}
		r := x.nodeReports[c.id]
		fast := x.fast
		if x.acceptedBallot > 0 {
			fast = nil
		}
		c.send(m.From, c.valuePayload(message{Kind: promise, Key: m.Key, Slot: m.Slot, Ballot: m.Ballot, AcceptedBallot: x.acceptedBallot, Value: x.classic, Fast: fast, Mask: r.Mask, StateTime: r.Time}))
	case promise:
		if c.id >= c.n || m.From >= c.n {
			return
		}
		c.cachePromise(x, m)
		if m.Ballot == uint64(c.n+c.id) {
			c.initialCensus(m.Key, m.Slot, x)
		}
		if x.phase != 1 || m.Ballot != x.ballot || x.promised > x.ballot {
			return
		}
		if old, ok := x.reports[m.From]; ok && old.AcceptedBallot >= m.AcceptedBallot {
			return
		}
		x.reports[m.From] = m
		c.completeCensus(m.Key, m.Slot, x)
	case accept:
		if c.id >= c.n || m.From >= c.n {
			return
		}
		if m.Value == nil || m.Ballot == 0 || int(m.Ballot%uint64(c.n)) != m.From {
			return
		}
		if m.Ballot < x.promised {
			c.send(m.From, message{Kind: nack, Key: m.Key, Slot: m.Slot, Ballot: x.promised})
			return
		}
		if x.acceptedBallot == m.Ballot && !sameValue(x.classic, m.Value) {
			panic("coordinator changed value within ballot")
		}
		x.frozen = true
		x.promised = m.Ballot
		x.acceptedBallot = m.Ballot
		x.followedBallot = m.Ballot
		x.classic = m.Value
		c.freezeFor(x, m.Ballot)
		for _, r := range m.Value.Records {
			c.remember(r)
		}
		c.send(m.From, message{Kind: accepted, Key: m.Key, Slot: m.Slot, Ballot: m.Ballot})
	case accepted:
		if c.id >= c.n || m.From >= c.n {
			return
		}
		if x.phase != 2 || m.Ballot != x.ballot || x.promised > x.ballot || (m.Value != nil && !sameValue(x.selected, m.Value)) {
			return
		}
		x.acks |= 1 << m.From
		c.mergeReport(x, nodeReport{From: m.From, Proposer: -1, Ballot: m.Ballot, AcceptedBallot: m.Ballot, Value: x.selected})
		if bits.OnesCount64(x.acks) >= c.n-c.f {
			c.stats.FallbackCommits++
			v := x.selected
			c.broadcastCommit(message{Kind: commit, Key: m.Key, Slot: m.Slot, Ballot: x.ballot, Value: v, References: true})
			c.learn(m.Key, m.Slot, v)
		}
	case nack:
		if m.Ballot > x.promised {
			c.freezeFor(x, m.Ballot)
			x.conflictAnnounced = true
		}
	case fetch:
		// The active slot participates in periodic recovery. A request need not
		// invent a fast acceptance merely to initiate a frozen census.
	}
	c.tryGraphCensus(m.Key, m.Slot, x)
}

func (c *core) tick(now time.Duration) {
	c.now = now
	c.heard[c.id] = now
	if now-c.lastBeat >= c.retry {
		c.probeSequence++
		c.probes[c.probeSequence%uint64(len(c.probes))] = heartbeatProbe{c.probeSequence, now}
		c.broadcastAll(message{Kind: heartbeat, Digest: c.plan.Digest, High: c.probeSequence})
		c.lastBeat = now
	}
	c.retryBudget = 256
	c.retryPending()
	c.retryPayload()
	coord := c.coordinator()
	// Round-robin bounded traversal: large backlogs cannot monopolize the loop
	// or starve slots behind the budget. Retired slots leave the rotation.
	remaining := len(c.retryQueue) - c.retryHead
	if remaining > 4096 {
		remaining = 4096
	}
	for ; remaining > 0; remaining-- {
		id := c.retryQueue[c.retryHead]
		c.retryHead++
		x := c.active[id]
		if x == nil {
			continue
		}
		c.retryQueue = append(c.retryQueue, id)
		// Non-voters retain/repair their input and query execution results;
		// voter recovery broadcasts each learned slot back to every participant.
		// They do not run a second global per-shard Fetch workload.
		if c.id >= c.n {
			continue
		}
		if id.Slot > c.executed(id.Key)+1 {
			continue
		}
		if now < x.nextRetry {
			continue
		}
		if c.retryBudget == 0 {
			c.stats.RetryDeferred++
			continue
		}
		c.retryBudget--
		c.deferRetry(x)
		if now-x.created >= c.failure && coord == c.id && (x.phase == 0 || x.promised > x.ballot) {
			c.startRecovery(id.Key, id.Slot)
			continue
		}
		if x.phase > 0 && x.ballot >= x.promised {
			c.stats.Retries++
			if x.phase == 1 {
				c.sendMissingPrepare(id.Key, id.Slot, x)
			} else {
				c.sendMissingAccept(id.Key, id.Slot, x)
			}
		} else {
			c.fetchPrefix(id.Key, id.Slot, x, coord, false)
		}
	}
	if c.retryHead > 4096 && c.retryHead*2 >= len(c.retryQueue) {
		copy(c.retryQueue, c.retryQueue[c.retryHead:])
		c.retryQueue = c.retryQueue[:len(c.retryQueue)-c.retryHead]
		c.retryHead = 0
	}

	readCount := len(c.readQueue) - c.readHead
	if readCount > 256 {
		readCount = 256
	}
	for ; readCount > 0; readCount-- {
		id := c.readQueue[c.readHead]
		c.readHead++
		q := c.reads[id]
		if q == nil {
			continue
		}
		c.readQueue = append(c.readQueue, id)
		c.stats.ReadTimerVisits++
		if !q.quorum && now-q.lastSend >= 4*c.retry && c.retryBudget > 0 {
			if c.sendMissingReadQueries(q) {
				c.retryBudget--
			}
			q.lastSend = now
		}
		c.finishRead(id, q)
		if c.reads[id] != nil && q.target > c.executed(q.record.Command.K) {
			c.repairPrefix(q.record.Command.K, q.target, true)
		}
	}
	if c.readHead > 4096 && c.readHead*2 >= len(c.readQueue) {
		copy(c.readQueue, c.readQueue[c.readHead:])
		c.readQueue = c.readQueue[:len(c.readQueue)-c.readHead]
		c.readHead = 0
	}
	// A decided suffix can arrive before its prefix even without a local read.
	if c.id < c.n {
		gapCount := len(c.gapQueue) - c.gapHead
		if gapCount > 256 {
			gapCount = 256
		}
		for ; gapCount > 0; gapCount-- {
			k := c.gapQueue[c.gapHead]
			c.gapHead++
			s := c.shards[k]
			if !c.gaps[k] {
				s.gapScheduled = false
				continue
			}
			c.gapQueue = append(c.gapQueue, k)
			c.stats.GapTimerVisits++
			if s.executed < s.high {
				c.repairPrefix(k, s.high, false)
			}
		}
		if c.gapHead > 4096 && c.gapHead*2 >= len(c.gapQueue) {
			copy(c.gapQueue, c.gapQueue[c.gapHead:])
			c.gapQueue = c.gapQueue[:len(c.gapQueue)-c.gapHead]
			c.gapHead = 0
		}
	}
}
