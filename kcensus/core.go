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
	shard state.Key
	slot  uint64
}

// Retry entries are validated against the active index and shard generation.
// Physical shard objects can be reused after the enclosing transition returns.
type retryItem struct {
	id         slotID
	instance   *slotState
	owner      *shard
	generation uint64
}

type slotState struct {
	fast                     *Value
	values                   map[int]*Value
	graphs                   map[int]*graphRun
	nodeReports              map[int]nodeReport
	classicValues            map[uint64]*Value
	conflictAnnounced        bool
	fetchSent                bool
	nextRetry, retryDelay    time.Duration
	knowledge                []replicaset.Set
	knowledgeTime            time.Duration
	frozen                   bool
	participants             uint64
	promised, acceptedBallot uint64
	classic, decided         *Value
	local                    *Value
	ballot                   uint64
	phase                    uint8 // 0: evidence; 1: census/prepare; 2: accept
	reports                  map[int]message
	selected                 *Value
	acceptNewValue           bool
	recoveryPayload          bool // abnormal census must repair interrupted payload trees
	acks                     uint64
	prepareSent, acceptSent  replicaset.Set
	created                  time.Duration
}

type shard struct {
	physicalIndex          int
	inUse                  bool
	generation             uint64
	lastUID                uint64
	hasLast                bool
	peakObjects, peakSlots int
	high, executed         uint64
	slots                  map[uint64]*slotState
	pending                []defs.RequestID
	queued                 map[defs.RequestID]bool
	shared                 map[defs.RequestID]bool
	objects                map[uint64]*Value
	nextUID                uint64
	pendingValue           *Value
	scheduled              bool
	gapScheduled           bool
	nextOffer              time.Duration
}

type Stats struct {
	encodingStats
	ShardsAllocated, ShardsReused, ShardsSlept                            uint64
	FastCommits, FallbackCommits, Conflicts, Prepares, Retries, Fetches   uint64
	SpreadMessages, KnowledgeUpdates, Executed, Deduplicated              uint64
	Reads, ReadWaits, ReadWaitNanos, Decisions                            uint64
	RetryDeferred                                                         uint64
	OfferedCommands, BatchCommands, MaxBatchCommands                      uint64
	LocalWrites, LocalWriteNanos, LocalWriteMaxNanos, LocalSlotWaits      uint64
	LocalWriteLatencyBuckets                                              [9]uint64
	Reproposals, InitialCensuses, TimeoutCensuses                         uint64
	OriginalProposals, BusyProposalSlots, SharedProposalDeferrals         uint64
	GraphMessages, GraphPayloads, GraphStates, FrozenRelays               uint64
	ReadyReadReplies, BufferedReadReplies                                 uint64
	ReferenceMessages, PayloadWaits                                       uint64
	NodeStateRelays                                                       uint64
	HeartbeatReplies, StaleHeartbeatReplies, FailedPeers, WrongBatchSlots uint64
	ReadTimerVisits, GapTimerVisits, TickNanos, TickMaxNanos              uint64
	FailedGraphBypasses, RecoveryResultReplies                            uint64
	SuppressedRecoveryRequests, RetiredRecoveryReplies                    uint64
	SuppressedReadRequests, DuplicateReadReplies                          uint64
	SuppressedRepairOffers                                                uint64
	ImmediateFetches, DeadlineFetches, DeferredInFlightFetches            uint64
	RecoveryAdmissionWaits                                                uint64
	CheckpointTransfers, CheckpointInstalls, CheckpointChunks             uint64
	CheckpointRequests, CheckpointMerged, CheckpointSwitches              uint64
	CheckpointHints, CheckpointSuppressed                                 uint64
	ResultQueries                                                         uint64
	InputMessages, InputSamples, InputSampleNanos                         [checkpointAck + 1]uint64
}

// core owns protocol state, ordering, recovery decisions and completion semantics.
type core struct {
	id, n, m, f     int
	plan            Plan
	priority        []int
	shardCount      state.Key
	shards          map[state.Key]*shard
	pool            *shardPool
	shardGeneration uint64
	turnDepth       int
	touched         map[state.Key]bool
	retiredSlots    []*slotState
	completed       map[int32]*completedSequences

	active              map[slotID]*slotState
	retryQueue          []retryItem
	retryHead           int
	retryBudget         int
	activeCensuses      int
	known               map[defs.RequestID]Record
	commandUIDs         map[defs.RequestID]uint64
	reads               map[defs.RequestID]*pendingRead
	readQueue           []defs.RequestID
	readHead            int
	readsByShard        map[state.Key]map[defs.RequestID]*pendingRead
	gaps                map[state.Key]bool
	gapQueue            []shardTask
	gapHead             int
	out                 []envelope
	clientPlanSent      uint64 // fingerprint piggybacked once per FIFO client stream
	now, lastBeat       time.Duration
	heard               []time.Duration
	failed              []bool
	failedGraphs        []bool
	connected           []bool
	probeSequence       uint64
	probes              [8]heartbeatProbe
	failure, retry      time.Duration
	execute             func(state.Command) state.Value
	reply               func(defs.RequestID, state.Value) bool
	stats               Stats
	localWrites         map[defs.RequestID]writeArrival
	pendingShards       []shardTask
	pendingHead         int
	future              map[state.Key][]message
	payloadWaiting      map[payloadWaitKey]*payloadWait
	payloadDependencies map[payloadObject]map[payloadWaitKey]bool
	payloadReady        []payloadObject
	payloadDraining     bool
	payloadSeen         map[payloadObject]map[payloadEdgeID]bool
	payloadByShard      map[state.Key]map[payloadWaitKey]*payloadWait
	repairHandoffs      map[payloadObject]uint64 // key/Single UID -> reliable-stream destinations
	checkpoints         map[state.Key]*shardCheckpoint
	snapshotInputs      map[state.Key]*snapshotInput
	snapshotSequence    uint64
	checkpointRequests  map[state.Key]*checkpointRequest
	checkpointQueue     []*checkpointRequest
	checkpointHead      int
	checkpointActive    int
	checkpointDonors    [64]checkpointDonor
	snapshotOutputs     map[snapshotTarget]*snapshotOutput
	snapshotQueue       []*snapshotOutput
	snapshotHead        int
	snapshotActive      int
	snapshotBytes       int
	resultWaiters       map[defs.RequestID]state.Key
	resultByShard       map[state.Key]map[defs.RequestID]bool
	resultQueue         []defs.RequestID
	resultHead          int
	checkpointProbes    map[state.Key]time.Duration
}

func newCore(id int, plan Plan, failure time.Duration, shardCount, keyCount int) *core {
	if shardCount < 0 {
		panic("KCensus shard count must not be negative")
	}
	if err := validateRequirements(plan.Requirements, plan.Voters); err != nil {
		panic(err)
	}
	m := len(plan.Requirements)
	n := plan.Voters
	if len(plan.Graphs) != m {
		panic("KCensus requires a synthesized propagation graph")
	}
	if id < 0 || id >= n {
		panic("invalid replica ID")
	}
	if failure <= 0 {
		failure = 1500 * time.Millisecond
	}
	logicalShards := keyCount
	if shardCount > 0 {
		logicalShards = shardCount
	}
	pool := newShardPool(logicalShards, id)
	c := &core{shardCount: state.Key(shardCount), id: id, n: n, m: m, f: n / 2, plan: plan, priority: plan.leaderPriority(), shards: make(map[state.Key]*shard, pool.initialSize), pool: pool, active: make(map[slotID]*slotState), known: make(map[defs.RequestID]Record), commandUIDs: make(map[defs.RequestID]uint64), touched: make(map[state.Key]bool), completed: make(map[int32]*completedSequences), reads: make(map[defs.RequestID]*pendingRead), readsByShard: make(map[state.Key]map[defs.RequestID]*pendingRead), gaps: make(map[state.Key]bool), heard: make([]time.Duration, n), failure: failure, retry: failure / 3, localWrites: make(map[defs.RequestID]writeArrival), future: make(map[state.Key][]message)}
	c.stats.ShardsAllocated = uint64(pool.initialSize)
	return c
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
		x = &slotState{created: c.now, nextRetry: c.now + c.retry, retryDelay: c.retry}
		x.knowledge = make([]replicaset.Set, c.m)
		s.slots[i] = x
		if len(s.slots) > s.peakSlots {
			s.peakSlots = len(s.slots)
		}
		c.active[slotID{k, i}] = x
		c.retryQueue = append(c.retryQueue, retryItem{slotID{k, i}, x, s, s.generation})
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
	c.attachLastValue(&e.Message)
	if e.Count == 0 && e.To >= c.n && e.To < c.m && c.clientPlanSent&(uint64(1)<<e.To) == 0 {
		e.Message.Digest = c.plan.Digest
		// A broadcast's other recipients may reuse the ordinary wire image.
		e.Encoded = nil
		c.clientPlanSent |= uint64(1) << e.To
	}
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

// Only the deciding process emits Commit. Like upstream Commit {slot, v},
// it carries the chosen UID, never a second copy of the command payload.
func (c *core) broadcastCommit(m message) {
	if m.Value == nil {
		return
	}
	uid, proposer := m.Value.UID, m.Value.Proposer
	m = message{Kind: commit, Shard: m.Shard, Slot: m.Slot, References: true,
		Value: &Value{UID: uid, Proposer: proposer, Reference: true}}
	var encoded []byte
	send := func(to int) {
		if to != c.id {
			c.sendShared(to, m, &encoded)
		}
	}
	first := -1
	if uid&1 == 0 {
		first = int((uid >> 1) % uint64(c.m))
		send(first)
	}
	for id := 0; id < c.m; id++ {
		if id != first {
			send(id)
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
	c.beginTurn()
	defer c.endTurn()
	if err := commandPolicy.Validate(r.Command); err != nil {
		return err
	}
	if c.requestCompleted(r.ID) {
		return nil
	}
	c.remember(r)
	k := c.shardFor(r.Command.K)
	if r.Command.Op == state.GET {
		if c.reads[r.ID] == nil {
			c.shard(k)
			q := &pendingRead{record: r, started: c.now, lastSend: c.now, fences: make(map[int]uint64)}
			c.reads[r.ID] = q
			c.readQueue = append(c.readQueue, r.ID)
			if c.readsByShard[k] == nil {
				c.readsByShard[k] = make(map[defs.RequestID]*pendingRead)
			}
			c.readsByShard[k][r.ID] = q
			c.stats.Reads++
			c.sendMissingReadQueries(q)
		}
		return nil
	}
	single := c.singleValue(r)
	s := c.shard(k)
	if _, exists := c.localWrites[r.ID]; !exists {
		c.localWrites[r.ID] = writeArrival{c.now, s.executed}
	}
	c.enqueue(r, false)
	c.propose(k)
	if !s.shared[r.ID] {
		s.shared[r.ID] = true
		c.offerValue(k, single, !c.graphUsable(c.id))
	}
	return nil
}

func (c *core) propose(k state.Key) {
	s := c.shard(k)
	for len(s.pending) > 0 {
		if s.queued[s.pending[0]] {
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
	if len(s.shared) > 0 && c.coordinator() != c.id {
		c.stats.SharedProposalDeferrals++
		return
	}
	shared := c.pendingRecords(k, &isShared)
	var v *Value
	if len(shared) > 0 {
		if c.coordinator() != c.id {
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
		if c.shardFor(r.Command.K) != k || r.Command.Op != state.PUT || len(r.Command.V) > state.MaxValueBytes || seen[r.ID] {
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
	c.beginTurn()
	defer c.endTurn()
	if i <= c.executed(k) {
		return
	}
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
				c.relayPayload(message{From: edge.From, Proposer: p, Shard: k, Slot: i, GraphTime: int64(edge.Time), Value: x.values[p]})
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

func (c *core) receive(m message) {
	c.receiveMessage(m, false)
}

func (c *core) receiveMessage(m message, replayed bool) {
	c.beginTurn()
	defer c.endTurn()
	if m.From < 0 || m.From >= c.m {
		return
	}
	if m.Kind == payloadQuery || m.Kind == payloadReply {
		return // Reserved legacy RPCs; all prefix repair uses Fetch/checkpoints.
	}
	if m.Kind == heartbeat {
		c.receiveHeartbeat(m)
		return
	}
	if !c.validShard(m.Shard) {
		return
	}
	if m.Kind == delegateRead {
		c.receiveDelegatedRead(m)
		return
	}
	if m.Kind == resultQuery {
		c.receiveResultQuery(m)
		return
	}
	if m.Kind == snapshotPart {
		c.receiveSnapshot(m)
		return
	}
	if m.Kind == checkpointStatus {
		c.receiveCheckpointStatus(m)
		return
	}
	if m.Kind == checkpointAck {
		c.receiveCheckpointAck(m)
		return
	}
	// An executed result alone is never a vote or protocol-prefix evidence.
	if m.Kind == executedResult {
		return
	}
	// These requests must reach the checkpoint before retired-slot filtering.
	if m.Kind == fetch && m.From < c.n {
		if m.Ballot == 0 || m.From == c.id {
			return
		}
		if m.Slot > 0 && m.Slot <= c.executed(m.Shard) {
			c.queueSnapshot(m)
			return
		}
		c.send(m.From, message{Kind: checkpointStatus, Shard: m.Shard, Slot: c.executed(m.Shard), Ballot: m.Ballot})
		if m.Explicit { // Result recovery probes do not invent empty instances.
			return
		}
	}

	if m.Kind == readReply && m.Value == nil && m.Fast == nil && len(m.Values) == 0 && len(m.Reports) == 0 && len(m.Knowledge) == 0 && len(m.UIDs) == 0 {
		q := c.reads[m.ID]
		if q == nil || c.shardFor(q.record.Command.K) != m.Shard {
			return
		}
		if _, seen := q.fences[m.From]; seen {
			c.stats.DuplicateReadReplies++
			return
		}
	}
	if m.From < c.n && m.Slot > 0 && m.Slot <= c.executed(m.Shard) &&
		(m.Kind == prepare || m.Kind == accept) {
		// A lagging coordinator may be waiting for promises, not issuing Fetch.
		c.hintCheckpoint(m.From, m.Shard)
		return
	}
	// Retired evidence is filtered before reference resolution or slot allocation.
	if m.Slot > 0 && m.Slot <= c.executed(m.Shard) {
		if (m.Kind == spread || m.Kind == offer) && m.Value != nil && !m.Value.Reference {
			c.receiveLatePayload(m)
		}
		c.stats.RetiredRecoveryReplies++
		return
	}
	c.checkLastValue(m)
	if m.Kind == offer && c.valueCompleted(m.Value) {
		return
	}
	if m.Value != nil && m.Value.Batch != nil && m.Value.Batch.Slot <= c.executed(m.Shard) {
		return
	}

	if m.Kind == prepare || m.Kind == promise || m.Kind == accept || m.Kind == commit {
		for _, v := range m.Values {
			if v == nil || v.Batch != nil || v.Reference {
				return
			}
			if err := c.storeValue(m.Shard, v); err != nil {
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
		if r.Value != nil && !r.Value.Reference && !c.validValue(m.Shard, r.Value, false) {
			return
		}
	}
	if m.Kind == readQuery {
		reply := message{Kind: readReply, Shard: m.Shard, ID: m.ID}
		reply.High = c.readFence(m.Shard)
		c.send(m.From, reply)
		return
	}
	if m.Kind == readReply {
		if m.From >= c.n {
			return
		}
		q := c.reads[m.ID]
		if q == nil || c.shardFor(q.record.Command.K) != m.Shard {
			return
		}
		// The first response from a voter describes that read's observation.
		// Retransmissions cannot raise its fence or count it a second time.
		if _, seen := q.fences[m.From]; !seen {
			q.fences[m.From] = m.High
		}
		c.finishRead(m.ID, q)
		if c.reads[m.ID] != nil && q.target > c.executed(m.Shard) {
			c.repairPrefix(m.Shard, q.target, true)
		}
		return
	}
	if m.Kind == offer {
		if !c.validValue(m.Shard, m.Value, true) {
			return
		}
		c.enqueueValue(m.Value)
		c.relayPayload(m)
		c.propose(m.Shard)
		return
	}
	if m.Value != nil && m.Value.Batch != nil && m.Value.Batch.Slot != m.Slot {
		c.stats.WrongBatchSlots++
		return
	}
	if m.Slot == 0 {
		return
	}
	if m.Value != nil && !c.validValue(m.Shard, m.Value, m.Kind == spread) {
		return
	}
	if m.Fast != nil && !c.validValue(m.Shard, m.Fast, true) {
		return
	}
	// As in upstream's slot layer, future-slot messages cannot produce an
	// acceptance before the preceding prefix executes. GET fences can then
	// refer to the current acceptance rather than metadata's highest slot.
	if m.Slot > c.executed(m.Shard)+1 {
		c.slot(m.Shard, m.Slot)
		c.future[m.Shard] = append(c.future[m.Shard], m)
		return
	}
	if m.Kind == spread || m.Kind == promise || m.Kind == accept {
		c.enqueueValue(m.Value)
		c.enqueueValue(m.Fast)
	}
	x := c.slot(m.Shard, m.Slot)
	c.cacheClassic(x, m)
	if m.Kind == commit {
		if m.Value != nil {
			c.learn(m.Shard, m.Slot, m.Value)
		}
		return
	}
	if x.decided != nil {
		return
	}

	switch m.Kind {
	case resumeCensus:
		c.completeCensus(m.Shard, m.Slot, x)
	case spread:
		c.receiveGraph(m, x)
	case abandon:
		x.participants |= m.Mask & ((uint64(1) << c.m) - 1)
		c.freezeConflict(m.Shard, m.Slot, x)
	case prepare:
		if m.From >= c.n {
			return
		}
		if m.Ballot == 0 || int(m.Ballot%uint64(c.n)) != m.From {
			return
		}
		if m.Ballot < x.promised {
			c.send(m.From, message{Kind: nack, Shard: m.Shard, Slot: m.Slot, Ballot: x.promised})
			return
		}
		x.promised = m.Ballot
		c.freezeFor(x, m.Ballot)
		if !x.conflictAnnounced {
			x.conflictAnnounced = true
			c.broadcast(message{Kind: abandon, Shard: m.Shard, Slot: m.Slot, Mask: x.participants})
		}
		r := x.nodeReports[c.id]
		fast := x.fast
		if x.acceptedBallot > 0 {
			fast = nil
		}
		c.send(m.From, c.valuePayload(message{Kind: promise, Shard: m.Shard, Slot: m.Slot, Ballot: m.Ballot, AcceptedBallot: x.acceptedBallot, Value: x.classic, Fast: fast, Mask: r.Mask, StateTime: r.Time}))
	case promise:
		if m.From >= c.n {
			return
		}
		c.cachePromise(x, m)
		if m.Ballot == uint64(c.n+c.id) {
			c.initialCensus(m.Shard, m.Slot, x)
		}
		if x.phase != 1 || m.Ballot != x.ballot || x.promised > x.ballot {
			return
		}
		if old, ok := x.reports[m.From]; ok && old.AcceptedBallot >= m.AcceptedBallot {
			return
		}
		x.reports[m.From] = m
		c.completeCensus(m.Shard, m.Slot, x)
	case accept:
		if m.From >= c.n {
			return
		}
		if m.Value == nil || m.Ballot == 0 || int(m.Ballot%uint64(c.n)) != m.From {
			return
		}
		if m.Ballot < x.promised {
			c.send(m.From, message{Kind: nack, Shard: m.Shard, Slot: m.Slot, Ballot: x.promised})
			return
		}
		if x.acceptedBallot == m.Ballot && !sameValue(x.classic, m.Value) {
			panic("coordinator changed value within ballot")
		}
		x.frozen = true
		x.promised = m.Ballot
		x.acceptedBallot = m.Ballot
		x.classic = m.Value
		c.freezeFor(x, m.Ballot)
		for _, r := range m.Value.Records {
			c.remember(r)
		}
		c.send(m.From, message{Kind: accepted, Shard: m.Shard, Slot: m.Slot, Ballot: m.Ballot})
	case accepted:
		if m.From >= c.n {
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
			c.broadcastCommit(message{Kind: commit, Shard: m.Shard, Slot: m.Slot, Value: v})
			c.learn(m.Shard, m.Slot, v)
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
	c.tryGraphCensus(m.Shard, m.Slot, x)
}

func (c *core) tick(now time.Duration) {
	c.beginTurn()
	defer c.endTurn()
	c.now = now
	c.heard[c.id] = now
	if now-c.lastBeat >= c.retry {
		c.probeSequence++
		c.probes[c.probeSequence%uint64(len(c.probes))] = heartbeatProbe{c.probeSequence, now}
		probe := message{Kind: heartbeat, Digest: c.plan.Digest, High: c.probeSequence}
		c.broadcast(probe)
		c.lastBeat = now
	}
	c.retryBudget = 256
	c.retryPending()
	coord := c.coordinator()
	c.retryResultWaiters()
	// Round-robin bounded traversal: large backlogs cannot monopolize the loop
	// or starve slots behind the budget. Retired slots leave the rotation.
	remaining := len(c.retryQueue) - c.retryHead
	if remaining > 4096 {
		remaining = 4096
	}
	for ; remaining > 0; remaining-- {
		item := c.retryQueue[c.retryHead]
		c.retryQueue[c.retryHead] = retryItem{}
		c.retryHead++
		id, x := item.id, item.instance
		if c.active[id] != x || c.shards[id.shard] != item.owner || item.owner.generation != item.generation {
			continue
		}
		c.retryQueue = append(c.retryQueue, item)
		if now < x.nextRetry {
			continue
		}
		if id.slot > item.owner.executed+1 {
			continue
		}
		if c.retryBudget == 0 {
			c.stats.RetryDeferred++
			continue
		}
		c.retryBudget--
		c.deferRetry(x)
		if now-x.created >= c.failure && coord == c.id && (x.phase == 0 || x.promised > x.ballot) {
			c.startRecovery(id.shard, id.slot)
			continue
		}
		if x.phase > 0 && x.ballot >= x.promised {
			c.stats.Retries++
			if x.phase == 1 {
				c.sendMissingPrepare(id.shard, id.slot, x)
			} else {
				c.sendMissingAccept(id.shard, id.slot, x)
			}
		} else {
			c.fetchPrefix(id.shard, id.slot, x, false)
		}
	}
	if c.retryHead > 4096 && c.retryHead*2 >= len(c.retryQueue) {
		retained := copy(c.retryQueue, c.retryQueue[c.retryHead:])
		clear(c.retryQueue[retained:])
		c.retryQueue = c.retryQueue[:retained]
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
		if c.reads[id] != nil && q.target > c.executed(c.shardFor(q.record.Command.K)) {
			c.repairPrefix(c.shardFor(q.record.Command.K), q.target, true)
		}
	}
	if c.readHead > 4096 && c.readHead*2 >= len(c.readQueue) {
		copy(c.readQueue, c.readQueue[c.readHead:])
		c.readQueue = c.readQueue[:len(c.readQueue)-c.readHead]
		c.readHead = 0
	}
	// A decided suffix can arrive before its prefix even without a local read.
	gapCount := len(c.gapQueue) - c.gapHead
	if gapCount > 256 {
		gapCount = 256
	}
	for ; gapCount > 0; gapCount-- {
		task := c.gapQueue[c.gapHead]
		k := task.key
		c.gapHead++
		s := c.shards[k]
		if s == nil || s.generation != task.generation {
			continue
		}
		if !c.gaps[k] {
			s.gapScheduled = false
			continue
		}
		c.gapQueue = append(c.gapQueue, task)
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
	c.tickCheckpoints()
}
