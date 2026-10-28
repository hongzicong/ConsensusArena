package kcensus

// Non-voting participants propose writes and receive voting-executor results.
import (
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/hongzicong/ConsensusArena/client"
	"github.com/hongzicong/ConsensusArena/config"
	"github.com/hongzicong/ConsensusArena/replica"
	"github.com/hongzicong/ConsensusArena/replica/defs"
	"github.com/hongzicong/ConsensusArena/state"
)

type clientRequest struct {
	record   Record
	delegate int
	uid      uint64
}

// The configured latency table is immutable. Availability is the only changing
// input to this client's delegate selection; retain just the most recent set.
type readDelegateCache struct {
	voters   uint64
	delegate int
	valid    bool // the empty voter set may legitimately cache delegate -1
}

type Client struct {
	client.StandardClient
	topology     processTopology
	proposer     *clientProposer
	inbox        chan clientInput
	proposals    chan Record
	peers        *replica.PeerStreams
	stop         chan struct{}
	closeOnce    sync.Once
	requests     map[defs.RequestID]*clientRequest
	readDelegate readDelegateCache
	readVoters   uint64 // last available voter set processed for GET failover
	readersOnce  sync.Once
	writeFailed  uint64
}

func NewClient(b *client.BufferClient, conf *config.Config, clone int) *Client {
	topology, err := configuredTopology(conf)
	if err != nil {
		panic(err)
	}
	pid := -1
	alias := fmt.Sprintf("%s#%d", conf.Alias, clone)
	for i, a := range topology.Aliases {
		if i >= topology.Plan.Voters && a == alias {
			pid = i
		}
	}
	if pid < 0 {
		panic("KCensus client alias/clone is not configured")
	}
	c := &Client{StandardClient: client.StandardClient{BufferClient: b}, topology: topology, proposer: newClientProposer(pid, topology.Plan, conf.KCensusFailure, conf.KCensusShards), inbox: make(chan clientInput, 65536), proposals: make(chan Record, 65536), stop: make(chan struct{}), requests: make(map[defs.RequestID]*clientRequest)}
	c.Printf("KCENSUS_SHARDS client=%d count=%d mapping=identity-if-zero-otherwise-modulo-v1", pid, conf.KCensusShards)
	b.ClientID = int32(pid)
	b.SkipPing = true
	b.ClosestID = topology.Plan.Leaders[pid]
	c.peers = replica.NewPeerStreams(c.proposer.n)
	b.SetProtocol(c)
	return c
}

var _ client.Adapter = (*Client)(nil)

func (*Client) RetryPolicy() string {
	return "PUT: synthesized voting delegate replies after ordered execution, original-ID result lookup at a survivor after stream failure; GET: voting-delegate quorum read with original-ID failover; in-memory executed-shard checkpoint catch-up; crash-stop lossless FIFO"
}

func (c *Client) Start() error {
	c.ClosestID = c.proposer.plan.Leaders[c.proposer.id]
	if c.PeerCount() != c.proposer.n {
		return fmt.Errorf("KCensus client membership differs from configured topology")
	}
	c.Printf("KCENSUS_CLIENT pid=%d alias=%s delegate=%d digest=%x voter_streams=%d", c.proposer.id, c.topology.Aliases[c.proposer.id], c.proposer.plan.Leaders[c.proposer.id], c.proposer.plan.Digest, c.proposer.n)
	go c.run()
	return nil
}

func (c *Client) WaitReplies(_ int) {
	c.readersOnce.Do(func() {
		select {
		case <-c.stop:
			return
		default:
		}
		for id := 0; id < c.proposer.n; id++ {
			if c.Connection(id) != nil {
				if c.bindStream(id, c.Connection(id), c.Writer(id)) == nil {
					go c.readStream(id, c.Connection(id), c.Reader(id))
				} else {
					c.MarkPeerFailed(id)
				}
			} else {
				c.MarkPeerFailed(id)
			}
		}
	})
}

func (c *Client) SendProposal(p defs.Propose) {
	if err := commandPolicy.Validate(p.Command); err != nil {
		panic(err)
	}
	select {
	case c.proposals <- Record{defs.RequestID{Client: int32(c.proposer.id), Sequence: p.CommandId}, p.Command}:
	case <-c.stop:
	}
}

func (c *Client) Close() {
	c.closeOnce.Do(func() {
		close(c.stop)
		c.peers.Close()
	})
}

func (c *Client) submitRequest(r Record) {
	if c.requests[r.ID] != nil || c.ReplyCompleted(r.ID) {
		return
	}
	q := &clientRequest{record: r, delegate: -1}
	c.requests[r.ID] = q
	if r.Command.Op == state.GET {
		c.sendDelegatedRead(q)
		return
	}
	q.delegate = c.proposer.plan.Leaders[c.proposer.id]
	q.uid = c.proposer.submit(r)
	// The proposer owns the retained Single/repair payload after submission.
	// The adapter needs only PUT identity, key, operation and delegate for replies.
	// Clear this slice header without mutating queued messages or payload bytes.
	q.record.Command.V = nil
	if c.proposer.peerFailed(q.delegate) {
		c.queryWriteResult(q)
	}
}

func (c *Client) availableVoters() uint64 {
	var voters uint64
	for id := 0; id < c.proposer.n; id++ {
		if !c.proposer.peerFailed(id) && (c.proposer.connected == nil || c.proposer.connected[id]) {
			voters |= uint64(1) << id
		}
	}
	return voters
}

func (c *Client) selectedReadDelegate() int {
	voters := c.availableVoters()
	// Normal GETs use the same synthesized voting delegate as PUTs. Retain the
	// existing alternative selection only when that delegate is unavailable.
	preferred := c.proposer.plan.Leaders[c.proposer.id]
	if preferred >= 0 && preferred < c.proposer.n && voters&(uint64(1)<<preferred) != 0 {
		return preferred
	}
	if c.readDelegate.valid && c.readDelegate.voters == voters {
		return c.readDelegate.delegate
	}
	// Membership fits the protocol's 63-process limit. Misses retain the exact
	// existing failover cost calculation and tie-breaking; hits avoid its allocations/sorts.
	var alive [63]bool
	for id := 0; id < c.proposer.n; id++ {
		alive[id] = voters&(uint64(1)<<id) != 0
	}
	delegate := survivingReadDelegate(c.topology.Latency, c.proposer.id, c.proposer.n, c.proposer.f, alive[:c.proposer.n])
	c.readDelegate = readDelegateCache{voters: voters, delegate: delegate, valid: true}
	return delegate
}

func (c *Client) sendDelegatedRead(q *clientRequest) {
	q.delegate = c.selectedReadDelegate()
	if q.delegate >= 0 {
		r := q.record
		c.proposer.send(q.delegate, message{Kind: delegateRead, Shard: c.proposer.shardFor(r.Command.K), ID: r.ID, Request: &r})
	}
}

func (c *Client) receive(i clientInput) {
	if i.kind != executedResult {
		c.proposer.receive(i)
		return
	}
	c.proposer.markConnected(i.from)
	q := c.requests[i.id]
	if q == nil || i.id.Client != int32(c.proposer.id) || i.from < 0 || i.from >= c.proposer.n ||
		i.from != q.delegate || c.proposer.peerFailed(i.from) || i.shard != c.proposer.shardFor(q.record.Command.K) {
		return
	}
	switch q.record.Command.Op {
	case state.GET:
		if i.slot != 0 {
			return
		}
	case state.PUT:
		if i.slot == 0 {
			return
		}
		// Execution evidence retires our payload, but does not invent a Commit UID
		// or advance the proposal cursor: the result's slot can be a later prefix.
		c.proposer.retire(i.shard, q.uid)
	default:
		return
	}
	value := append(state.Value(nil), i.result...)
	c.CompleteReply(i.id, value, func() {
		delete(c.requests, i.id)
	}, c.stop)
}

// A live FIFO retains its queued GET/result. Revisit pending GETs only when
// availability changes; an unrelated change never resends to a live delegate.
func (c *Client) recoverReadDelegates() {
	voters := c.availableVoters()
	if voters == c.readVoters {
		return
	}
	c.readVoters = voters
	for _, q := range c.requests {
		if q.record.Command.Op == state.GET && (q.delegate < 0 || voters&(uint64(1)<<q.delegate) == 0) {
			c.sendDelegatedRead(q)
		}
	}
}

// All pending PUTs share the same original executor for this client. Scan
// requests only when that executor (or a subsequent recovery executor) fails;
// a healthy run adds neither per-request timers nor result-query traffic.
func (c *Client) recoverWriteResults() {
	failed := c.proposer.failed
	if failed == c.writeFailed {
		return
	}
	c.writeFailed = failed
	for _, q := range c.requests {
		if q.record.Command.Op == state.PUT && (q.delegate < 0 || c.proposer.peerFailed(q.delegate)) {
			c.queryWriteResult(q)
		}
	}
}

func (c *Client) queryWriteResult(q *clientRequest) {
	q.delegate = c.selectedReadDelegate()
	if q.delegate < 0 {
		return
	}
	r := q.record
	c.proposer.send(q.delegate, message{Kind: resultQuery, Shard: c.proposer.shardFor(r.Command.K), ID: r.ID, Request: &r})
}

func (c *Client) run() {
	start := time.Now()
	c.proposer.connected = make([]bool, c.proposer.m)
	for id := 0; id < c.proposer.n; id++ {
		if c.Connection(id) != nil && !c.PeerFailed(id) {
			c.proposer.markConnected(id)
		}
	}
	c.readVoters = c.availableVoters()
	receive := c.receive
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-c.stop:
			return
		case r := <-c.proposals:
			c.submitRequest(r)
		case m := <-c.inbox:
			receive(m)
		case now := <-ticker.C:
			c.peers.Failed().Range(func(id int) bool { c.proposer.markFailed(id); return true })
			for id := 0; id < c.proposer.n; id++ {
				if c.PeerFailed(id) {
					c.proposer.markFailed(id)
				}
			}
			c.proposer.tick(now.Sub(start))
			c.recoverReadDelegates()
			c.recoverWriteResults()
		}
		// Drain a bounded group of already-arrived evidence before generating
		// another timer's repairs. All protocol transitions remain serialized.
		for count := 0; count < 64; count++ {
			select {
			case m := <-c.inbox:
				receive(m)
			default:
				count = 64
			}
		}
		c.drain()
	}
}

// clientInput is a projection made before inbox admission. It cannot retain a
// foreign Record, graph report, KV image, or snapshot. Results belong to this
// client; batch members contain only this client's UIDs.
type clientInput struct {
	kind              uint8
	from, proposer    int
	shard             state.Key
	slot, uid         uint64
	hasValue, hasLast bool
	last              uint64
	graphTime         int64
	id                defs.RequestID
	result            state.Value
	batches           []clientBatch
}

type clientBatch struct {
	uid, slot uint64
	members   []uint64
}

func projectClientMessage(m message, owner, processes int) clientInput {
	i := clientInput{kind: m.Kind, from: m.From, proposer: m.Proposer, shard: m.Shard,
		slot: m.Slot, graphTime: m.GraphTime}
	if m.LastV != nil {
		i.hasLast, i.last = true, *m.LastV
	}
	if m.Value != nil {
		i.hasValue, i.uid = true, m.Value.UID
	}
	owned := func(uid uint64) bool { return uid&1 == 0 && int((uid>>1)%uint64(processes)) == owner }
	if m.Kind == executedResult && m.ID.Client == int32(owner) {
		i.id, i.result = m.ID, m.Result
	}
	batch := func(v *Value) {
		if v == nil || v.Reference || v.Batch == nil {
			return
		}
		b := clientBatch{uid: v.UID, slot: v.Batch.Slot}
		for _, uid := range v.Batch.Members {
			if owned(uid) {
				b.members = append(b.members, uid)
			}
		}
		if len(b.members) > 0 {
			i.batches = append(i.batches, b)
		}
	}
	batch(m.Value)
	batch(m.Fast)
	for _, v := range m.Values {
		batch(v)
	}
	for _, r := range m.Reports {
		batch(r.Value)
	}
	return i
}

// This is an input-owner state machine, not an SMR learner. A voting Commit
// supplies the next proposal slot and predecessor UID, without materializing or
// executing that slot. Only executedResult from the selected voting delegate
// completes a request. No client votes, runs a census, or relays foreign input.
type clientProposer struct {
	id, n, m, f int
	plan        Plan
	priority    []int
	shardCount  state.Key
	shards      map[state.Key]*clientShard
	out         []envelope
	planSent    uint64 // fingerprint piggybacked once per voter stream
	now, retry  time.Duration
	connected   []bool
	failed      uint64
	graphFailed bool
	queue       []state.Key
	head        int
}

type clientShard struct {
	nextUID, slot, lastUID, busy uint64
	hasLast                      bool
	// Only this owner's unretired PUT bodies. Idle shards retain scalar cursors.
	inputs    map[uint64]*Value
	batches   map[uint64]clientBatch
	handoffs  map[uint64]uint64
	nextOffer time.Duration
	scheduled bool
}

func newClientProposer(id int, plan Plan, failure time.Duration, shards int) *clientProposer {
	if shards < 0 || id < plan.Voters || id >= len(plan.Requirements) {
		panic("invalid KCensus client configuration")
	}
	if err := validateRequirements(plan.Requirements, plan.Voters); err != nil {
		panic(err)
	}
	if len(plan.Graphs) != len(plan.Requirements) {
		panic("KCensus requires a synthesized propagation graph")
	}
	if failure <= 0 {
		failure = 1500 * time.Millisecond
	}
	return &clientProposer{id: id, n: plan.Voters, m: len(plan.Requirements), f: plan.Voters / 2,
		plan: plan, priority: plan.leaderPriority(), shardCount: state.Key(shards), shards: make(map[state.Key]*clientShard),
		retry: failure / 3}
}

func (p *clientProposer) shardFor(key state.Key) state.Key { return shardFor(key, p.shardCount) }

func (p *clientProposer) shard(k state.Key) *clientShard {
	s := p.shards[k]
	if s == nil {
		s = &clientShard{nextUID: uint64(2 * p.id)}
		p.shards[k] = s
	}
	return s
}

func (p *clientProposer) peerFailed(id int) bool {
	return id < 0 || id >= p.m || p.failed&(uint64(1)<<id) != 0
}
func (p *clientProposer) markConnected(id int) {
	if p.connected != nil && id >= 0 && id < p.n && !p.peerFailed(id) {
		p.connected[id] = true
	}
}
func (p *clientProposer) markFailed(id int) {
	if id < 0 || id >= p.n || p.peerFailed(id) {
		return
	}
	p.failed |= uint64(1) << id
	p.graphFailed = p.graphFailed || graphNeedsPeer(p.plan, p.id, id)
}
func (p *clientProposer) coordinator() int {
	for _, id := range p.priority {
		// Established FIFO streams remain eligible until a definitive failure.
		// A transport-free driver supplies failures explicitly through markFailed.
		if id < p.n && !p.peerFailed(id) && (p.connected == nil || p.connected[id]) {
			return id
		}
	}
	return -1
}

func (p *clientProposer) send(to int, m message) {
	if to < 0 || to >= p.n || p.peerFailed(to) {
		return
	}
	m.From = p.id
	if p.planSent&(uint64(1)<<to) == 0 {
		m.Digest = p.plan.Digest
		p.planSent |= uint64(1) << to
	}
	switch m.Kind {
	case spread, offer:
		if s := p.shards[m.Shard]; s != nil && s.hasLast {
			uid := s.lastUID
			m.LastV = &uid
		}
	}
	p.out = append(p.out, envelope{To: to, Message: m})
}

func (p *clientProposer) submit(r Record) uint64 {
	k := p.shardFor(r.Command.K)
	s := p.shard(k)
	uid := s.nextUID
	if uid > math.MaxUint64-uint64(2*p.m)-1 {
		panic("KCensus UID space exhausted")
	}
	s.nextUID += uint64(2 * p.m)
	r.Command.V = append(state.Value(nil), r.Command.V...)
	v := &Value{UID: uid, Proposer: p.id, Records: []Record{r}}
	if s.inputs == nil {
		s.inputs = make(map[uint64]*Value)
	}
	// Previously offered own input must be reproposed by the voting reproposer.
	direct := len(s.inputs) == 0 && s.busy <= s.slot && !p.graphFailed
	s.inputs[uid] = v
	if !s.scheduled {
		s.scheduled = true
		s.nextOffer = p.now + 4*p.retry
		p.queue = append(p.queue, k)
	}
	if direct {
		s.busy = s.slot + 1
		for _, edge := range p.plan.Graphs[p.id].States[p.id][0].Outgoing {
			payload := p.plan.Graphs[p.id].Payload[edge]
			p.send(edge.To, message{Kind: spread, Shard: k, Slot: s.slot + 1, Proposer: p.id, GraphTime: int64(edge.Time),
				Mask: uint64(1) << p.id, Value: v, NewValue: true, References: !payload})
		}
	} else {
		p.offer(k, v, p.graphFailed)
	}
	return uid
}

func (p *clientProposer) offer(k state.Key, v *Value, repair bool) {
	m := message{Kind: offer, Shard: k, ID: v.Records[0].ID, Proposer: p.id, Value: v}
	if repair {
		to := p.coordinator()
		if to < 0 {
			return
		}
		s := p.shards[k]
		reliable := p.connected != nil && p.connected[to] && !p.peerFailed(to)
		if reliable {
			if s.handoffs == nil {
				s.handoffs = make(map[uint64]uint64)
			}
			bit := uint64(1) << to
			if s.handoffs[v.UID]&bit != 0 {
				return
			}
			s.handoffs[v.UID] |= bit
		}
		m.GraphTime = -1
		p.send(to, m)
		return
	}
	for _, edge := range p.plan.Graphs[p.id].States[p.id][0].Outgoing {
		if p.plan.Graphs[p.id].Payload[edge] {
			m.GraphTime = int64(edge.Time)
			p.send(edge.To, m)
		}
	}
}

func (p *clientProposer) retire(k state.Key, uid uint64) {
	s := p.shards[k]
	if s == nil {
		return
	}
	delete(s.inputs, uid)
	delete(s.handoffs, uid)
	if len(s.inputs) == 0 {
		s.inputs = nil
		s.handoffs = nil
		s.batches = nil
	}
}

func (p *clientProposer) receive(i clientInput) {
	if i.from < 0 || i.from >= p.n {
		return
	}
	p.markConnected(i.from)
	if p.shardCount != 0 && (i.shard < 0 || i.shard >= p.shardCount) {
		return
	}
	switch i.kind {
	case spread, accept, commit, offer:
	default:
		return
	}
	if i.kind == spread && (i.proposer < 0 || i.proposer >= p.m || !p.plan.Graphs[i.proposer].Edges[graphEdge{i.from, p.id, time.Duration(i.graphTime)}]) {
		return
	}
	s := p.shards[i.shard]
	if s == nil {
		switch i.kind {
		case offer:
			return
		}
		s = p.shard(i.shard)
	}
	if i.slot == s.slot+1 && i.hasLast && (!s.hasLast || s.lastUID != i.last) {
		panic(fmt.Sprintf("KCensus last_v mismatch shard=%d slot=%d from=%d", i.shard, i.slot, i.from))
	}
	for _, b := range i.batches {
		var members []uint64
		for _, uid := range b.members {
			if s.inputs[uid] != nil {
				members = append(members, uid)
			}
		}
		if len(members) > 0 && b.slot > s.slot {
			if s.batches == nil {
				s.batches = make(map[uint64]clientBatch)
			}
			b.members = members
			s.batches[b.uid] = b
		}
	}
	switch i.kind {
	case spread, accept:
		if i.slot > s.busy {
			s.busy = i.slot
		}
	case commit:
		if !i.hasValue || i.slot == 0 || i.slot <= s.slot {
			return
		}
		p.retire(i.shard, i.uid)
		if b, ok := s.batches[i.uid]; ok {
			for _, uid := range b.members {
				p.retire(i.shard, uid)
			}
		}
		s.slot, s.lastUID, s.hasLast = i.slot, i.uid, true
		for uid, b := range s.batches {
			if b.slot <= s.slot {
				delete(s.batches, uid)
			}
		}
	}
}

func (p *clientProposer) tick(now time.Duration) {
	p.now = now
	budget := 64
	for count := min(256, len(p.queue)-p.head); count > 0; count-- {
		k := p.queue[p.head]
		p.head++
		s := p.shards[k]
		if len(s.inputs) == 0 {
			s.scheduled = false
			continue
		}
		p.queue = append(p.queue, k)
		if now < s.nextOffer || budget == 0 {
			continue
		}
		s.nextOffer = now + 4*p.retry
		var lowest *Value
		for _, v := range s.inputs {
			if lowest == nil || v.UID < lowest.UID {
				lowest = v
			}
		}
		p.offer(k, lowest, true)
		budget--
	}
	if p.head > 4096 && p.head*2 >= len(p.queue) {
		copy(p.queue, p.queue[p.head:])
		p.queue = p.queue[:len(p.queue)-p.head]
		p.head = 0
	}
}
