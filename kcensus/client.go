package kcensus

// Non-voting client construction, routing, and executed-result completion.
import (
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/hongzicong/ConsensusArena/client"
	"github.com/hongzicong/ConsensusArena/config"
	"github.com/hongzicong/ConsensusArena/replica"
	"github.com/hongzicong/ConsensusArena/replica/defs"
	"github.com/hongzicong/ConsensusArena/state"
)

type clientRequest struct {
	record     Record
	delegate   int
	nextQuery  time.Duration
	queryDelay time.Duration
	queryPeer  int
}

type Client struct {
	client.StandardClient
	topology    processTopology
	engine      *core
	inbox       chan message
	control     chan message
	resultInbox chan message
	proposals   chan Record
	peers       []replica.PendingSender
	stop        chan struct{}
	closeOnce   sync.Once
	listener    net.Listener
	sendMu      sync.Mutex
	requests    map[CommandID]*clientRequest
	done        map[CommandID]bool
	readersOnce sync.Once
	queryQueue  []CommandID
	queryHead   int
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
	c := &Client{StandardClient: client.StandardClient{BufferClient: b}, topology: topology, engine: newCore(pid, topology.Plan, conf.KCensusFailure), inbox: make(chan message, 65536), resultInbox: make(chan message, 4096), proposals: make(chan Record, 65536), stop: make(chan struct{}), requests: make(map[CommandID]*clientRequest), done: make(map[CommandID]bool)}
	b.ClientId = int32(pid)
	c.control = make(chan message, 4*c.engine.m)
	b.SkipPing = true
	b.ClosestId = topology.Plan.Leaders[pid]
	c.peers = make([]replica.PendingSender, len(topology.Aliases))
	b.SetProtocol(c)
	return c
}

var _ client.Adapter = (*Client)(nil)

func (*Client) RetryPolicy() string {
	return "GET: original-ID delegate failover; PUT: executed-result cache queries; protocol evidence/payload repair"
}

func (c *Client) Start() error {
	c.ClosestId = c.engine.plan.Leaders[c.engine.id]
	if c.PeerCount() != c.engine.n {
		return fmt.Errorf("KCensus client membership differs from configured topology")
	}
	listener, err := net.Listen("tcp", c.topology.Listeners[c.engine.id])
	if err != nil {
		return fmt.Errorf("KCensus client listener: %w", err)
	}
	c.listener = listener
	c.Printf("KCENSUS_CLIENT pid=%d alias=%s delegate=%d digest=%x listener=%s", c.engine.id, c.topology.Aliases[c.engine.id], c.engine.plan.Leaders[c.engine.id], c.engine.plan.Digest, listener.Addr())
	go c.run()
	go c.acceptMesh()
	for id := c.engine.id + 1; id < c.engine.m; id++ {
		go c.dialMesh(id)
	}
	return nil
}

func (c *Client) WaitReplies(_ int) {
	c.readersOnce.Do(func() {
		c.sendMu.Lock()
		defer c.sendMu.Unlock()
		select {
		case <-c.stop:
			return
		default:
		}
		for id := 0; id < c.engine.n; id++ {
			if c.Connection(id) != nil {
				c.startStreamSender(id, c.Connection(id), c.Writer(id))
				go c.readStream(id, c.Connection(id), c.Reader(id))
			} else {
				c.MarkPeerFailed(id)
			}
		}
	})
}

func (c *Client) SendProposal(p defs.Propose) {
	if p.Command.Op != state.PUT && p.Command.Op != state.GET {
		panic(fmt.Sprintf("KCensus supports PUT/GET, not operation %d", p.Command.Op))
	}
	select {
	case c.proposals <- Record{CommandID{int32(c.engine.id), p.CommandId}, p.Command}:
	case <-c.stop:
	}
}

func (c *Client) Close() {
	c.closeOnce.Do(func() {
		close(c.stop)
		if c.listener != nil {
			c.listener.Close()
		}
		c.sendMu.Lock()
		defer c.sendMu.Unlock()
		for i := range c.peers {
			p := &c.peers[i]
			p.Close()
		}
	})
}

func (c *Client) selectedDelegate() int {
	leader := c.engine.plan.Leaders[c.engine.id]
	if c.Connection(leader) != nil && !c.PeerFailed(leader) {
		return leader
	}
	alive := make([]bool, c.engine.n)
	for id := range alive {
		alive[id] = c.Connection(id) != nil && !c.PeerFailed(id)
	}
	if delegate := survivingReadDelegate(c.topology.Latency, c.engine.id, c.engine.n, c.engine.f, alive); delegate >= 0 {
		return delegate
	}
	for _, id := range c.engine.priority {
		if id < c.engine.n && c.Connection(id) != nil && !c.PeerFailed(id) {
			return id
		}
	}
	return -1
}

func (c *Client) receiveResult(m message) {
	q := c.requests[m.ID]
	if m.From < 0 || m.From >= c.engine.n || q == nil || q.record.Command.K != m.Key || c.done[m.ID] {
		return
	}
	c.done[m.ID] = true
	delete(c.requests, m.ID)
	if q.record.Command.Op == state.PUT {
		c.engine.recordWriteCompletion(m.ID, c.engine.executed(m.Key))
		// A voter has executed this exact input. Stop offering it while local
		// commit/payload catch-up continues; this does not advance a slot prefix.
		c.engine.ordered[m.ID] = true
		delete(c.engine.repairHandoffs, payloadObject{m.Key, c.engine.commandUIDs[m.ID]})
		c.engine.results[m.ID] = append(state.Value(nil), m.Result...)
		s := c.engine.shard(m.Key)
		delete(s.queued, m.ID)
		delete(s.shared, m.ID)
	}
	c.RegisterReply(m.Result, m.ID.Seq)
}

// Fair, bounded cache repair. Queries never propose a PUT or authorize completion.
func (c *Client) queryResults() {
	count := len(c.queryQueue) - c.queryHead
	if count > 256 {
		count = 256
	}
	budget := 32
	for ; count > 0; count-- {
		id := c.queryQueue[c.queryHead]
		c.queryHead++
		q := c.requests[id]
		if q == nil {
			continue
		}
		c.queryQueue = append(c.queryQueue, id)
		if c.engine.now < q.nextQuery || budget == 0 {
			continue
		}
		if q.record.Command.Op == state.GET {
			if q.delegate < 0 || c.Connection(q.delegate) == nil || c.PeerFailed(q.delegate) {
				q.delegate = c.selectedDelegate()
				c.sendRead(q)
			}
			// A live delegate will deliver this GET's own result. Other voters
			// do not execute this read and cannot answer a cache lookup for it.
			q.nextQuery = c.engine.now + c.engine.retry
			continue
		}
		if q.delegate >= 0 && c.Connection(q.delegate) != nil && !c.PeerFailed(q.delegate) {
			q.nextQuery = c.engine.now + c.engine.retry
			continue
		}
		for attempt := 0; attempt < c.engine.n; attempt++ {
			peer := q.queryPeer
			if peer < 0 {
				peer = 0
			}
			q.queryPeer = (peer + 1) % c.engine.n
			if c.Connection(peer) == nil || c.PeerFailed(peer) {
				continue
			}
			c.engine.send(peer, message{Kind: resultQuery, Key: q.record.Command.K, ID: id})
			budget--
			break
		}
		q.queryDelay *= 2
		if q.queryDelay > 8*time.Second {
			q.queryDelay = 8 * time.Second
		}
		q.nextQuery = c.engine.now + q.queryDelay
	}
	if c.queryHead > 4096 && c.queryHead*2 >= len(c.queryQueue) {
		copy(c.queryQueue, c.queryQueue[c.queryHead:])
		c.queryQueue = c.queryQueue[:len(c.queryQueue)-c.queryHead]
		c.queryHead = 0
	}
}

func (c *Client) sendRead(q *clientRequest) {
	if q.delegate >= 0 {
		r := q.record
		c.engine.send(q.delegate, message{Kind: delegateRead, Key: r.Command.K, ID: r.ID, Request: &r})
	}
}
