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
	core        *core
	inbox       chan message
	control     chan message
	resultInbox chan message
	proposals   chan Record
	peers       *replica.PeerStreams
	stop        chan struct{}
	closeOnce   sync.Once
	listener    net.Listener
	requests    map[defs.RequestID]*clientRequest
	readersOnce sync.Once
	queryQueue  []defs.RequestID
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
	c := &Client{StandardClient: client.StandardClient{BufferClient: b}, topology: topology, core: newCore(pid, topology.Plan, conf.KCensusFailure, conf.KCensusShards), inbox: make(chan message, 65536), resultInbox: make(chan message, 4096), proposals: make(chan Record, 65536), stop: make(chan struct{}), requests: make(map[defs.RequestID]*clientRequest)}
	c.Printf("KCENSUS_SHARDS client=%d count=%d mapping=identity-if-zero-otherwise-modulo-v1", pid, conf.KCensusShards)
	b.ClientID = int32(pid)
	c.control = make(chan message, 4*c.core.m)
	b.SkipPing = true
	b.ClosestID = topology.Plan.Leaders[pid]
	c.peers = replica.NewPeerStreams(len(topology.Aliases))
	b.SetProtocol(c)
	return c
}

var _ client.Adapter = (*Client)(nil)

func (*Client) RetryPolicy() string {
	return "GET: original-ID delegate failover; PUT: executed-result cache queries; protocol evidence/payload repair"
}

func (c *Client) Start() error {
	c.ClosestID = c.core.plan.Leaders[c.core.id]
	if c.PeerCount() != c.core.n {
		return fmt.Errorf("KCensus client membership differs from configured topology")
	}
	listener, err := net.Listen("tcp", c.topology.Listeners[c.core.id])
	if err != nil {
		return fmt.Errorf("KCensus client listener: %w", err)
	}
	c.listener = listener
	c.Printf("KCENSUS_CLIENT pid=%d alias=%s delegate=%d digest=%x listener=%s", c.core.id, c.topology.Aliases[c.core.id], c.core.plan.Leaders[c.core.id], c.core.plan.Digest, listener.Addr())
	go c.run()
	go c.acceptMesh()
	for id := c.core.id + 1; id < c.core.m; id++ {
		go c.dialMesh(id)
	}
	return nil
}

func (c *Client) WaitReplies(_ int) {
	c.readersOnce.Do(func() {
		select {
		case <-c.stop:
			return
		default:
		}
		for id := 0; id < c.core.n; id++ {
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
	case c.proposals <- Record{defs.RequestID{Client: int32(c.core.id), Sequence: p.CommandId}, p.Command}:
	case <-c.stop:
	}
}

func (c *Client) Close() {
	c.closeOnce.Do(func() {
		close(c.stop)
		if c.listener != nil {
			c.listener.Close()
		}
		c.peers.Close()
	})
}

func (c *Client) selectedDelegate() int {
	leader := c.core.plan.Leaders[c.core.id]
	if c.Connection(leader) != nil && !c.PeerFailed(leader) {
		return leader
	}
	alive := make([]bool, c.core.n)
	for id := range alive {
		alive[id] = c.Connection(id) != nil && !c.PeerFailed(id)
	}
	if delegate := survivingReadDelegate(c.topology.Latency, c.core.id, c.core.n, c.core.f, alive); delegate >= 0 {
		return delegate
	}
	for _, id := range c.core.priority {
		if id < c.core.n && c.Connection(id) != nil && !c.PeerFailed(id) {
			return id
		}
	}
	return -1
}

func (c *Client) receiveResult(m message) {
	q := c.requests[m.ID]
	if q == nil || !c.core.acceptsResult(m, q.record) || c.ReplyCompleted(m.ID) {
		return
	}
	c.CompleteReply(m.ID, m.Result, func() {
		delete(c.requests, m.ID)
		c.core.recordResult(m, q.record)
	}, nil)
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
		if c.core.now < q.nextQuery || budget == 0 {
			continue
		}
		if q.record.Command.Op == state.GET {
			if q.delegate < 0 || c.Connection(q.delegate) == nil || c.PeerFailed(q.delegate) {
				q.delegate = c.selectedDelegate()
				c.sendRead(q)
			}
			// A live delegate will deliver this GET's own result. Other voters
			// do not execute this read and cannot answer a cache lookup for it.
			q.nextQuery = c.core.now + c.core.retry
			continue
		}
		if q.delegate >= 0 && c.Connection(q.delegate) != nil && !c.PeerFailed(q.delegate) {
			q.nextQuery = c.core.now + c.core.retry
			continue
		}
		for attempt := 0; attempt < c.core.n; attempt++ {
			peer := q.queryPeer
			if peer < 0 {
				peer = 0
			}
			q.queryPeer = (peer + 1) % c.core.n
			if c.Connection(peer) == nil || c.PeerFailed(peer) {
				continue
			}
			c.core.send(peer, message{Kind: resultQuery, Shard: c.core.shardFor(q.record.Command.K), ID: id})
			budget--
			break
		}
		q.queryDelay *= 2
		if q.queryDelay > 8*time.Second {
			q.queryDelay = 8 * time.Second
		}
		q.nextQuery = c.core.now + q.queryDelay
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
		c.core.send(q.delegate, message{Kind: delegateRead, Shard: c.core.shardFor(r.Command.K), ID: r.ID, Request: &r})
	}
}
