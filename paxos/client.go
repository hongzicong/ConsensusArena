package paxos

import (
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/hongzicong/ConsensusArena/client"
	"github.com/hongzicong/ConsensusArena/replica/defs"
)

// Client owns this protocol's routing and selects executed-result completion.
type Client struct {
	client.StandardClient
	lookup   time.Time
	sendMu   sync.Mutex
	mu       sync.Mutex
	pending  map[int32]pendingProposal
	stop     chan struct{}
	done     chan struct{}
	closed   bool
	started  bool
	attempts uint64
	retries  uint64
}

type pendingProposal struct {
	proposal defs.Propose
	attempt  time.Time
}

const requestRetryInterval = time.Second

func NewClient(b *client.BufferClient, _ int) *Client {
	c := &Client{StandardClient: client.StandardClient{BufferClient: b}, pending: make(map[int32]pendingProposal), stop: make(chan struct{}), done: make(chan struct{})}
	b.SetProtocol(c)
	return c
}

var _ client.Adapter = (*Client)(nil)

func (c *Client) Start() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return fmt.Errorf("paxos client is closed")
	}
	if c.started {
		return nil
	}
	c.started = true
	go c.retryPending()
	return nil
}

func (c *Client) Close() {
	c.mu.Lock()
	if !c.closed {
		c.closed = true
		close(c.stop)
	}
	started := c.started
	c.mu.Unlock()
	if started {
		<-c.done
	}
}

func (c *Client) WaitReplies(_ int) {
	c.WaitAnyRepliesWithHandler(func(r *defs.ProposeReplyTS) {
		c.mu.Lock()
		delete(c.pending, r.CommandId)
		c.mu.Unlock()
	})
}

func (c *Client) RetryPolicy() string {
	return "protocol retry after 1s; original client/command ID and payload; executed-result deduplication"
}

func (c *Client) RetryStats() map[string]uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return map[string]uint64{"attempts": c.attempts, "retry_attempts": c.retries, "pending": uint64(len(c.pending))}
}

// Retain the original request until completion, including failed/unsent writes.
// Replaying its identity lets Core return Values or finish its existing slot.
func (c *Client) SendProposal(p defs.Propose) {
	p.Command.V = append([]byte(nil), p.Command.V...)
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}
	c.pending[p.CommandId] = pendingProposal{proposal: p}
	c.mu.Unlock()
	c.send(p.CommandId, false)
}

func (c *Client) retryPending() {
	defer close(c.done)
	ticker := time.NewTicker(requestRetryInterval)
	defer ticker.Stop()
	for {
		select {
		case <-c.stop:
			return
		case now := <-ticker.C:
			c.mu.Lock()
			ids := make([]int32, 0, len(c.pending))
			for id, p := range c.pending {
				if now.Sub(p.attempt) >= requestRetryInterval {
					ids = append(ids, id)
				}
			}
			c.mu.Unlock()
			sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
			for _, id := range ids {
				c.send(id, true)
			}
		}
	}
}

func (c *Client) send(sequence int32, retry bool) {
	// The workload and timer share leader lookup and the stream write order.
	c.sendMu.Lock()
	defer c.sendMu.Unlock()
	c.mu.Lock()
	p, ok := c.pending[sequence]
	if !ok || c.closed || (retry && time.Since(p.attempt) < requestRetryInterval) {
		c.mu.Unlock()
		return
	}
	p.attempt = time.Now()
	c.pending[sequence] = p
	c.attempts++
	if retry {
		c.retries++
	}
	c.mu.Unlock()

	id := c.LeaderId
	valid := func(id int) bool {
		return id >= 0 && id < c.PeerCount() && !c.PeerFailed(id) && c.Connection(id) != nil
	}
	if (!valid(id) || retry) && time.Since(c.lookup) >= requestRetryInterval {
		c.lookup = time.Now()
		if leader, err := c.LookupLeader(); err == nil && valid(leader) {
			id = leader
			c.LeaderId = id
		}
	}
	if !valid(id) {
		return
	}
	c.WriteProposalTo(id, p.proposal)
}
