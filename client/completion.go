package client

import (
	"sync"
	"time"

	"github.com/hongzicong/ConsensusArena/replica/defs"
	fastrpc "github.com/hongzicong/ConsensusArena/rpc"
	"github.com/hongzicong/ConsensusArena/state"
)

// Completion records survive ballot changes. Protocol adapters decide when
// evidence authorizes completion; this component only publishes it once.
type replyCompletions struct {
	mu  sync.RWMutex
	ids map[defs.RequestID]struct{}
}

func (c *BufferClient) ReplyCompleted(id defs.RequestID) bool {
	c.completion.mu.RLock()
	_, ok := c.completion.ids[id]
	c.completion.mu.RUnlock()
	return ok
}

func (c *BufferClient) CompletedReplies() int {
	c.completion.mu.RLock()
	count := len(c.completion.ids)
	c.completion.mu.RUnlock()
	return count
}

// CompleteReply claims an executed result and releases adapter state before
// workload notification. The timestamp is observed before cleanup. No adapter
// callback or channel send runs under the completion lock. stop may be nil.
// False means a foreign/duplicate result or cancellation of notification.
func (c *BufferClient) CompleteReply(id defs.RequestID, value state.Value, beforeNotify func(), stop <-chan struct{}) bool {
	if id.Client != c.ClientId {
		return false
	}
	observed := time.Now()
	c.completion.mu.Lock()
	if _, exists := c.completion.ids[id]; exists {
		c.completion.mu.Unlock()
		return false
	}
	if c.completion.ids == nil {
		c.completion.ids = make(map[defs.RequestID]struct{})
	}
	c.completion.ids[id] = struct{}{}
	c.completion.mu.Unlock()
	if beforeNotify != nil {
		beforeNotify()
	}
	return fastrpc.Deliver(c.Reply, &ReqReply{Val: value, Seqnum: int(id.Sequence), Time: observed}, stop)
}

func (c *BufferClient) RegisterReply(val state.Value, seqnum int32) {
	c.CompleteReply(defs.RequestID{Client: c.ClientId, Sequence: seqnum}, val, nil, nil)
}
