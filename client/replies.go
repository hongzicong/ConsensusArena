package client

import (
	"sync"

	"github.com/hongzicong/ConsensusArena/replica/defs"
)

// WaitAnyReplies accepts an executed result from any connection and delivers
// each command ID once. The protocol adapter selects this completion policy.
func (c *BufferClient) WaitAnyReplies() {
	c.WaitAnyRepliesWithHandler(nil)
}

// The handler runs once per completed command, before workload notification.
// Adapters use it to release requests retained for retransmission.
func (c *BufferClient) WaitAnyRepliesWithHandler(onReply func(*defs.ProposeReplyTS)) {
	var mu sync.Mutex
	completed := make(map[int32]bool)
	for id, reader := range c.readers {
		if reader == nil {
			continue
		}
		go func(id int) {
			for {
				r, err := c.GetReplyFrom(id)
				if err != nil {
					c.peerDead[id].Store(true)
					c.markFaultPeer(id)
					return
				}
				if r.OK != defs.TRUE {
					continue
				}
				mu.Lock()
				seen := completed[r.CommandId]
				completed[r.CommandId] = true
				mu.Unlock()
				if !seen {
					if onReply != nil {
						onReply(r)
					}
					c.RegisterReply(r.Value, r.CommandId)
				}
			}
		}(id)
	}
}
