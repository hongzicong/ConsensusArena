package client

import "github.com/hongzicong/ConsensusArena/replica/defs"

// WaitAnyReplies accepts an executed result from any connection and delivers
// each command ID once. The protocol adapter selects this completion policy.
func (c *BufferClient) WaitAnyReplies() {
	c.WaitAnyRepliesWithHandler(nil)
}

// The handler runs once per completed command, before workload notification.
// Adapters use it to release requests retained for retransmission.
func (c *BufferClient) WaitAnyRepliesWithHandler(onReply func(*defs.ProposeReplyTS)) {
	for id, reader := range c.readers {
		if reader == nil {
			continue
		}
		go func(id int) {
			err := c.ReadReplies(id, func(r *defs.ProposeReplyTS) bool {
				if r.OK != defs.TRUE {
					return true
				}
				c.CompleteReply(defs.RequestID{Client: c.ClientId, Sequence: r.CommandId}, r.Value, func() {
					if onReply != nil {
						onReply(r)
					}
				}, nil)
				return true
			})
			if err != nil {
				c.peerDead[id].Store(true)
				c.markFaultPeer(id)
			}
		}(id)
	}
}
