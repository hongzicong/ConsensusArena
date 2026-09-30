package client

import (
	"github.com/hongzicong/ConsensusArena/replica/defs"
	"sync"
	"time"
)

func (c *Client) sendFastPaxos(p defs.Propose) {
	for id, conn := range c.servers {
		if conn == nil || c.fastPaxosDead[id].Load() {
			continue
		}
		c.writeMu[id].Lock()
		_ = conn.SetWriteDeadline(time.Now().Add(2 * time.Second))
		c.writers[id].WriteByte(defs.PROPOSE)
		p.Marshal(c.writers[id])
		err := c.writers[id].Flush()
		c.writeMu[id].Unlock()
		if err != nil {
			c.fastPaxosDead[id].Store(true)
			c.markFaultPeer(id)
			conn.Close()
			if c.fault != nil {
				c.fault.errors.Add(1)
			}
		}
	}
}

func (c *BufferClient) waitFastPaxos() {
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
					c.fastPaxosDead[id].Store(true)
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
					c.RegisterReply(r.Value, r.CommandId)
				}
			}
		}(id)
	}
}
