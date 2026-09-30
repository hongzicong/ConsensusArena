package client

import (
	"github.com/hongzicong/ConsensusArena/replica/defs"
	"time"
)

// Each request has exactly one ingress owner. Only NEW requests move away
// from a failed connection: EPaxos commands do not carry a replicated client
// deduplication key, so retrying an ambiguous write could execute it twice.
func (c *Client) sendEPaxos(p defs.Propose) {
	for offset := 0; offset < len(c.servers); offset++ {
		id := (c.ClosestId + offset) % len(c.servers)
		conn := c.servers[id]
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
		return
	}
}
