package client

import (
	"github.com/hongzicong/ConsensusArena/replica/defs"
	"time"
)

// Move new commands after a broken leader connection. A failed write may have
// reached the old leader, so never replay it under a second Paxos instance.
func (c *Client) sendPaxos(p defs.Propose) {
	id := c.LeaderId
	if id < 0 || id >= len(c.servers) || c.fastPaxosDead[id].Load() {
		if time.Since(c.paxosLookup) < time.Second {
			return
		}
		c.paxosLookup = time.Now()
		reply := &defs.GetLeaderReply{}
		if err := c.call(c.master, "Master.GetLeader", &defs.GetLeaderArgs{}, reply); err != nil {
			return
		}
		id = reply.LeaderId
		if id < 0 || id >= len(c.servers) || c.fastPaxosDead[id].Load() {
			return
		}
		c.LeaderId = id
	}
	if c.servers[id] == nil {
		return
	}
	c.writeMu[id].Lock()
	_ = c.servers[id].SetWriteDeadline(time.Now().Add(2 * time.Second))
	c.writers[id].WriteByte(defs.PROPOSE)
	p.Marshal(c.writers[id])
	err := c.writers[id].Flush()
	c.writeMu[id].Unlock()
	if err != nil {
		c.fastPaxosDead[id].Store(true)
		c.markFaultPeer(id)
		c.servers[id].Close()
		if c.fault != nil {
			c.fault.errors.Add(1)
		}
	}
}
