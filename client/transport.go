package client

import (
	"bufio"
	"net"
	"sync"
	"time"

	"github.com/hongzicong/ConsensusArena/replica/defs"
)

// Adapters choose destinations; these accessors expose existing connections
// without making the common transport depend on protocol packages.
func (c *Client) PeerCount() int                { return len(c.servers) }
func (c *Client) Connection(id int) net.Conn    { return c.servers[id] }
func (c *Client) Writer(id int) *bufio.Writer   { return c.writers[id] }
func (c *Client) Reader(id int) *bufio.Reader   { return c.readers[id] }
func (c *Client) WriteMutex(id int) *sync.Mutex { return &c.writeMu[id] }
func (c *Client) ReplicaAddress(id int) string  { return c.replicas[id] }
func (c *Client) PeerFailed(id int) bool        { return c.peerDead[id].Load() }
func (c *Client) MarkFaultPeer(id int)          { c.markFaultPeer(id) }
func (c *Client) ObserveWriteError() {
	if c.fault != nil {
		c.fault.errors.Add(1)
	}
}

// LookupLeader performs one bounded control RPC. The adapter owns lookup timing.
func (c *Client) LookupLeader() (int, error) {
	reply := &defs.GetLeaderReply{}
	err := c.call(c.master, "Master.GetLeader", &defs.GetLeaderArgs{}, reply)
	return reply.LeaderId, err
}

// WriteProposalTo retains the existing lock, deadline and failure accounting.
// A failed send is never automatically retried: it may already have arrived.
func (c *Client) WriteProposalTo(id int, p defs.Propose) {
	conn := c.servers[id]
	c.writeMu[id].Lock()
	_ = conn.SetWriteDeadline(time.Now().Add(2 * time.Second))
	c.writers[id].WriteByte(defs.PROPOSE)
	p.Marshal(c.writers[id])
	err := c.writers[id].Flush()
	c.writeMu[id].Unlock()
	if err != nil {
		c.peerDead[id].Store(true)
		c.markFaultPeer(id)
		conn.Close()
		c.ObserveWriteError()
	}
}

// Custom adapters use this for framed-reader EOF and failed protocol writes.
func (c *Client) MarkPeerFailed(id int) {
	c.peerDead[id].Store(true)
	c.markFaultPeer(id)
	if c.servers[id] != nil {
		c.servers[id].Close()
	}
}
