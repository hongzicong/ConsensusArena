package curp

import (
	"math/bits"

	"github.com/hongzicong/ConsensusArena/client"
	"github.com/hongzicong/ConsensusArena/replica/defs"
	fastrpc "github.com/hongzicong/ConsensusArena/rpc"
	"github.com/hongzicong/ConsensusArena/state"
)

type completion struct {
	votes       uint64
	value       state.Value
	leaderReply bool
}

type Client struct {
	client.StandardClient
	N       int
	cs      CommunicationSupply
	ballot  int32
	pending map[defs.RequestID]*completion
}

func NewClient(b *client.BufferClient, n int) *Client {
	c := &Client{StandardClient: client.StandardClient{BufferClient: b}, N: n, ballot: -1, pending: map[defs.RequestID]*completion{}}
	b.MonitorRPCFailures = true
	b.SetProtocol(c)
	return c
}

func (c *Client) evidence(id defs.RequestID, ballot, replica int32) *completion {
	if id.Client != c.ClientId || replica < 0 || int(replica) >= c.N || ballot < c.ballot {
		return nil
	}
	if c.ReplyCompleted(id) {
		return nil
	}
	if ballot > c.ballot {
		// A quorum must belong to one ballot. Never combine witness records across elections.
		c.ballot = ballot
		c.pending = map[defs.RequestID]*completion{}
	}
	p := c.pending[id]
	if p == nil {
		p = &completion{}
		c.pending[id] = p
	}
	return p
}

func (c *Client) finish(id defs.RequestID, v state.Value) {
	c.CompleteReply(id, v, func() { delete(c.pending, id) }, nil)
}

func (c *Client) tryFast(id defs.RequestID, p *completion) {
	f := c.N / 2
	if p.leaderReply && bits.OnesCount64(p.votes) >= f+(f+1)/2+1 {
		c.finish(id, p.value)
	}
}

func (c *Client) handleReply(r *MReply) {
	p := c.evidence(r.CmdId, r.Ballot, r.Replica)
	if p == nil || r.Ok != TRUE || r.Replica != r.Ballot%int32(c.N) {
		return
	}
	p.value = append(state.Value(nil), r.Rep...)
	p.leaderReply = true
	p.votes |= uint64(1) << r.Replica
	c.tryFast(r.CmdId, p)
}

func (c *Client) handleRecordAck(r *MRecordAck) {
	p := c.evidence(r.CmdId, r.Ballot, r.Replica)
	if p == nil || r.Ok != TRUE {
		return
	}
	p.votes |= uint64(1) << r.Replica
	c.tryFast(r.CmdId, p)
}

func (c *Client) handleSyncReply(r *MSyncReply) {
	if c.evidence(r.CmdId, r.Ballot, r.Replica) == nil {
		return
	}
	// Sent only after a majority-chosen command executes, including cached replay.
	c.finish(r.CmdId, append(state.Value(nil), r.Rep...))
}

func (c *Client) handleMsgs() {
	for {
		select {
		case m := <-c.cs.replyChan:
			c.handleReply(m.(*MReply))
		case m := <-c.cs.recordAckChan:
			c.handleRecordAck(m.(*MRecordAck))
		case m := <-c.cs.syncReplyChan:
			c.handleSyncReply(m.(*MSyncReply))
		}
	}
}

var _ client.Adapter = (*Client)(nil)

func (c *Client) WaitReplies(_ int) {
	table := fastrpc.NewTableId(defs.RPC_TABLE)
	initCs(&c.cs, table)
	c.RegisterRPCTable(table)
	go c.handleMsgs()
}

// Broadcast to surviving connections; a failed write is not replayed.
func (c *Client) SendProposal(p defs.Propose) {
	for id := 0; id < c.PeerCount(); id++ {
		if c.Connection(id) == nil || c.PeerFailed(id) {
			continue
		}
		c.WriteProposalTo(id, p)
	}
}
