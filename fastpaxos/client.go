package fastpaxos

import (
	"github.com/hongzicong/ConsensusArena/client"
	"github.com/hongzicong/ConsensusArena/replica/defs"
)

// Client owns this protocol's routing and selects executed-result completion.
type Client struct {
	client.StandardClient
}

func NewClient(b *client.BufferClient, _ int) *Client {
	c := &Client{StandardClient: client.StandardClient{BufferClient: b}}
	b.SetProtocol(c)
	return c
}

var _ client.Adapter = (*Client)(nil)

// Broadcast to surviving connections; a failed write is not replayed.
func (c *Client) SendProposal(p defs.Propose) {
	for id := 0; id < c.PeerCount(); id++ {
		if c.Connection(id) == nil || c.PeerFailed(id) {
			continue
		}
		c.WriteProposalTo(id, p)
	}
}
