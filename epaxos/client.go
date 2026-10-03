package epaxos

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

// Each request has one ingress owner. Only new requests move after a failure.
func (c *Client) SendProposal(p defs.Propose) {
	for offset := 0; offset < c.PeerCount(); offset++ {
		id := (c.ClosestId + offset) % c.PeerCount()
		if c.Connection(id) == nil || c.PeerFailed(id) {
			continue
		}
		c.WriteProposalTo(id, p)
		return
	}
}
