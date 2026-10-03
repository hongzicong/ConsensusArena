package client

import (
	"bufio"

	"github.com/hongzicong/ConsensusArena/replica/defs"
	fastrpc "github.com/hongzicong/ConsensusArena/rpc"
)

func (c *Client) GetReplyFrom(rid int) (*defs.ProposeReplyTS, error) {
	rep := &defs.ProposeReplyTS{}
	err := rep.Unmarshal(c.readers[rid])
	return rep, err
}

// ReadReplies preserves the untagged executed-result format. The adapter owns
// completion policy and failure handling; false stops without reading ahead.
func (c *Client) ReadReplies(rid int, receive func(*defs.ProposeReplyTS) bool) error {
	return fastrpc.ReadLoop(func() (*defs.ProposeReplyTS, error) {
		return c.GetReplyFrom(rid)
	}, receive)
}

func (c *Client) RegisterRPCTable(t *fastrpc.Table) {
	for i, reader := range c.readers {
		go func(i int, reader *bufio.Reader) {
			if reader == nil {
				return
			}
			err := fastrpc.ReadStream(reader, t.Decode, fastrpc.Pair.Deliver)
			if err != nil {
				if c.MonitorRPCFailures {
					c.peerDead[i].Store(true)
				}
				c.markFaultPeer(i)
			}
		}(i, reader)
	}
}
