package swift

import (
	"github.com/hongzicong/ConsensusArena/client"
	"github.com/hongzicong/ConsensusArena/replica"
	"github.com/hongzicong/ConsensusArena/replica/defs"
	fastrpc "github.com/hongzicong/ConsensusArena/rpc"
	"github.com/hongzicong/ConsensusArena/state"
)

type Client struct {
	*client.BufferClient

	val       state.Value
	ready     chan struct{}
	ballot    int32
	delivered map[CommandId]struct{}

	SQ         replica.QuorumI
	FQ         replica.QuorumI
	slowPathH  map[CommandId]*replica.MsgSet
	fastPathH  map[CommandId]*replica.MsgSet
	ackBallots map[CommandId]int32

	fixedMajority bool

	slowPaths   int
	alreadySlow map[CommandId]struct{}

	cs CommunicationSupply
}

func NewClient(b *client.BufferClient, repNum int) *Client {
	c := &Client{
		BufferClient: b,

		val:       nil,
		ready:     make(chan struct{}, 1),
		ballot:    -1,
		delivered: make(map[CommandId]struct{}),

		SQ: replica.NewMajorityOf(repNum),
		FQ: replica.NewThreeQuartersOf(repNum),

		slowPathH:  make(map[CommandId]*replica.MsgSet),
		fastPathH:  make(map[CommandId]*replica.MsgSet),
		ackBallots: make(map[CommandId]int32),

		fixedMajority: true,

		slowPaths:   0,
		alreadySlow: make(map[CommandId]struct{}),
	}

	t := fastrpc.NewTableId(defs.RPC_TABLE)
	initCs(&c.cs, t)
	c.RegisterRPCTable(t)

	if c.fixedMajority {
		// TODO: it has to be the correct majority
		c.FQ = replica.NewMajorityOf(repNum)
	}

	c.Println("SQ:", c.SQ)
	c.Println("FQ:", c.FQ)

	go c.handleMsgs()

	return c
}

func (c *Client) initMsgSets(cmdId CommandId) {
	m, exists := c.slowPathH[cmdId]
	initSlow := !exists || m == nil
	m, exists = c.fastPathH[cmdId]
	initFast := !exists || m == nil

	if !initSlow && !initFast {
		return
	}

	accept := func(msg, leaderMsg interface{}) bool {
		if leaderMsg == nil {
			return true
		}

		leaderFastAck := leaderMsg.(*MFastAck)
		fastAck := msg.(*MFastAck)

		depEq := Dep(fastAck.Dep).Equals(Dep(leaderFastAck.Dep))
		if _, exists := c.alreadySlow[fastAck.CmdId]; !depEq && !exists {
			c.slowPaths++
			c.alreadySlow[fastAck.CmdId] = struct{}{}
		}

		hashEq := SHashesEq(leaderFastAck.Checksum, fastAck.Checksum)
		if !hashEq {
			return false
		}

		return fastAck.Checksum == nil || hashEq
	}

	free := func(msg interface{}) {
		switch f := msg.(type) {
		case *MFastAck:
			releaseFastAck(f)
		}
	}

	if initSlow {
		c.slowPathH[cmdId] = c.slowPathH[cmdId].ReinitMsgSet(c.SQ, accept, free, c.handleFastAndSlowAcks)
	}
	if initFast {
		c.fastPathH[cmdId] = c.fastPathH[cmdId].ReinitMsgSet(c.FQ, accept, free, c.handleFastAndSlowAcks)
	}
}

func (c *Client) handleMsgs() {
	for {
		select {
		case m := <-c.cs.replyChan:
			reply := m.(*MReply)
			c.handleReply(reply)

		case m := <-c.cs.acceptChan:
			accept := m.(*MAccept)
			c.handleAccept(accept)

		case m := <-c.cs.fastAckChan:
			fastAck := m.(*MFastAck)
			c.handleFastAck(fastAck, false)

		case m := <-c.cs.fastAckClientChan:
			fastAckClient := m.(*MFastAckClient)
			f := newFastAck()
			f.Replica = fastAckClient.Replica
			f.Ballot = fastAckClient.Ballot
			f.CmdId = fastAckClient.CmdId
			f.Checksum = fastAckClient.Checksum
			c.handleFastAck(f, false)

		case m := <-c.cs.lightSlowAckChan:
			lightSlowAck := m.(*MLightSlowAck)
			c.handleLightSlowAck(lightSlowAck)

		case m := <-c.cs.acksChan:
			acks := m.(*MAcks)
			for _, f := range acks.FastAcks {
				c.handleFastAck(copyFastAck(&f), false)
			}
			for _, s := range acks.LightSlowAcks {
				ls := s
				c.handleLightSlowAck(&ls)
			}

		case m := <-c.cs.optAcksChan:
			optAcks := m.(*MOptAcks)
			for _, ack := range optAcks.Acks {
				fastAck := newFastAck()
				fastAck.Replica = optAcks.Replica
				fastAck.Ballot = optAcks.Ballot
				fastAck.CmdId = ack.CmdId
				if _, exists := c.delivered[fastAck.CmdId]; exists {
					continue
				}
				if !IsNilDepOfCmdId(ack.CmdId, ack.Dep) {
					fastAck.Checksum = ack.Checksum
				} else {
					if _, exists := c.alreadySlow[fastAck.CmdId]; !exists {
						c.slowPaths++
						c.alreadySlow[fastAck.CmdId] = struct{}{}
					}
					fastAck.Checksum = nil
				}
				if !c.handleFastAck(fastAck, false) {
					continue
				}
				if _, exists := c.delivered[fastAck.CmdId]; !exists && fastAck.Checksum == nil {
					fastAck := copyFastAck(fastAck)
					fastAck.Checksum = nil
					c.initMsgSets(fastAck.CmdId)
					c.slowPathH[fastAck.CmdId].Add(fastAck.Replica, false, fastAck)
				}
			}
		}
	}
}

func (c *Client) handleFastAck(f *MFastAck, fromLeader bool) bool {
	if c.ballot == -1 {
		c.ballot = f.Ballot
	} else if c.ballot < f.Ballot {
		c.ballot = f.Ballot
	} else if c.ballot > f.Ballot {
		return false
	}

	if _, exists := c.delivered[f.CmdId]; exists {
		return false
	}
	if b, exists := c.ackBallots[f.CmdId]; !exists || b != f.Ballot {
		// Previous-ballot acknowledgements cannot count in this ballot.
		// Release the maps by dropping ownership, not by returning aliased
		// messages to pools while another set could still reference them.
		delete(c.fastPathH, f.CmdId)
		delete(c.slowPathH, f.CmdId)
		c.ackBallots[f.CmdId] = f.Ballot
	}

	c.initMsgSets(f.CmdId)
	c.fastPathH[f.CmdId].Add(f.Replica, fromLeader, f)
	return true
}

func (c *Client) handleLightSlowAck(ls *MLightSlowAck) {
	if _, exists := c.delivered[ls.CmdId]; exists {
		return
	}

	if _, exists := c.alreadySlow[ls.CmdId]; !exists {
		c.slowPaths++
		c.alreadySlow[ls.CmdId] = struct{}{}
	}

	f := newFastAck()
	f.Replica = ls.Replica
	f.Ballot = ls.Ballot
	f.CmdId = ls.CmdId
	f.Checksum = nil
	if !c.handleFastAck(f, false) {
		return
	}
	if _, exists := c.delivered[f.CmdId]; !exists {
		f := copyFastAck(f)
		f.Checksum = nil
		c.initMsgSets(f.CmdId)
		c.slowPathH[f.CmdId].Add(f.Replica, false, f)
	}
}

func (c *Client) handleFastAndSlowAcks(leaderMsg interface{}, msgs []interface{}) {
	if leaderMsg == nil {
		return
	}

	cmdId := leaderMsg.(*MFastAck).CmdId
	if _, exists := c.delivered[cmdId]; exists {
		return
	}
	c.delivered[cmdId] = struct{}{}
	c.RegisterReply(c.val, cmdId.SeqNum)

	c.Println("Slow Paths:", c.slowPaths)
}

func (c *Client) handleReply(r *MReply) {
	if _, exists := c.delivered[r.CmdId]; exists {
		return
	}
	f := newFastAck()
	f.Replica = r.Replica
	f.Ballot = r.Ballot
	f.CmdId = r.CmdId
	f.Checksum = r.Checksum
	c.val = r.Rep
	if !c.handleFastAck(f, true) {
		return
	}
	if _, exists := c.delivered[f.CmdId]; !exists {
		f := copyFastAck(f)
		f.Checksum = nil
		c.initMsgSets(f.CmdId)
		c.slowPathH[f.CmdId].Add(f.Replica, true, f)
	}
}

func (c *Client) handleAccept(a *MAccept) {
	if _, exists := c.delivered[a.CmdId]; exists {
		return
	}
	c.delivered[a.CmdId] = struct{}{}

	c.val = a.Rep
	c.RegisterReply(c.val, a.CmdId.SeqNum)
	c.Println("Slow Paths:", c.slowPaths)
}
