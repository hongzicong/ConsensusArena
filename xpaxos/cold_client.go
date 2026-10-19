package xpaxos

import (
	"encoding/binary"
	"encoding/json"
	"github.com/hongzicong/ConsensusArena/replica/defs"
	"github.com/hongzicong/ConsensusArena/state"
	"math/bits"
	"time"
)

func (c *Client) sendColdWrite(cmd defs.Propose, origin int) bool {
	if !c.coldEnabled || cmd.Command.Op != state.PUT || !c.keyPolicy.cold(cmd.Command.K, c.coldCut) {
		return false
	}
	hint := c.bodega.roster.Load()
	if hint != nil && hint.ColdMigrated {
		return false
	}
	g := coldGeneration{}
	q := c.coldQ
	leader := c.initialLeader
	if hint != nil {
		g = coldGeneration{hint.ColdBallot, hint.ColdEpoch}
		leader = hint.Leader
		if hint.ColdQuorum != 0 {
			q = hint.ColdQuorum
		}
	}
	env := cmd
	env.Command.Op = coldPUT
	env.Command.V = make(state.Value, 20+len(cmd.Command.V))
	binary.LittleEndian.PutUint64(env.Command.V[:8], g.B)
	binary.LittleEndian.PutUint64(env.Command.V[8:16], g.E)
	binary.LittleEndian.PutUint32(env.Command.V[16:20], uint32(origin))
	copy(env.Command.V[20:], cmd.Command.V)
	for i := 0; i < c.PeerCount(); i++ {
		if q&bit(i) != 0 && c.bodegaPeerAlive(i) {
			c.coldSent.Add(1)
			c.writeBodegaProposal(env, i, false)
		}
	}
	if q&bit(leader) == 0 && c.bodegaPeerAlive(leader) {
		c.coldSent.Add(1)
		c.writeBodegaProposal(env, leader, false)
	}
	return true
}
func (c *Client) startColdProofs() {
	if !c.coldEnabled {
		return
	}
	c.coldProofQueue = make(chan coldMessage, 8192)
	go func() {
		for {
			select {
			case <-c.bodega.stop:
				return
			case m := <-c.coldProofQueue:
				data, _ := json.Marshal(m.Record)
				cmd := m.Request.Proposal
				cmd.Command.Op = coldProof
				cmd.Command.V = make(state.Value, 4+len(data)+len(m.Request.Proposal.Command.V))
				binary.LittleEndian.PutUint32(cmd.Command.V[:4], uint32(len(data)))
				copy(cmd.Command.V[4:], data)
				copy(cmd.Command.V[4+len(data):], m.Request.Proposal.Command.V)
				for i := 0; i < c.PeerCount(); i++ {
					if !c.bodegaPeerAlive(i) {
						continue
					}
					c.WriteMutex(i).Lock()
					_ = c.Connection(i).SetWriteDeadline(time.Now().Add(2 * time.Second))
					_ = c.Writer(i).WriteByte(defs.PROPOSE)
					cmd.Marshal(c.Writer(i))
					err := c.Writer(i).Flush()
					c.WriteMutex(i).Unlock()
					if err != nil {
						c.ObserveWriteError()
						c.markBodegaPeer(i)
					}
				}
			}
		}
	}()
}
func (c *Client) acceptColdReply(reply *defs.ProposeReplyTS, source int) bool {
	if !c.coldEnabled || source < 0 || source >= c.PeerCount() {
		return false
	}
	b := c.bodega
	b.mu.Lock()
	p := b.pending[reply.CommandId]
	if p == nil || p.cmd.Command.Op != state.PUT || !c.keyPolicy.cold(p.cmd.Command.K, c.coldCut) {
		b.mu.Unlock()
		return false
	}
	hash := commandDigest(p.cmd.Command)
	if reply.OK == coldDoneOK {
		var d coldID
		ok := json.Unmarshal(reply.Value, &d) == nil && d.ID == p.cmd.RequestID() && d.Hash == hash && d.Key == p.cmd.Command.K
		b.mu.Unlock()
		if !ok || !c.completeBodegaRequestKind(reply.CommandId, source, true) {
			return false
		}
		c.coldDone.Add(1)
		reply.OK = defs.TRUE
		reply.Value = nil
		return true
	}
	var v ColdVoteIdentity
	if json.Unmarshal(reply.Value, &v) != nil || v.ID != p.cmd.RequestID() || v.Key != p.cmd.Command.K || v.Hash != hash || v.Origin != p.initial || v.Round.G.B == 0 || v.Version == 0 || bits.OnesCount64(v.Q) != c.PeerCount()/2+1 || v.Q>>uint(c.PeerCount()) != 0 || v.Q&bit(source) == 0 {
		b.mu.Unlock()
		return false
	}
	votes := c.coldVotes[reply.CommandId]
	if votes == nil {
		votes = map[ColdVoteIdentity]uint64{}
		c.coldVotes[reply.CommandId] = votes
	}
	if len(votes) >= 8 && votes[v] == 0 {
		b.mu.Unlock()
		return false
	}
	votes[v] |= bit(source)
	if votes[v]&v.Q != v.Q {
		b.mu.Unlock()
		return false
	}
	m := coldMessage{Type: "chosen", G: v.Round.G, Record: coldRecord{ColdVoteIdentity: v, Fast: true, Voters: v.Q}, Request: request{Proposal: p.cmd, Origin: p.initial}}
	select {
	case c.coldProofQueue <- m:
	default:
		c.coldProofErrors.Add(1)
		b.mu.Unlock()
		return false
	}
	delete(c.coldVotes, reply.CommandId)
	b.mu.Unlock()
	if !c.completeBodegaRequestKind(reply.CommandId, source, true) {
		return false
	}
	c.coldWins.Add(1)
	reply.OK = defs.TRUE
	reply.Value = nil
	return true
}
