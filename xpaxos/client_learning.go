package xpaxos

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"github.com/hongzicong/ConsensusArena/replica/defs"
	"github.com/hongzicong/ConsensusArena/state"
	"math/bits"
	"time"
)

const registerLearner = state.Operation(251)
const clientVoteOK = uint8(2)

type clientCertificate struct {
	Ballot, Slot, Responders uint64
	Batch                    [32]byte
}

func commandDigest(cmd state.Command) [32]byte {
	var b bytes.Buffer
	b.WriteByte(byte(cmd.Op))
	_ = binary.Write(&b, binary.LittleEndian, int64(cmd.K))
	b.Write(cmd.V)
	return sha256.Sum256(b.Bytes())
}

func (r *Replica) sendClientVotes(v entry) {
	e := r.engine
	start := time.Now()
	defer func() {
		e.stats.CertificateDispatchSamples++
		e.stats.CertificateDispatchNanos += uint64(time.Since(start))
	}()
	local, ok := e.log[v.Slot]
	if !e.opt.ClientLearning || !v.ReadFresh || !ok || local.Ballot != v.Ballot || !sameEntry(local, v) || v.Ballot != e.current.Ballot {
		return
	}
	var b bytes.Buffer
	mask := uint64(0)
	for _, req := range v.requests() {
		_ = binary.Write(&b, binary.LittleEndian, int32(req.Origin))
		req.Proposal.Marshal(&b)
		if req.Proposal.Command.Op == state.PUT {
			mask |= e.current.respondersFor(req.Proposal.Command.K)
		}
	}
	batch := sha256.Sum256(b.Bytes())
	for _, req := range v.requests() {
		if req.Proposal.Command.Op != state.PUT || (!e.opt.AllKeyLearning && e.opt.KeyPolicy.rank(req.Proposal.Command.K) < int64(e.opt.KeyPolicy.hotCutoff)) {
			continue
		}
		value := make(state.Value, 88)
		binary.LittleEndian.PutUint64(value[0:8], v.Ballot)
		binary.LittleEndian.PutUint64(value[8:16], v.Slot)
		binary.LittleEndian.PutUint64(value[16:24], mask)
		copy(value[24:56], batch[:])
		digest := commandDigest(req.Proposal.Command)
		copy(value[56:88], digest[:])
		e.stats.ClientVoteCommands++
		if e.opt.AllKeyLearning {
			e.stats.AllKeyVoteCommands++
			if e.opt.KeyPolicy.hot(req.Proposal.Command.K) {
				e.stats.HotCertificateVoteCommands++
			}
		}
		reply := &defs.ProposeReplyTS{OK: clientVoteOK, CommandId: req.Proposal.CommandId, Value: value, Timestamp: req.Proposal.Timestamp}
		for _, stream := range r.learnerStreams[req.Proposal.ClientId] {
			if err := r.ReplyProposal(stream, reply, 8192); err != nil {
				e.stats.ClientVoteErrors++
			} else {
				e.stats.ClientVoteFrames++
				e.stats.ClientVoteBytes += 105
			}
		}
	}
}

func (c *Client) registerLearnerStreams() {
	if !c.learningEnabled {
		return
	}
	cmd := defs.Propose{ClientId: c.ClientId, CommandId: -1, Command: state.Command{Op: registerLearner}}
	for id := 0; id < c.PeerCount(); id++ {
		if c.bodegaPeerAlive(id) {
			c.WriteMutex(id).Lock()
			_ = c.Writer(id).WriteByte(defs.PROPOSE)
			cmd.Marshal(c.Writer(id))
			_ = c.Writer(id).Flush()
			c.WriteMutex(id).Unlock()
		}
	}
}

func (c *Client) acceptClientReply(reply *defs.ProposeReplyTS, source int) bool {
	if reply.OK == coldVoteOK || reply.OK == coldDoneOK {
		return c.acceptColdReply(reply, source)
	}
	if reply.OK == defs.TRUE {
		return c.completeBodegaRequest(reply.CommandId, source)
	}
	if reply.OK != clientVoteOK || !c.learningEnabled || len(reply.Value) != 88 || source < 0 || source >= c.PeerCount() {
		return false
	}
	c.learnerVoteReplies.Add(1)
	b := c.bodega
	b.mu.Lock()
	p := b.pending[reply.CommandId]
	if p == nil || p.cmd.Command.Op != state.PUT || (!c.allKeyLearning && c.keyPolicy.rank(p.cmd.Command.K) < int64(c.keyPolicy.hotCutoff)) {
		b.mu.Unlock()
		return false
	}
	digest := commandDigest(p.cmd.Command)
	if !bytes.Equal(digest[:], reply.Value[56:88]) {
		b.mu.Unlock()
		return false
	}
	cert := clientCertificate{Ballot: binary.LittleEndian.Uint64(reply.Value[:8]), Slot: binary.LittleEndian.Uint64(reply.Value[8:16]), Responders: binary.LittleEndian.Uint64(reply.Value[16:24])}
	copy(cert.Batch[:], reply.Value[24:56])
	if cert.Ballot == 0 || cert.Slot == 0 || cert.Responders == 0 || cert.Responders>>uint(c.PeerCount()) != 0 {
		b.mu.Unlock()
		return false
	}
	if p.certificates == nil {
		p.certificates = map[clientCertificate]uint64{}
	}
	mask, exists := p.certificates[cert]
	if !exists && len(p.certificates) >= 4 {
		b.mu.Unlock()
		return false
	}
	mask |= bit(source)
	p.certificates[cert] = mask
	chosen := bits.OnesCount64(mask) >= c.PeerCount()/2+1 && mask&cert.Responders == cert.Responders
	b.mu.Unlock()
	if !chosen || !c.completeBodegaRequestKind(reply.CommandId, source, true) {
		return false
	}
	c.learnerWins.Add(1)
	reply.OK = defs.TRUE
	reply.Value = nil
	return true
}

func (c *Client) startHotReadRace(cmd defs.Propose, primary int) {
	if !c.learningEnabled || cmd.Command.Op != state.GET || !c.keyPolicy.hot(cmd.Command.K) {
		return
	}
	hint := c.bodega.roster.Load()
	if hint == nil || hint.Ballot != c.initialBallot || hint.Leader != c.initialLeader {
		return
	}
	target := -1
	rank := c.keyPolicy.rank(cmd.Command.K)
	for _, span := range c.raceRanges {
		if rank >= span.Start && rank < span.End {
			target = span.Target
			break
		}
	}
	mask := defs.BodegaRespondersFor(hint.Responders, hint.Ranges, hint.Leader, int64(cmd.Command.K))
	if target < 0 || target == primary || !c.bodegaPeerAlive(target) || mask&bit(target) == 0 {
		return
	}
	b := c.bodega
	b.mu.Lock()
	p := b.pending[cmd.CommandId]
	if p == nil {
		b.mu.Unlock()
		return
	}
	if p.timer != nil {
		p.timer.Stop()
	}
	p.hedge = target
	b.mu.Unlock()
	c.readRaceSent.Add(1)
	c.writeBodegaProposal(cmd, target, true)
}
