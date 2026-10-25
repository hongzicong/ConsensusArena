package swift

import (
	"log"

	"github.com/hongzicong/ConsensusArena/replica/defs"
)

// A recovery confirmation is not an ordinary hash-log update. Keep this
// explicit across both MAcks and MOptAcks without changing the wire layout.
const recoveryAckSeqnum = -1

func newRecoveryFastAck(replica, ballot int32, cmdID defs.RequestID, dep Dep) *MFastAck {
	msg := newFastAck()
	msg.Replica, msg.Ballot, msg.CmdId = replica, ballot, cmdID
	msg.Dep = dep
	if msg.Dep == nil {
		msg.Dep = NilDepOfCmdID(cmdID)
	}
	msg.Checksum = nil
	msg.Seqnum = recoveryAckSeqnum
	return msg
}

func (c *core) installRecoveryHashBoundary(phases map[defs.RequestID]int) {
	log.Printf("swift_recovery_hash_boundary ballot=%d installed_commands=%d discarded_pending_updates=%d", c.ballot, len(phases), len(c.pendingHashUpds))
	log.Printf("swift_proposal_batches batches=%d commands=%d max=%d", c.proposalBatches, c.proposalBatchCommands, c.proposalBatchMax)
	log.Printf("swift_deferred_hash_elisions commands=%d", c.deferredHashElisions)
	log.Printf("swift_hash_backlog_deferrals commands=%d", c.hashBacklogDeferrals)
	c.recoveryCmds = phases
	c.proposedInBallot = make(map[defs.RequestID]struct{})
	// These entries were received in the previous ballot. Their leader/sequence
	// context must not be applied when a delayed proposal arrives after Sync.
	c.pendingHashUpds = make(map[defs.RequestID]*UpdateEntry)
	removed := 0
	for _, log := range c.hlog {
		removed += log.BeginBallot(c.ballot)
	}
	log.Printf("swift_hash_epoch ballot=%d keys=%d sealed_pending_nodes=%d", c.ballot, len(c.hlog), removed)
}

func (c *core) recordLeaderAck(msg *MFastAck) {
	if msg.Ballot != c.ballot || msg.Replica != c.leader() || c.id == c.leader() {
		return
	}
	if msg.Seqnum == recoveryAckSeqnum {
		_, installed := c.recoveryCmds[msg.CmdId]
		if !installed || len(msg.Checksum) != 0 {
			c.Fatal("invalid recovery hash acknowledgement", msg.CmdId, msg.Ballot)
			return
		}
		// Keep dependency/quorum processing, but do not manufacture hash evidence.
		return
	}
	if msg.Seqnum < 0 {
		c.Fatal("invalid leader hash sequence", msg.CmdId, msg.Seqnum)
		return
	}
	c.recordLeaderHash(msg.CmdId, msg.Seqnum, msg.Checksum)
}

func (c *core) receiveFastAck(msg *MFastAck) {
	if c.status != NORMAL || msg.Ballot != c.ballot {
		return
	}
	c.recordLeaderAck(msg)
	c.getCmdDescSeq(msg.CmdId, msg, nil, nil, c.leader() == c.id)
}

// Send the same recovery acknowledgements whether the proposal was present at
// Sync or arrives later. Return local processing so Sync can defer it until all
// recovered descriptors have been installed, as in the original recovery path.
func (c *core) sendRecoveryAck(cmdID defs.RequestID, dep Dep, propose *proposal, desc *commandDesc) func() {
	if !c.sq.Contains(int(c.id)) {
		return func() {}
	}
	if c.id == c.leader() {
		ack := newRecoveryFastAck(c.id, c.ballot, cmdID, dep)
		c.batcher.SendFastAck(copyFastAck(ack))
		c.sendClient(propose.ClientId, c.codes.replyRPC, &MReply{Replica: c.id, Ballot: c.ballot, CmdId: cmdID})
		if desc != nil {
			return func() { c.handleFastAck(ack, desc) }
		}
	} else {
		ack := &MLightSlowAck{Replica: c.id, Ballot: c.ballot, CmdId: cmdID}
		c.batcher.SendLightSlowAckClient(ack)
		if desc != nil {
			return func() { c.handleLightSlowAck(ack, desc) }
		}
	}
	return func() {}
}
