package swift

import (
	"log"

	"github.com/hongzicong/ConsensusArena/replica/defs"
)

// A recovery confirmation is not an ordinary hash-log update. Keep this
// explicit across both MAcks and MOptAcks without changing the wire layout.
const recoveryAckSeqnum = -1

func newRecoveryFastAck(replica, ballot int32, cmdId CommandId, dep Dep) *MFastAck {
	msg := newFastAck()
	msg.Replica, msg.Ballot, msg.CmdId = replica, ballot, cmdId
	msg.Dep = dep
	if msg.Dep == nil {
		msg.Dep = NilDepOfCmdId(cmdId)
	}
	msg.Checksum = nil
	msg.Seqnum = recoveryAckSeqnum
	return msg
}

func (r *Replica) installRecoveryHashBoundary(phases map[CommandId]int) {
	log.Printf("swift_recovery_hash_boundary ballot=%d installed_commands=%d discarded_pending_updates=%d", r.ballot, len(phases), len(r.pendingHashUpds))
	log.Printf("swift_proposal_batches batches=%d commands=%d max=%d", r.proposalBatches, r.proposalBatchCommands, r.proposalBatchMax)
	log.Printf("swift_deferred_hash_elisions commands=%d", r.deferredHashElisions)
	log.Printf("swift_hash_backlog_deferrals commands=%d", r.hashBacklogDeferrals)
	r.recoveryCmds = phases
	r.proposedInBallot = make(map[CommandId]struct{})
	// These entries were received in the previous ballot. Their leader/sequence
	// context must not be applied when a delayed proposal arrives after Sync.
	r.pendingHashUpds = make(map[CommandId]*UpdateEntry)
	removed := 0
	for _, log := range r.hlog {
		removed += log.BeginBallot(r.ballot)
	}
	log.Printf("swift_hash_epoch ballot=%d keys=%d sealed_pending_nodes=%d", r.ballot, len(r.hlog), removed)
}

func (r *Replica) recordLeaderAck(msg *MFastAck) {
	if msg.Ballot != r.ballot || msg.Replica != r.leader() || r.Id == r.leader() {
		return
	}
	if msg.Seqnum == recoveryAckSeqnum {
		_, installed := r.recoveryCmds[msg.CmdId]
		if !installed || len(msg.Checksum) != 0 {
			r.Fatal("invalid recovery hash acknowledgement", msg.CmdId, msg.Ballot)
			return
		}
		// Keep dependency/quorum processing, but do not manufacture hash evidence.
		return
	}
	if msg.Seqnum < 0 {
		r.Fatal("invalid leader hash sequence", msg.CmdId, msg.Seqnum)
		return
	}
	r.recordLeaderHash(msg.CmdId, msg.Seqnum, msg.Checksum)
}

func (r *Replica) receiveFastAck(msg *MFastAck) {
	if r.status != NORMAL || msg.Ballot != r.ballot {
		return
	}
	r.recordLeaderAck(msg)
	r.getCmdDescSeq(msg.CmdId, msg, nil, nil, r.leader() == r.Id)
}

// Send the same recovery acknowledgements whether the proposal was present at
// Sync or arrives later. Return local processing so Sync can defer it until all
// recovered descriptors have been installed, as in the original recovery path.
func (r *Replica) sendRecoveryAck(cmdId CommandId, dep Dep, propose *defs.GPropose, desc *commandDesc) func() {
	if !r.SQ.Contains(r.Id) {
		return func() {}
	}
	if r.Id == r.leader() {
		ack := newRecoveryFastAck(r.Id, r.ballot, cmdId, dep)
		r.batcher.SendFastAck(copyFastAck(ack))
		r.SendClientMsg(propose.ClientId, r.cs.replyRPC, &MReply{Replica: r.Id, Ballot: r.ballot, CmdId: cmdId})
		if desc != nil {
			return func() { r.handleFastAck(ack, desc) }
		}
	} else {
		ack := &MLightSlowAck{Replica: r.Id, Ballot: r.ballot, CmdId: cmdId}
		r.batcher.SendLightSlowAckClient(ack, propose.ClientId)
		if desc != nil {
			return func() { r.handleLightSlowAck(ack, desc) }
		}
	}
	return func() {}
}
