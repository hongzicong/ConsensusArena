package fastpaxos

// Message sending, connection writers, and output queues.
import (
	"github.com/hongzicong/ConsensusArena/protocol"
	"github.com/hongzicong/ConsensusArena/replica"
	"github.com/hongzicong/ConsensusArena/replica/defs"
	"time"
)

type replyJob struct {
	proposal *defs.GPropose
	reply    defs.ProposeReplyTS
}

func (r *Replica) drain() {
	protocol.Drain(&r.engine.out, func(e envelope) bool {
		if e.To == int(r.Id) {
			protocol.Must(r.Handle(e.Message, time.Time{}))
		} else {
			_ = r.peerQueues[e.To].Enqueue(replica.Encode(r.code, &wireMessage{e.Message}, true))
			// Retransmission, heartbeat catch-up and stalled-fast recovery repair drops.
		}
		return true
	})
}

// Full queues retain replies in the protocol's pending map for retry.
func (r *Replica) flushReplies() {
	for id, job := range r.pendingReplies {
		if r.ReplyProposal(job.proposal, &job.reply, 8192) == nil {
			delete(r.pendingReplies, id)
		}
	}
}
