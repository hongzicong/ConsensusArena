package fastpaxos

// Message sending, connection writers, and output queues.
import (
	"github.com/hongzicong/ConsensusArena/protocol"
	"github.com/hongzicong/ConsensusArena/replica"
	"github.com/hongzicong/ConsensusArena/replica/defs"
	"io"
	"time"
)

type replyJob struct {
	proposal *defs.GPropose
	reply    defs.ProposeReplyTS
}

func (r *Replica) drain() {
	sender := r.Messages()
	sender.Local = func(m interface{ Marshal(io.Writer) }) {
		protocol.Must(r.Handle(m.(*wireMessage).message, time.Time{}))
	}
	protocol.Drain(&r.engine.out, func(e envelope) bool {
		m := &wireMessage{e.Message}
		if e.To == -1 {
			sender.SendToAll(m, r.code, replica.SendPlan{IncludeSelf: true})
		} else {
			_ = sender.Send(e.To, r.code, m)
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
