package epaxos

// Protocol broadcast policy and shared Sender integration.
import (
	"github.com/hongzicong/ConsensusArena/replica"

	fastrpc "github.com/hongzicong/ConsensusArena/rpc"
)

// Each phase supplies its own recipient order and attempt limit. Failed queue
// admission still consumes an attempt, just as the original loops did.
func (r *Replica) broadcast(order []int32, limit int, code uint8, msg fastrpc.Serializable) {
	r.Stats.M["sendQueueDrops"] += r.Replica.SendToAll(msg, code, replica.SendPlan{Order: order, Limit: limit})
}

func (r *Replica) Send(id int32, code uint8, msg fastrpc.Serializable) {
	if r.Replica.Send(id, code, msg) != nil {
		r.Stats.M["sendQueueDrops"]++
	}
}

type transportState struct {
	cyclicPeers []int32
}

func newTransportState(id, n int) transportState {
	t := transportState{cyclicPeers: make([]int32, 0, n-1)}
	for offset := 1; offset < n; offset++ {
		t.cyclicPeers = append(t.cyclicPeers, int32((id+offset)%n))
	}
	return t
}
