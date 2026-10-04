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

func (r *Replica) recoverBroadcast(phase string) {
	if err := recover(); err != nil {
		r.Println(phase+" bcast failed:", err)
	}
}

func (r *Replica) bcastPrepare(replica int32, instance int32) {
	defer r.recoverBroadcast("Prepare")
	lb := r.InstanceSpace[replica][instance].lb
	msg := &Prepare{LeaderId: r.Id, Replica: replica, Instance: instance, Ballot: lb.lastTriedBallot}
	r.broadcast(r.cyclicPeers, r.N-1, r.prepareRPC, msg)
}

func (r *Replica) bcastPreAccept(replica int32, instance int32) {
	defer r.recoverBroadcast("PreAccept")
	lb := r.InstanceSpace[replica][instance].lb
	msg := &PreAccept{
		LeaderId: r.Id, Replica: replica, Instance: instance,
		Ballot: lb.lastTriedBallot, Command: lb.cmds, Seq: lb.seq, Deps: lb.deps,
	}
	limit := r.N - 1
	if r.Thrifty {
		limit = r.Replica.FastQuorumSize() - 1
	}
	r.broadcast(r.PreferredPeerOrder[:r.N-1], limit, r.preAcceptRPC, msg)
}

func (r *Replica) bcastTryPreAccept(replica int32, instance int32) {
	defer r.recoverBroadcast("PreAccept")
	lb := r.InstanceSpace[replica][instance].lb
	msg := &TryPreAccept{
		LeaderId: r.Id, Replica: replica, Instance: instance,
		Ballot: lb.lastTriedBallot, Command: lb.cmds, Seq: lb.seq, Deps: lb.deps,
	}
	r.broadcast(nil, r.N-1, r.tryPreAcceptRPC, msg)
}

func (r *Replica) bcastAccept(replica int32, instance int32) {
	defer r.recoverBroadcast("Accept")
	lb := r.InstanceSpace[replica][instance].lb
	msg := &Accept{
		LeaderId: r.Id, Replica: replica, Instance: instance,
		Ballot: lb.lastTriedBallot, Seq: lb.seq, Deps: lb.deps, Command: lb.cmds,
	}
	limit := r.N - 1
	if r.Thrifty {
		limit = r.N / 2
	}
	r.broadcast(r.PreferredPeerOrder[:r.N-1], limit, r.acceptRPC, msg)
}

func (r *Replica) bcastCommit(replica int32, instance int32) {
	defer r.recoverBroadcast("Commit")
	lb := r.InstanceSpace[replica][instance].lb
	msg := &Commit{
		LeaderId: r.Id, Replica: replica, Instance: instance,
		Ballot: lb.ballot, Command: lb.cmds, Seq: lb.seq, Deps: lb.deps,
	}
	r.broadcast(r.PreferredPeerOrder[:r.N-1], r.N-1, r.commitRPC, msg)
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
