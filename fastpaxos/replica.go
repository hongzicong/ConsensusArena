package fastpaxos

// Replica state, configuration, RPC registration, and startup.
import (
	"github.com/hongzicong/ConsensusArena/config"
	"github.com/hongzicong/ConsensusArena/dlog"
	"github.com/hongzicong/ConsensusArena/replica"
	"github.com/hongzicong/ConsensusArena/replica/defs"
	fastrpc "github.com/hongzicong/ConsensusArena/rpc"
	"github.com/hongzicong/ConsensusArena/state"
)

// Replica owns runtime resources and exposes the external protocol event boundary.
type Replica struct {
	*replica.Replica
	core           *core
	inbox          chan fastrpc.Serializable
	code           uint8
	proposals      map[defs.RequestID]*defs.GPropose
	pendingReplies map[defs.RequestID]replyJob
}

func New(alias string, replicaID int, addrs []string, executeEnabled bool, f int, conf *config.Config, logger *dlog.Logger) *Replica {
	r := &Replica{Replica: replica.New(alias, replicaID, f, addrs, executeEnabled, conf, logger), inbox: make(chan fastrpc.Serializable, 65536), proposals: make(map[defs.RequestID]*defs.GPropose), pendingReplies: make(map[defs.RequestID]replyJob)}
	if len(conf.Plan.FastQuorum) != 0 {
		panic("Fast Paxos no longer accepts a fixed fast-voter set")
	}
	if size := conf.Plan.FastQuorumSize; size != 0 && size != fastQuorumSize(len(addrs)) {
		panic("Fast Paxos needs an unrestricted fast quorum larger than 3n/4")
	}
	r.core = newCore(replicaID, len(addrs))
	r.core.execute = func(cmd state.Command) state.Value {
		if !r.ExecuteEnabled {
			return state.NIL()
		}
		return cmd.Execute(r.State)
	}
	r.core.reply = r.queueReply
	r.code = r.RPC.Register(&wireMessage{}, r.inbox)
	go r.run()
	return r
}
