package paxos

// Replica state, configuration, RPC registration, and startup.
import (
	"github.com/hongzicong/ConsensusArena/config"
	"github.com/hongzicong/ConsensusArena/dlog"
	"github.com/hongzicong/ConsensusArena/replica"
	fastrpc "github.com/hongzicong/ConsensusArena/rpc"
)

// Replica runs classic Multi-Paxos: acceptors reply to the leader, which learns
// a majority and disseminates Commit. All transitions belong to one event loop.
type Replica struct {
	*replica.Replica
	recovery *protocolRuntime
}

func New(alias string, id int, addrs []string, f int, conf *config.Config, logger *dlog.Logger) *Replica {
	r := &Replica{Replica: replica.New(alias, id, f, addrs, false, true, false, conf, logger)}
	leader := conf.Plan.LeaderID()
	r.recovery = newProtocolRuntime(r.Replica, leader)

	go r.recovery.Run()
	return r
}

// Register must run before Run and in the same order at every replica.
func (r *transportRuntime) Register(message fastrpc.Serializable) uint8 {
	return r.Base.RPC.Register(message, r.in)
}
