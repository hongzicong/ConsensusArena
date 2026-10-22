package paxos

// Replica state, configuration, RPC registration, and startup.
import (
	"github.com/hongzicong/ConsensusArena/config"
	"github.com/hongzicong/ConsensusArena/dlog"
	"github.com/hongzicong/ConsensusArena/replica"
)

// Replica runs classic Multi-Paxos: acceptors reply to the leader, which learns
// a majority and disseminates Commit. All transitions belong to one event loop.
// Replica owns runtime resources and exposes the external protocol event boundary.
type Replica struct {
	*replica.Replica
	runtime *runtime
}

func New(alias string, replicaID int, addrs []string, f int, conf *config.Config, logger *dlog.Logger) *Replica {
	r := &Replica{Replica: replica.New(alias, replicaID, f, addrs, true, conf, logger)}
	leader := conf.Plan.LeaderID()
	r.runtime = newRuntime(r.Replica, leader)

	go r.runtime.Run()
	return r
}

// Register must run before Run and in the same order at every replica.
