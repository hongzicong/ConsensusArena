package kcensus

// Replica state, configuration, RPC registration, and startup.
import (
	"time"

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
	topology       processTopology
	core           *core
	inbox          chan fastrpc.Serializable
	code           uint8
	peers          *replica.PeerStreams
	proposals      map[defs.RequestID]*defs.GPropose
	pendingReplies map[defs.RequestID]replyJob
}

func New(alias string, replicaID int, addrs []string, conf *config.Config, logger *dlog.Logger) *Replica {
	if conf.Noop {
		panic("KCensus requires noop: false (client replies require execution)")
	}
	if conf.CommandSize > state.MaxValueBytes {
		panic("KCensus commandSize exceeds Arena wire limit")
	}
	n := len(addrs)
	start := time.Now()
	topology, err := configuredTopology(conf)
	if err != nil {
		panic(err)
	}
	plan := topology.Plan
	logger.Printf("KCENSUS_PLAN replica=%d digest=%x processes=%v delegates=%v predicted_mean=%s budgets=%v returns=%v synthesis=%s requirements=%v", replicaID, plan.Digest, topology.Aliases, plan.Leaders, plan.Mean, plan.Budgets, plan.Returns, time.Since(start), plan.Requirements)
	logger.Printf("KCENSUS_PRIORITY replica=%d order=%v", replicaID, plan.leaderPriority())
	r := &Replica{topology: topology, Replica: replica.New(alias, replicaID, n/2, addrs, true, conf, logger), core: newCore(replicaID, plan, conf.KCensusFailure, conf.KCensusShards), inbox: make(chan fastrpc.Serializable, 65536), proposals: make(map[defs.RequestID]*defs.GPropose), pendingReplies: make(map[defs.RequestID]replyJob)}
	logger.Printf("KCENSUS_SHARDS replica=%d count=%d mapping=identity-if-zero-otherwise-modulo-v1", replicaID, conf.KCensusShards)
	r.core.execute = func(cmd state.Command) state.Value { return cmd.Execute(r.State) }
	r.core.reply = r.queueReply
	r.code = r.RPC.Register(&wireMessage{}, r.inbox)
	go r.run()
	return r
}
