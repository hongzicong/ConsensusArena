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

type Replica struct {
	*replica.Replica
	topology       processTopology
	engine         *core
	inbox          chan fastrpc.Serializable
	code           uint8
	peers          []replica.PendingSender
	proposals      map[CommandID]*defs.GPropose
	pendingReplies map[CommandID]replyJob
}

func New(alias string, rid int, addrs []string, conf *config.Config, l *dlog.Logger) *Replica {
	if conf.Noop {
		panic("KCensus requires noop: false (client replies require execution)")
	}
	if conf.CommandSize > 65535 {
		panic("KCensus commandSize exceeds Arena wire limit")
	}
	if len(conf.ReplicaAliases) != len(addrs) || conf.ReplicaAliases[rid] != alias {
		panic("KCensus replica IDs must match configuration order")
	}
	n := len(addrs)
	start := time.Now()
	topology, err := configuredTopology(conf)
	if err != nil {
		panic(err)
	}
	plan := topology.Plan
	l.Printf("KCENSUS_PLAN replica=%d digest=%x processes=%v delegates=%v predicted_mean=%s budgets=%v returns=%v synthesis=%s requirements=%v", rid, plan.Digest, topology.Aliases, plan.Leaders, plan.Mean, plan.Budgets, plan.Returns, time.Since(start), plan.Requirements)
	l.Printf("KCENSUS_PRIORITY replica=%d order=%v", rid, plan.leaderPriority())
	r := &Replica{topology: topology, Replica: replica.New(alias, rid, n/2, addrs, false, true, false, conf, l), engine: newCore(rid, plan, conf.KCensusFailure), inbox: make(chan fastrpc.Serializable, 65536), proposals: make(map[CommandID]*defs.GPropose), pendingReplies: make(map[CommandID]replyJob)}
	r.engine.execute = func(v Record) state.Value { return v.Command.Execute(r.State) }
	r.engine.complete = func(id CommandID, v state.Value) {
		if p := r.proposals[id]; p != nil {
			r.pendingReplies[id] = replyJob{p, defs.ProposeReplyTS{OK: defs.TRUE, CommandId: id.Seq, Value: v, Timestamp: p.Timestamp}}
		} else if int(id.Client) >= r.engine.n && int(id.Client) < r.engine.m {
			record, ok := r.engine.known[id]
			delegate := plan.Leaders[int(id.Client)]
			recoveryReply := r.engine.peerFailed(delegate) && r.engine.coordinator() == rid
			if ok && (record.Command.Op == state.GET || delegate == rid || recoveryReply) {
				if recoveryReply && record.Command.Op == state.PUT {
					r.engine.stats.RecoveryResultReplies++
				}
				r.engine.send(int(id.Client), message{Kind: executedResult, Key: record.Command.K, ID: id, Result: v})
			}
		}
	}
	r.code = r.RPC.Register(&wireMessage{}, r.inbox)
	go r.run()
	return r
}
