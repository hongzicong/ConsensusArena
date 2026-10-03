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

type Replica struct {
	*replica.Replica
	engine         *core
	inbox          chan fastrpc.Serializable
	code           uint8
	proposals      map[CommandId]*defs.GPropose
	peerQueues     []*replica.Sender
	pendingReplies map[CommandId]replyJob
}

func New(alias string, rid int, addrs []string, exec bool, f int, conf *config.Config, l *dlog.Logger) *Replica {
	r := &Replica{Replica: replica.New(alias, rid, f, addrs, false, exec, false, conf, l), inbox: make(chan fastrpc.Serializable, 65536), proposals: make(map[CommandId]*defs.GPropose), pendingReplies: make(map[CommandId]replyJob)}
	mask := uint64(0)
	for _, member := range conf.Plan.FastQuorum {
		mask |= 1 << member.Rank
	}
	size := len(conf.Plan.FastQuorum)
	if size != len(addrs)/2+1 {
		panic("Fast Paxos needs its planned fixed majority")
	}
	fixed := true
	r.engine = newCore(rid, len(addrs), mask, size, fixed)
	r.engine.execute = func(v record) state.Value {
		if !r.Exec {
			return state.NIL()
		}
		return v.Command.Execute(r.State)
	}
	r.engine.complete = func(id CommandId, result state.Value) {
		if p := r.proposals[id]; p != nil {
			job := replyJob{p, defs.ProposeReplyTS{OK: defs.TRUE, CommandId: id.SeqNum, Value: result, Timestamp: p.Timestamp}}
			r.pendingReplies[id] = job
		}
	}
	r.code = r.RPC.Register(&wireMessage{}, r.inbox)
	go r.run()
	return r
}
