package fastpaxos

import (
	"time"

	"github.com/hongzicong/ConsensusArena/config"
	"github.com/hongzicong/ConsensusArena/dlog"
	"github.com/hongzicong/ConsensusArena/protocol"
	"github.com/hongzicong/ConsensusArena/replica"
	"github.com/hongzicong/ConsensusArena/replica/defs"
	fastrpc "github.com/hongzicong/ConsensusArena/rpc"
	"github.com/hongzicong/ConsensusArena/state"
)

// Replica owns runtime resources and one protocol core.
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

func (r *Replica) run() {
	r.ClientReplyCapacity = 8192
	r.ConnectToPeers()
	defer r.CloseSenders()
	go r.WaitForClientConnections()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	start := time.Now()
	lastStats := time.Duration(0)
	for !r.Shutdown {
		select {
		case p := <-r.ProposeChan:
			protocol.Must(r.Propose(p, time.Time{}))
		case msg := <-r.inbox:
			protocol.Must(r.Step(msg, time.Time{}))
		case now := <-ticker.C:
			t := now.Sub(start)
			protocol.Must(r.Tick(protocol.Tick{Now: now, Elapsed: t}))
			r.flushReplies()
			if t-lastStats >= 5*time.Second {
				e := r.core
				r.Printf("FASTPAXOS_PROGRESS replica=%d epoch=%d coordinator=%d preparing=%t active=%t fast_quorum=%d fast_voters=%d high=%d executed=%d pending=%d fast=%d classic=%d elections=%d repaired=%d window_fallbacks=%d age_fallbacks=%d", r.ID, e.promise, e.owner(), e.preparing, e.active, e.fastSize, e.n, e.high, e.executed, len(e.pending), e.fastCommits, e.classicCommits, e.elections, e.repaired, e.windowFallbacks, e.ageFallbacks)
				lastStats = t
			}
		}
		r.drain()
	}
}

var _ protocol.Machine = (*Replica)(nil)

func (r *Replica) Propose(p *defs.GPropose, _ time.Time) error {
	if err := protocol.ValidateProposal(p); err != nil {
		return err
	}
	id := p.RequestID()
	r.proposals[id] = p
	r.core.submit(record{ID: id, Command: p.Command})
	return nil
}

func (r *Replica) Step(event any, _ time.Time) error {
	switch m := event.(type) {
	case *wireMessage:
		if m == nil {
			return protocol.UnsupportedEvent("fastpaxos", event)
		}
		r.core.receive(m.message)
	case message: // Locally delivered output, without an encode/decode round trip.
		r.core.receive(m)
	default:
		return protocol.UnsupportedEvent("fastpaxos", event)
	}
	return nil
}

func (r *Replica) Tick(t protocol.Tick) error {
	if t.Kind != protocol.Maintenance {
		return protocol.UnsupportedTimer("fastpaxos", t.Kind)
	}
	r.core.tick(t.Elapsed)
	return nil
}
