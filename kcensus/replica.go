package kcensus

import (
	"encoding/json"
	"runtime/metrics"
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
	r := &Replica{topology: topology, Replica: replica.New(alias, replicaID, n/2, addrs, true, conf, logger), core: newCore(replicaID, plan, conf.KCensusFailure, conf.KCensusShards, conf.KeyCount), inbox: make(chan fastrpc.Serializable, 65536), proposals: make(map[defs.RequestID]*defs.GPropose), pendingReplies: make(map[defs.RequestID]replyJob)}
	logger.Printf("KCENSUS_SHARDS replica=%d count=%d mapping=identity-if-zero-otherwise-modulo-v1", replicaID, conf.KCensusShards)
	r.core.execute = func(cmd state.Command) state.Value { return cmd.Execute(r.State) }
	r.core.reply = r.queueReply
	r.code = r.RPC.Register(&wireMessage{}, r.inbox)
	go r.run()
	return r
}

func (r *Replica) run() {
	r.configureTransport()
	r.ConnectToPeersConcurrent()
	defer r.CloseSenders()
	r.core.connected = make([]bool, r.core.m)
	for id := 0; id < r.N; id++ {
		r.core.markConnected(id)
	}
	r.peers = replica.NewPeerStreams(r.core.m)
	defer r.peers.Close()
	for id := 0; id < r.N; id++ {
		if id != int(r.ID) {
			_ = r.peers.Bind(id, func() *replica.Sender { return r.PeerSenders[id] })
		}
	}
	go r.WaitForClientConnections()
	start := time.Now()
	lastStats := time.Duration(0)
	gcSamples := []metrics.Sample{{Name: "/cpu/classes/gc/total:cpu-seconds"}, {Name: "/memory/classes/heap/objects:bytes"}, {Name: "/gc/cycles/total:gc-cycles"}}
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for !r.Shutdown {
		select {
		case raw := <-r.inbox:
			protocol.Must(r.Step(raw, time.Time{}))
		case p := <-r.ProposeChan:
			protocol.Must(r.Propose(p, time.Time{}))
		case now := <-ticker.C:
			t := now.Sub(start)
			protocol.Must(r.Tick(protocol.Tick{Now: now, Elapsed: t}))
			if t-lastStats >= 5*time.Second {
				metrics.Read(gcSamples)
				r.Printf("KCENSUS_MEMORY replica=%d gc_cpu_seconds=%f heap_bytes=%d gc_cycles=%d", r.ID, gcSamples[0].Value.Float64(), gcSamples[1].Value.Uint64(), gcSamples[2].Value.Uint64())
				data, _ := json.Marshal(r.core.stats)
				r.Printf("KCENSUS_STATS replica=%d stats=%s active_slots=%d pending_reads=%d", r.ID, data, len(r.core.active), len(r.core.reads))
				r.Printf("KCENSUS_RECOVERY replica=%d status=%s", r.ID, r.core.recoverySnapshot())
				r.Printf("KCENSUS_RECEIVE replica=%d pending=%d", r.ID, len(r.inbox))
				lifecycle, _ := json.Marshal(r.core.lifecycleSnapshot())
				r.Printf("KCENSUS_LIFECYCLE replica=%d state=%s", r.ID, lifecycle)
				lastStats = t
			}
		}
		r.drain()
		r.flushReplies()
	}
}

// Sample one handler per 64 messages of each kind, avoiding clock reads on
// every packet. This observes service cost, never changes voting state.
func (r *Replica) receiveObserved(m message, connection *fastrpc.ClientConnection) {
	if int(m.Kind) >= len(r.core.stats.InputMessages) {
		r.receive(m, connection)
		return
	}
	r.core.stats.InputMessages[m.Kind]++
	if r.core.stats.InputMessages[m.Kind]%64 == 1 {
		before := time.Now()
		r.receive(m, connection)
		r.core.stats.InputSamples[m.Kind]++
		r.core.stats.InputSampleNanos[m.Kind] += uint64(time.Since(before))
	} else {
		r.receive(m, connection)
	}
}

func (r *Replica) receive(m message, connection *fastrpc.ClientConnection) {
	if connection != nil {
		id, err := connection.BindPeer(func() (int, error) { return r.bindClient(m, connection) })
		if err != nil || r.core.peerFailed(id) {
			connection.Conn.Close()
			return
		}
		m.From = id
	}
	r.core.receive(m)
}

var _ protocol.Machine = (*Replica)(nil)

// A self-delivery keeps the original unsampled local transition.
type localMessage struct{ message }

func (r *Replica) Propose(p *defs.GPropose, _ time.Time) error {
	if err := protocol.ValidateProposal(p); err != nil {
		return err
	}
	id := p.RequestID()
	r.proposals[id] = p
	if err := r.core.submit(Record{id, p.Command}); err != nil {
		r.Printf("KCENSUS_REJECT replica=%d client=%d request=%d error=%q", r.ID, id.Client, id.Sequence, err)
		r.pendingReplies[id] = replyJob{p, defs.ProposeReplyTS{OK: defs.FALSE, CommandId: id.Sequence, Timestamp: p.Timestamp}}
	}
	return nil
}

func (r *Replica) Step(event any, _ time.Time) error {
	switch m := event.(type) {
	case localMessage:
		r.core.receive(m.message)
	case message:
		r.receiveObserved(m, nil)
	case *wireMessage:
		if m == nil {
			return protocol.UnsupportedEvent("kcensus", event)
		}
		r.receiveObserved(m.message, m.connection)
	default:
		return protocol.UnsupportedEvent("kcensus", event)
	}
	return nil
}

func (r *Replica) Tick(t protocol.Tick) error {
	if t.Kind != protocol.Maintenance {
		return protocol.UnsupportedTimer("kcensus", t.Kind)
	}
	r.M.Lock()
	for id := 0; id < r.N; id++ {
		if id != int(r.ID) && !r.Alive[id] {
			r.core.markFailed(id)
			r.peers.ClosePeer(id)
		}
	}
	r.M.Unlock()
	r.peers.Failed().Range(func(id int) bool { r.core.markFailed(id); return true })
	tickStart := time.Now()
	r.core.tick(t.Elapsed)
	elapsed := uint64(time.Since(tickStart))
	r.core.stats.TickNanos += elapsed
	if elapsed > r.core.stats.TickMaxNanos {
		r.core.stats.TickMaxNanos = elapsed
	}
	return nil
}
