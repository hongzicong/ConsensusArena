package fastpaxos

import (
	"github.com/hongzicong/ConsensusArena/config"
	"github.com/hongzicong/ConsensusArena/dlog"
	"github.com/hongzicong/ConsensusArena/replica"
	"github.com/hongzicong/ConsensusArena/replica/defs"
	fastrpc "github.com/hongzicong/ConsensusArena/rpc"
	"github.com/hongzicong/ConsensusArena/state"
	"time"
)

type Replica struct {
	*replica.Replica
	engine         *core
	inbox          chan fastrpc.Serializable
	code           uint8
	proposals      map[CommandId]*defs.GPropose
	peerQueues     []chan message
	replyQueues    map[int32]chan replyJob
	pendingReplies map[CommandId]replyJob
}
type replyJob struct {
	proposal *defs.GPropose
	reply    defs.ProposeReplyTS
}

func New(alias string, rid int, addrs []string, exec bool, f int, conf *config.Config, l *dlog.Logger) *Replica {
	r := &Replica{Replica: replica.New(alias, rid, f, addrs, false, exec, false, conf, l), inbox: make(chan fastrpc.Serializable, 65536), proposals: make(map[CommandId]*defs.GPropose), replyQueues: make(map[int32]chan replyJob), pendingReplies: make(map[CommandId]replyJob)}
	qs, _, err := replica.NewQuorumsFromFile(conf.Quorum, r.Replica)
	mask := uint64(1)<<len(addrs) - 1
	size := 3*len(addrs)/4 + 1
	fixed := false
	if err == nil && len(qs) > 0 {
		mask = 0
		for id := range qs[0] {
			mask |= 1 << id
		}
		size = len(qs[0])
		fixed = true
	} else if err != replica.NO_QUORUM_FILE && err != replica.THREE_QUARTERS {
		panic(err)
	}
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
func (r *Replica) run() {
	r.ConnectToPeers()
	r.peerQueues = make([]chan message, r.N)
	for i := 0; i < r.N; i++ {
		if i == int(r.Id) {
			continue
		}
		r.peerQueues[i] = make(chan message, 16384)
		go r.sendPeer(i)
	}
	go r.WaitForClientConnections()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	start := time.Now()
	lastStats := time.Duration(0)
	for !r.Shutdown {
		select {
		case p := <-r.ProposeChan:
			id := CommandId{p.ClientId, p.CommandId}
			r.proposals[id] = p
			r.ensureReplyQueue(p.ClientId)
			r.engine.submit(record{ID: id, Command: p.Command})
		case msg := <-r.inbox:
			r.engine.step(msg.(*wireMessage).message)
		case now := <-ticker.C:
			t := now.Sub(start)
			r.engine.tick(t)
			r.flushReplies()
			if t-lastStats >= 5*time.Second {
				e := r.engine
				r.Printf("FASTPAXOS_PROGRESS replica=%d epoch=%d coordinator=%d preparing=%t active=%t high=%d executed=%d pending=%d fast=%d classic=%d elections=%d repaired=%d window_fallbacks=%d age_fallbacks=%d", r.Id, e.promise, e.owner(), e.preparing, e.active, e.high, e.executed, len(e.pending), e.fastCommits, e.classicCommits, e.elections, e.repaired, e.windowFallbacks, e.ageFallbacks)
				lastStats = t
			}
		}
		r.drain()
	}
}
func (r *Replica) drain() {
	for len(r.engine.out) > 0 {
		out := r.engine.out
		r.engine.out = nil
		for _, e := range out {
			if e.To == int(r.Id) {
				r.engine.step(e.Message)
				continue
			}
			select {
			case r.peerQueues[e.To] <- e.Message:
			default:
			}
			// Retransmission, heartbeat catch-up and stalled-fast recovery repair drops.
		}
	}
}
func (r *Replica) sendPeer(id int) {
	for m := range r.peerQueues[id] {
		conn := r.Peers[id]
		w := r.PeerWriters[id]
		if conn == nil || w == nil {
			continue
		}
		// This peer has a dedicated worker. Backpressure may block it without
		// blocking the protocol or other peers. A short write deadline would
		// permanently sever a healthy WAN connection during suffix transfer.
		if err := w.WriteByte(r.code); err != nil {
			return
		}
		(&wireMessage{m}).Marshal(w)
		if err := w.Flush(); err != nil {
			_ = conn.Close()
			return
		}
	}
}

// A full per-client queue retains replies for retry instead of losing completion.
func (r *Replica) ensureReplyQueue(id int32) {
	if r.replyQueues[id] != nil {
		return
	}
	q := make(chan replyJob, 8192)
	r.replyQueues[id] = q
	go func() {
		for j := range q {
			j.proposal.Mutex.Lock()
			j.reply.Marshal(j.proposal.Reply)
			j.proposal.Reply.Flush()
			j.proposal.Mutex.Unlock()
		}
	}()
}
func (r *Replica) flushReplies() {
	for id, job := range r.pendingReplies {
		select {
		case r.replyQueues[id.ClientId] <- job:
			delete(r.pendingReplies, id)
		default:
		}
	}
}
