package kcensus

// Serialized replica/client event loops and protocol integration.
import (
	"encoding/json"
	"runtime/metrics"
	"time"

	"github.com/hongzicong/ConsensusArena/protocol"
	"github.com/hongzicong/ConsensusArena/replica"
	"github.com/hongzicong/ConsensusArena/replica/defs"
	fastrpc "github.com/hongzicong/ConsensusArena/rpc"
	"github.com/hongzicong/ConsensusArena/state"
)

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

func (c *Client) run() {
	start := time.Now()
	c.core.connected = make([]bool, c.core.m)
	for id := 0; id < c.core.n; id++ {
		if c.Connection(id) != nil && !c.PeerFailed(id) {
			c.core.markConnected(id)
		}
	}
	receive := func(m message) {
		c.core.markConnected(m.From)
		c.core.receive(m)
	}
	lastStats := time.Duration(0)
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		for count := 0; count < 4*c.core.m; count++ {
			select {
			case m := <-c.control:
				receive(m)
			default:
				count = 4 * c.core.m
			}
		}
		select {
		case m := <-c.control:
			receive(m)
		case <-c.stop:
			return
		case r := <-c.proposals:
			if c.requests[r.ID] != nil || c.ReplyCompleted(r.ID) {
				continue
			}
			delegate := c.selectedDelegate()
			q := &clientRequest{record: r, delegate: delegate, nextQuery: c.core.now + c.core.failure, queryDelay: c.core.retry, queryPeer: delegate}
			c.requests[r.ID] = q
			c.queryQueue = append(c.queryQueue, r.ID)
			if r.Command.Op == state.PUT {
				if err := c.core.submit(r); err != nil {
					panic(err)
				}
			} else if r.Command.Op == state.GET {
				c.sendRead(q)
			}
		case m := <-c.inbox:
			receive(m)
		case m := <-c.resultInbox:
			c.receiveResult(m)
		case now := <-ticker.C:
			c.peers.Failed().Range(func(id int) bool { c.core.markFailed(id); return true })
			for id := 0; id < c.core.n; id++ {
				if c.PeerFailed(id) {
					c.core.markFailed(id)
				}
			}
			c.core.tick(now.Sub(start))
			if c.core.now-lastStats >= 5*time.Second {
				data, _ := json.Marshal(c.core.stats)
				c.Printf("KCENSUS_CLIENT_STATS pid=%d pending=%d completed=%d write_replies=%d write_reply_ns=%d graph_messages=%d payload_waits=%d graph_usable=%t stats=%s", c.core.id, len(c.requests), c.CompletedReplies(), c.core.stats.LocalWrites, c.core.stats.LocalWriteNanos, c.core.stats.GraphMessages, c.core.stats.PayloadWaits, c.core.graphUsable(c.core.id), data)
				lastStats = c.core.now
			}
			c.queryResults()
		}
		// Drain a bounded group of already-arrived evidence before generating
		// another timer's repairs. All protocol transitions remain serialized.
		for count := 0; count < 64; count++ {
			select {
			case m := <-c.inbox:
				receive(m)
			default:
				count = 64
			}
		}
		for count := 0; count < 32; count++ {
			select {
			case m := <-c.resultInbox:
				c.receiveResult(m)
			default:
				count = 32
			}
		}
		c.drain()
	}
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
