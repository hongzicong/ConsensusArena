package kcensus

// Serialized replica/client event loops and protocol integration.
import (
	"encoding/json"
	"runtime/metrics"
	"time"

	"github.com/hongzicong/ConsensusArena/protocol"
	"github.com/hongzicong/ConsensusArena/replica"
	"github.com/hongzicong/ConsensusArena/replica/defs"
	"github.com/hongzicong/ConsensusArena/state"
)

func (r *Replica) run() {
	r.configureTransport()
	r.ConnectToPeersConcurrent()
	defer r.CloseSenders()
	r.engine.connected = make([]bool, r.engine.m)
	for id := 0; id < r.N; id++ {
		r.engine.markConnected(id)
	}
	r.peers = replica.NewPeerStreams(r.engine.m)
	defer r.peers.Close()
	for id := 0; id < r.N; id++ {
		if id != int(r.Id) {
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
			protocol.Must(r.Handle(raw, time.Time{}))
		case p := <-r.ProposeChan:
			protocol.Must(r.Propose(p, time.Time{}))
		case now := <-ticker.C:
			t := now.Sub(start)
			protocol.Must(r.Tick(protocol.Tick{Now: now, Elapsed: t}))
			if t-lastStats >= 5*time.Second {
				metrics.Read(gcSamples)
				r.Printf("KCENSUS_MEMORY replica=%d gc_cpu_seconds=%f heap_bytes=%d gc_cycles=%d", r.Id, gcSamples[0].Value.Float64(), gcSamples[1].Value.Uint64(), gcSamples[2].Value.Uint64())
				data, _ := json.Marshal(r.engine.stats)
				r.Printf("KCENSUS_STATS replica=%d stats=%s active_slots=%d pending_reads=%d", r.Id, data, len(r.engine.active), len(r.engine.reads))
				r.Printf("KCENSUS_RECOVERY replica=%d status=%s", r.Id, r.engine.recoverySnapshot())
				r.Printf("KCENSUS_RECEIVE replica=%d pending=%d", r.Id, len(r.inbox))
				lastStats = t
			}
		}
		r.drain()
		r.flushReplies()
	}
}

// Sample one handler per 64 messages of each kind, avoiding clock reads on
// every packet. This observes service cost, never changes voting state.
func (r *Replica) receiveObserved(m message) {
	if int(m.Kind) >= len(r.engine.stats.InputMessages) {
		r.receive(m)
		return
	}
	r.engine.stats.InputMessages[m.Kind]++
	if r.engine.stats.InputMessages[m.Kind]%64 == 1 {
		before := time.Now()
		r.receive(m)
		r.engine.stats.InputSamples[m.Kind]++
		r.engine.stats.InputSampleNanos[m.Kind] += uint64(time.Since(before))
	} else {
		r.receive(m)
	}
}

func (r *Replica) receive(m message) {
	if connection := m.ClientConnection; connection != nil {
		id, err := connection.BindPeer(func() (int, error) { return r.bindClient(m) })
		if err != nil || r.engine.peerFailed(id) {
			connection.Conn.Close()
			return
		}
		m.From = id
	}
	r.engine.step(m)
}

func (c *Client) run() {
	start := time.Now()
	c.engine.connected = make([]bool, c.engine.m)
	for id := 0; id < c.engine.n; id++ {
		if c.Connection(id) != nil && !c.PeerFailed(id) {
			c.engine.markConnected(id)
		}
	}
	receive := func(m message) {
		c.engine.markConnected(m.From)
		c.engine.step(m)
	}
	lastStats := time.Duration(0)
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		for count := 0; count < 4*c.engine.m; count++ {
			select {
			case m := <-c.control:
				receive(m)
			default:
				count = 4 * c.engine.m
			}
		}
		select {
		case m := <-c.control:
			receive(m)
		case <-c.stop:
			return
		case r := <-c.proposals:
			if c.requests[r.ID] != nil || c.done[r.ID] {
				continue
			}
			delegate := c.selectedDelegate()
			q := &clientRequest{record: r, delegate: delegate, nextQuery: c.engine.now + c.engine.failure, queryDelay: c.engine.retry, queryPeer: delegate}
			c.requests[r.ID] = q
			c.queryQueue = append(c.queryQueue, r.ID)
			if r.Command.Op == state.PUT {
				if err := c.engine.submit(r); err != nil {
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
			c.peers.Failed().Range(func(id int) bool { c.engine.markFailed(id); return true })
			for id := 0; id < c.engine.n; id++ {
				if c.PeerFailed(id) {
					c.engine.markFailed(id)
				}
			}
			c.engine.tick(now.Sub(start))
			if c.engine.now-lastStats >= 5*time.Second {
				data, _ := json.Marshal(c.engine.stats)
				c.Printf("KCENSUS_CLIENT_STATS pid=%d pending=%d completed=%d write_replies=%d write_reply_ns=%d graph_messages=%d payload_waits=%d graph_usable=%t stats=%s", c.engine.id, len(c.requests), len(c.done), c.engine.stats.LocalWrites, c.engine.stats.LocalWriteNanos, c.engine.stats.GraphMessages, c.engine.stats.PayloadWaits, c.engine.graphUsable(c.engine.id), data)
				lastStats = c.engine.now
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
	if err := r.engine.submit(Record{id, p.Command}); err != nil {
		r.Printf("KCENSUS_REJECT replica=%d client=%d request=%d error=%q", r.Id, id.Client, id.Sequence, err)
		r.pendingReplies[id] = replyJob{p, defs.ProposeReplyTS{OK: defs.FALSE, CommandId: id.Sequence, Timestamp: p.Timestamp}}
	}
	return nil
}

func (r *Replica) Handle(event any, _ time.Time) error {
	switch m := event.(type) {
	case localMessage:
		r.engine.step(m.message)
	case message:
		r.receiveObserved(m)
	case *wireMessage:
		if m == nil {
			return protocol.UnsupportedEvent("kcensus", event)
		}
		r.receiveObserved(m.message)
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
		if id != int(r.Id) && !r.Alive[id] {
			r.engine.markFailed(id)
			r.peers.ClosePeer(id)
		}
	}
	r.M.Unlock()
	r.peers.Failed().Range(func(id int) bool { r.engine.markFailed(id); return true })
	tickStart := time.Now()
	r.engine.tick(t.Elapsed)
	elapsed := uint64(time.Since(tickStart))
	r.engine.stats.TickNanos += elapsed
	if elapsed > r.engine.stats.TickMaxNanos {
		r.engine.stats.TickMaxNanos = elapsed
	}
	return nil
}
