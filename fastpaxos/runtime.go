package fastpaxos

// Event scheduling, protocol integration, and control/status handling.
import (
	"time"

	"github.com/hongzicong/ConsensusArena/protocol"
	"github.com/hongzicong/ConsensusArena/replica/defs"
)

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
			protocol.Must(r.Handle(msg, time.Time{}))
		case now := <-ticker.C:
			t := now.Sub(start)
			protocol.Must(r.Tick(protocol.Tick{Now: now, Elapsed: t}))
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

var _ protocol.Machine = (*Replica)(nil)

func (r *Replica) Propose(p *defs.GPropose, _ time.Time) error {
	if err := protocol.ValidateProposal(p); err != nil {
		return err
	}
	id := p.RequestID()
	r.proposals[id] = p
	r.engine.submit(record{ID: id, Command: p.Command})
	return nil
}

func (r *Replica) Handle(event any, _ time.Time) error {
	switch m := event.(type) {
	case *wireMessage:
		if m == nil {
			return protocol.UnsupportedEvent("fastpaxos", event)
		}
		r.engine.step(m.message)
	case message: // Locally delivered output, without an encode/decode round trip.
		r.engine.step(m)
	default:
		return protocol.UnsupportedEvent("fastpaxos", event)
	}
	return nil
}

func (r *Replica) Tick(t protocol.Tick) error {
	if t.Kind != protocol.Maintenance {
		return protocol.UnsupportedTimer("fastpaxos", t.Kind)
	}
	r.engine.tick(t.Elapsed)
	return nil
}
