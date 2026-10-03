package bodega

// Event scheduling, protocol integration, and control/status handling.
import (
	"encoding/json"
	"fmt"
	"slices"
	"time"

	"github.com/hongzicong/ConsensusArena/protocol"
	"github.com/hongzicong/ConsensusArena/replica/defs"
	"github.com/hongzicong/ConsensusArena/rpc"
	"github.com/hongzicong/ConsensusArena/state"
)

type leaderCall struct{ done chan int }

// GetBodegaRoster only observes the serialized engine; unlike BeTheLeader it
// cannot propose a roster or initiate an election.
func (r *Replica) GetBodegaRoster(_ *defs.GetLeaderArgs, out *defs.BodegaRosterReply) error {
	done := make(chan defs.BodegaRosterReply, 1)
	timer := time.NewTimer(time.Second)
	defer timer.Stop()
	select {
	case r.rosterQueries <- done:
	case <-timer.C:
		return fmt.Errorf("Bodega roster query busy")
	}
	select {
	case *out = <-done:
		return nil
	case <-timer.C:
		return fmt.Errorf("Bodega roster query timeout")
	}
}

func (r *Replica) BeTheLeader(_ *defs.BeTheLeaderArgs, out *defs.BeTheLeaderReply) error {
	done := make(chan int, 1)
	select {
	case r.control <- leaderCall{done}:
	case <-time.After(5 * time.Second):
		return fmt.Errorf("Bodega control busy")
	}
	select {
	case id := <-done:
		out.Leader = int32(id)
		out.NextLeader = -1
		return nil
	case <-time.After(5 * time.Second):
		return fmt.Errorf("Bodega control timeout")
	}
}

func (r *Replica) run(opt options, isLeader bool, code uint8, inbox chan rpc.Serializable) {
	transport := r.configureTransport(code)
	r.ClientReplyCapacity = 8192
	r.ConnectToPeersConcurrent()
	defer r.CloseSenders()
	go r.WaitForClientConnections()
	waiting := map[defs.RequestID]*defs.GPropose{}
	e := newEngine(int(r.Id), r.N, opt, time.Now())
	r.engine, r.waiting = e, waiting
	e.clock = time.Now
	e.batching = true
	e.trace = r.Printf
	r.Printf("BODEGA_TIMING lease=%s margin=%s heartbeat=%s failure_min=%s failure_max=%s", opt.Lease, opt.Margin, opt.Heartbeat, opt.Failure, opt.FailureMax)
	e.execute = func(c state.Command) state.Value { return c.Execute(r.State) }
	e.trySend = func(to int, m message) bool {
		if transport.trySend(to, m) {
			return true
		}
		if m.Kind != promise {
			e.stats.DroppedMessages++
		}
		return false
	}
	e.reply = func(req request, v state.Value) {
		if p, ok := waiting[req.id()]; ok {
			if r.sendReply(p, v) {
				delete(waiting, req.id())
			} else {
				e.enqueue(req)
			}
		}
	}

	if isLeader {
		e.proposeRoster(e.id, opt.Responders, time.Now())
	}
	ticker := time.NewTicker(opt.Heartbeat)
	defer ticker.Stop()
	batchTicker := time.NewTicker(batchInterval)
	defer batchTicker.Stop()
	metrics := time.NewTicker(time.Second)
	defer metrics.Stop()
	var lastBallot uint64
	for {
		select {
		case raw := <-inbox:
			protocol.Must(r.Handle(raw, time.Now()))
		case peer := <-transport.drained:
			protocol.Must(r.Handle(promisesDrained(peer), time.Time{}))
		case p := <-r.ProposeChan:
			protocol.Must(r.Propose(p, time.Now()))
		case <-batchTicker.C:
			protocol.Must(r.Tick(protocol.Tick{Kind: protocol.Batch, Now: time.Now()}))
		case <-ticker.C:
			protocol.Must(r.Tick(protocol.Tick{Now: time.Now()}))
		case done := <-r.rosterQueries:
			done <- defs.BodegaRosterReply{Ballot: e.current.Ballot, Leader: e.current.Leader, Ready: e.active(), Responders: e.current.Responders, Ranges: slices.Clone(e.current.Ranges)}
		case call := <-r.control:
			protocol.Must(r.Handle(call, time.Now()))
		case <-metrics.C:
			var bytes, frames [kindCount]uint64
			for i := range bytes {
				bytes[i] = transport.wireBytes[i].Load()
				frames[i] = transport.wireFrames[i].Load()
			}
			r.Printf("BODEGA_WIRE replica=%d encoded_bytes_by_kind=%v frames_by_kind=%v", e.id, bytes, frames)
			s, _ := json.Marshal(e.stats)
			r.Printf("BODEGA_STATS replica=%d ballot=%d leader=%d prefix=%d high=%d pending=%d held=%d accepted_prefix=%d prepared=%t counters=%s", e.id, e.current.Ballot, e.current.Leader, e.prefix, e.high, len(waiting), len(e.held), e.acceptedPrefix, e.prepared, s)
		}
		if e.current.Ballot != lastBallot {
			lastBallot = e.current.Ballot
			r.Printf("BODEGA_ROSTER replica=%d ballot=%d leader=%d responders=%x", e.id, lastBallot, e.current.Leader, e.current.Responders)
			if len(e.current.Ranges) > 0 {
				ranges, _ := json.Marshal(e.current.Ranges)
				r.Printf("BODEGA_RANGES replica=%d ballot=%d ranges=%s", e.id, lastBallot, ranges)
			}
		}
	}
}

var _ protocol.Machine = (*Replica)(nil)

type promisesDrained int

func (r *Replica) Propose(p *defs.GPropose, now time.Time) error {
	if err := protocol.ValidateProposal(p); err != nil {
		return err
	}
	req := request{Proposal: *p.Propose, Origin: r.engine.id}
	if p.Command.Op == defs.BodegaCancelRead {
		if old, ok := r.waiting[req.id()]; ok && old.Command.Op == state.GET {
			r.engine.cancelRead(req.id())
			delete(r.waiting, req.id())
		}
		return nil
	}
	if p.Command.Op > state.SCAN || len(p.Command.V) > maxFrame-4096 {
		return nil
	}
	r.waiting[req.id()] = p
	r.engine.submit(req, now)
	return nil
}

func (r *Replica) Handle(event any, now time.Time) error {
	switch m := event.(type) {
	case *message:
		if m == nil {
			return protocol.UnsupportedEvent("bodega", event)
		}
		r.engine.receive(*m, now)
	case promisesDrained:
		r.engine.flushPromises(int(m))
	case leaderCall:
		r.handleLeaderCall(m, now)
	default:
		return protocol.UnsupportedEvent("bodega", event)
	}
	return nil
}

func (r *Replica) Tick(t protocol.Tick) error {
	switch t.Kind {
	case protocol.Maintenance:
		r.engine.tick(t.Now)
	case protocol.Batch:
		r.engine.flushBatch(t.Now)
	default:
		return protocol.UnsupportedTimer("bodega", t.Kind)
	}
	return nil
}

func (r *Replica) handleLeaderCall(call leaderCall, now time.Time) {
	leader := r.engine.current.Leader
	if r.engine.current.Ballot == 0 || !r.engine.peerHealthy(leader, now) {
		mask := (uint64(1) << uint(r.engine.n)) - 1
		for p := range r.engine.seen {
			if !r.engine.peerHealthy(p, now) {
				mask &^= bit(p)
			}
		}
		r.engine.proposeFilteredRoster(r.engine.id, mask, now)
		leader = r.engine.id
	}
	call.done <- leader
}
