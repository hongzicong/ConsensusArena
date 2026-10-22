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

// GetBodegaRoster only observes the serialized core; unlike BeTheLeader it
// cannot propose a roster or initiate an election.
func (r *Replica) GetBodegaRoster(_ *defs.GetLeaderArgs, out *BodegaRosterReply) error {
	done := make(chan BodegaRosterReply, 1)
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
	proposals := r.proposals
	c := r.core
	c.activate(time.Now())
	c.clock = time.Now
	c.batching = true
	c.trace = r.Printf
	r.Printf("BODEGA_TIMING lease=%s margin=%s heartbeat=%s failure_min=%s failure_max=%s", opt.Lease, opt.Margin, opt.Heartbeat, opt.Failure, opt.FailureMax)
	c.execute = func(c state.Command) state.Value { return c.Execute(r.State) }

	c.trySend = transport.trySend
	c.tryBroadcast = transport.broadcast
	c.reply = func(req request, v state.Value) bool {
		if p, ok := proposals[req.id()]; ok {
			if !r.sendReply(p, v) {
				return false
			}
			delete(proposals, req.id())
		}
		return true
	}

	if isLeader {
		c.proposeRoster(c.id, opt.Responders, time.Now())
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
			done <- BodegaRosterReply{Ballot: c.current.Ballot, Leader: c.current.Leader, Ready: c.active(), Responders: c.current.Responders, Ranges: slices.Clone(c.current.Ranges)}
		case call := <-r.control:
			protocol.Must(r.Handle(call, time.Now()))
		case <-metrics.C:
			var bytes, frames [kindCount]uint64
			for i := range bytes {
				bytes[i] = transport.wireBytes[i].Load()
				frames[i] = transport.wireFrames[i].Load()
			}
			r.Printf("BODEGA_WIRE replica=%d encoded_bytes_by_kind=%v frames_by_kind=%v", c.id, bytes, frames)
			s, _ := json.Marshal(c.stats)
			r.Printf("BODEGA_STATS replica=%d ballot=%d leader=%d prefix=%d high=%d pending=%d held=%d accepted_prefix=%d prepared=%t counters=%s", c.id, c.current.Ballot, c.current.Leader, c.prefix, c.high, len(proposals), len(c.held), c.acceptedPrefix, c.prepared, s)
		}
		if c.current.Ballot != lastBallot {
			lastBallot = c.current.Ballot
			r.Printf("BODEGA_ROSTER replica=%d ballot=%d leader=%d responders=%x", c.id, lastBallot, c.current.Leader, c.current.Responders)
			if len(c.current.Ranges) > 0 {
				ranges, _ := json.Marshal(c.current.Ranges)
				r.Printf("BODEGA_RANGES replica=%d ballot=%d ranges=%s", c.id, lastBallot, ranges)
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
	req := request{Proposal: *p.Propose, Origin: r.core.id}
	if p.Command.Op == BodegaCancelRead {
		if old, ok := r.proposals[req.id()]; ok && old.Command.Op == state.GET {
			r.core.cancelRead(req.id())
			delete(r.proposals, req.id())
		}
		return nil
	}
	if err := (state.CommandPolicy{MaxValueBytes: maxFrame - 4096}).Validate(p.Command); err != nil {
		return nil
	}
	r.proposals[req.id()] = p
	r.core.submit(req, now)
	return nil
}

func (r *Replica) Handle(event any, now time.Time) error {
	switch m := event.(type) {
	case *message:
		if m == nil {
			return protocol.UnsupportedEvent("bodega", event)
		}
		r.core.receive(*m, now)
	case promisesDrained:
		r.core.flushPromises(int(m))
	case leaderCall:
		m.done <- r.core.handleLeaderCall(now)
	default:
		return protocol.UnsupportedEvent("bodega", event)
	}
	return nil
}

func (r *Replica) Tick(t protocol.Tick) error {
	switch t.Kind {
	case protocol.Maintenance:
		r.core.tick(t.Now)
	case protocol.Batch:
		r.core.flushBatch(t.Now)
	default:
		return protocol.UnsupportedTimer("bodega", t.Kind)
	}
	return nil
}
