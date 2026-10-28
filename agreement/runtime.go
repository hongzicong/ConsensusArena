package agreement

// Event scheduling, protocol integration, and control/status handling.
import (
	"encoding/binary"
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
func (r *Replica) GetBodegaRoster(_ *defs.GetLeaderArgs, out *RosterReply) error {
	done := make(chan RosterReply, 1)
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
	waiting := map[defs.RequestID][]*defs.GPropose{}
	e := newEngine(int(r.ID), r.N, opt, time.Now())
	r.engine, r.waiting = e, waiting
	e.observeLocalRead = r.readPaths.Local
	r.learnerStreams = map[int32][]*defs.GPropose{}
	e.clientVote = r.sendClientVotes
	r.ingressSince = map[defs.RequestID]time.Time{}
	e.clock = time.Now
	r.cold = newColdReplica(r, r.coldCfg)
	e.auxRoles = r.cold.requiredRoles
	e.intercept = r.cold.intercept
	e.applySpecial = r.cold.applyBridgeOrScan
	e.beforeApply = r.cold.beforeBridgeApply
	e.finishSpecial = r.cold.finishMigrated
	e.clientVote = func(v entry) {
		if !r.cold.migrated && !r.cold.hasBridge() {
			r.sendClientVotes(v)
		}
	}
	r.Printf("VOTE_SUBSCRIPTIONS replica=%d planned_mask=%x", e.id, opt.Learners)
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
		if attempts, ok := waiting[req.id()]; ok {
			anySent := false
			for _, p := range attempts {
				e.stats.FanoutAttempts++
				if r.sendReply(p, v) {
					anySent = true
					e.stats.FanoutEnqueued++
				}
			}
			if anySent {
				if req.Proposal.Command.Op == state.GET && r.cold.isCold(req.Proposal.Command.K) {
					if _, retained := r.cold.reads[req.id()]; retained {
						r.cold.stats.CompletedReadRetirements++
					}
					delete(r.cold.reads, req.id())
					delete(r.cold.readSince, req.id())
				}
				if start, ok := r.ingressSince[req.id()]; ok {
					d := time.Since(start)
					e.stats.IngressSamples++
					e.stats.IngressNanos += uint64(d)
					e.stats.IngressHistogram[histogramBucket(d)]++
					delete(r.ingressSince, req.id())
				}
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
			protocol.Must(r.Step(raw, time.Now()))
		case peer := <-transport.drained:
			protocol.Must(r.Step(promisesDrained(peer), time.Time{}))
		case p := <-r.ProposeChan:
			protocol.Must(r.Propose(p, time.Now()))
		case <-batchTicker.C:
			protocol.Must(r.Tick(protocol.Tick{Kind: protocol.Batch, Now: time.Now()}))
		case <-ticker.C:
			protocol.Must(r.Tick(protocol.Tick{Now: time.Now()}))
		case done := <-r.rosterQueries:
			done <- RosterReply{ColdBallot: r.cold.installed.B, ColdEpoch: r.cold.installed.E, ColdMigrated: r.cold.migrated, ColdQuorum: r.cold.advertisedQ(), ColdReady: r.cold.ready, Ballot: e.current.Ballot, Leader: e.current.Leader, Ready: e.active(), Responders: e.current.Responders, Ranges: slices.Clone(e.current.Ranges)}
		case call := <-r.control:
			protocol.Must(r.Step(call, time.Now()))
		case <-metrics.C:
			r.Printf("%s", r.readPaths.Status("agreement", e.id))
			var bytes, frames [kindCount]uint64
			for i := range bytes {
				bytes[i] = transport.wireBytes[i].Load()
				frames[i] = transport.wireFrames[i].Load()
			}
			r.Printf("BODEGA_WIRE replica=%d encoded_bytes_by_kind=%v frames_by_kind=%v", e.id, bytes, frames)
			r.cold.printStats()
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
	if r.cold.proposeEnvelope(p, now) {
		return nil
	}
	if p.Command.Op == registerLearner {
		streams := r.learnerStreams[p.ClientId]
		for _, old := range streams {
			if old.Reply == p.Reply {
				return nil
			}
		}
		if len(streams) < 4 {
			r.learnerStreams[p.ClientId] = append(streams, p)
		}
		return nil
	}
	if p.Command.Op == directPUT {
		if len(p.Command.V) < 4 || len(p.Command.V) > maxFrame-4096 {
			return nil
		}
		origin := int(binary.LittleEndian.Uint32(p.Command.V[:4]))
		if origin < 0 || origin >= r.N {
			return nil
		}
		req := request{Proposal: *p.Propose, Origin: origin}
		req.Proposal.Command.Op = state.PUT
		req.Proposal.Command.V = p.Command.V[4:]
		e := r.engine
		e.stats.DirectAdmissionArrivals++
		if e.id == e.current.Leader && e.active() && e.prepared {
			_, inflight := e.inflight[req.id()]
			_, done := e.completed[req.id()]
			if !inflight && !done && !e.batchIDs[req.id()] {
				e.stats.DirectFreshAdmissions++
			} else {
				e.stats.DirectRepeatedArrivals++
			}
		} else {
			e.stats.DirectForwardAdmissions++
		}
		e.submit(req, now)
		return nil
	}
	req := request{Proposal: *p.Propose, Origin: r.engine.id}
	if p.Command.Op == cancelRead {
		if old, ok := r.waiting[req.id()]; ok && len(old) > 0 && old[0].Command.Op == state.GET {
			r.engine.cancelRead(req.id())
			delete(r.cold.reads, req.id())
			delete(r.cold.readSince, req.id())
			delete(r.waiting, req.id())
		}
		return nil
	}
	if p.Command.Op > state.SCAN || len(p.Command.V) > maxFrame-4096 {
		return nil
	}
	if attempts, ok := r.waiting[req.id()]; ok {
		for _, existing := range attempts {
			if existing.Reply == p.Reply {
				return nil
			}
		}
		r.waiting[req.id()] = append(attempts, p)
		r.engine.stats.CoalescedAttempts++
		return nil
	}
	r.waiting[req.id()] = []*defs.GPropose{p}
	if p.Command.Op == state.PUT || p.Command.Op == state.SCAN {
		if _, ok := r.ingressSince[req.id()]; !ok {
			r.ingressSince[req.id()] = now
		}
	}
	r.engine.submit(req, now)
	return nil
}

func (r *Replica) Step(event any, now time.Time) error {
	switch m := event.(type) {
	case *message:
		if m == nil {
			return protocol.UnsupportedEvent("bodega", event)
		}
		if m.Kind == coldControl {
			r.cold.receive(m.From, m.Value, now)
		} else {
			r.engine.receive(*m, now)
		}
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
		r.cold.tick(t.Now)
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
