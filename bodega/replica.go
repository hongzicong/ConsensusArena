package bodega

import (
	"encoding/json"
	"fmt"
	"io"
	"slices"
	"strings"
	"sync/atomic"
	"time"

	"github.com/hongzicong/ConsensusArena/config"
	"github.com/hongzicong/ConsensusArena/dlog"
	"github.com/hongzicong/ConsensusArena/replica"
	"github.com/hongzicong/ConsensusArena/replica/defs"
	"github.com/hongzicong/ConsensusArena/rpc"
	"github.com/hongzicong/ConsensusArena/state"
)

type leaderCall struct{ done chan int }
type Replica struct {
	*replica.Replica
	control       chan leaderCall
	rosterQueries chan chan defs.BodegaRosterReply
}

func readOptions(c *config.Config, n int) (options, error) {
	o := options{Lease: 2 * time.Second, Margin: 100 * time.Millisecond, Heartbeat: 100 * time.Millisecond, Failure: 1200 * time.Millisecond}
	if n < 3 || n > 63 || n%2 == 0 {
		return o, fmt.Errorf("Bodega requires odd membership of 3..63 replicas")
	}
	if c.Noop {
		return o, fmt.Errorf("Bodega requires state-machine execution (noop: false)")
	}
	if c.BodegaLease != 0 {
		o.Lease = c.BodegaLease
	}
	if c.BodegaMargin != 0 {
		o.Margin = c.BodegaMargin
	}
	if c.BodegaHeartbeat != 0 {
		o.Heartbeat = c.BodegaHeartbeat
	}
	if c.BodegaFailure != 0 {
		o.Failure = c.BodegaFailure
	}
	if o.Heartbeat <= 0 || o.Failure <= 2*o.Heartbeat || o.Lease <= o.Failure || o.Margin <= 0 {
		return o, fmt.Errorf("Bodega requires 0 < 2*heartbeat < failure < lease, positive margin")
	}
	names := strings.TrimSpace(c.BodegaResponders)
	if names == "" {
		names = "all"
	}
	var err error
	o.Responders, err = responderMask(names, c, n)
	if err != nil {
		return o, err
	}
	o.Ranges, err = responderRanges(c.BodegaResponderRanges, c, n)
	return o, err
}

func New(alias string, id int, addrs []string, isLeader bool, c *config.Config, l *dlog.Logger) *Replica {
	opt, err := readOptions(c, len(addrs))
	if err != nil {
		panic(err)
	}
	r := &Replica{Replica: replica.New(alias, id, (len(addrs)-1)/2, addrs, false, true, false, c, l), control: make(chan leaderCall, 8)}
	r.rosterQueries = make(chan chan defs.BodegaRosterReply, 8)
	inbox := make(chan rpc.Serializable, 8192)
	code := r.RPC.Register(&message{}, inbox)
	go r.run(opt, isLeader, code, inbox)
	return r
}

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
	r.ConnectToPeersConcurrent()
	go r.WaitForClientConnections()
	// Each peer has one writer. A slow peer never stalls the protocol clock.
	var wireBytes, wireFrames [11]atomic.Uint64
	queues := make([]chan message, r.N)
	controlQueues := make([]chan message, r.N)
	for i := 0; i < r.N; i++ {
		if i == int(r.Id) {
			continue
		}
		queues[i] = make(chan message, 4096)
		controlQueues[i] = make(chan message, 4096)
		go func(peer int) {
			for {
				var m message
				select {
				case m = <-controlQueues[peer]:
				default:
					select {
					case m = <-controlQueues[peer]:
					case m = <-queues[peer]:
					}
				}
				conn, w := r.Peers[peer], r.PeerWriters[peer]
				if conn == nil || w == nil {
					continue
				}
				_ = conn.SetWriteDeadline(time.Now().Add(2 * time.Second))
				if err := w.WriteByte(code); err != nil {
					continue
				}
				counter := countedWriter{w: w}
				m.Marshal(&counter)
				wireBytes[m.Kind].Add(uint64(counter.n + 1))
				wireFrames[m.Kind].Add(1)
				if err := w.Flush(); err != nil {
					_ = conn.Close()
				}
			}
		}(i)
	}
	type response struct {
		p     *defs.GPropose
		value state.Value
	}
	replies := make(chan response, 8192)
	go func() {
		for x := range replies {
			x.p.Mutex.Lock()
			(&defs.ProposeReplyTS{OK: defs.TRUE, CommandId: x.p.CommandId, Value: x.value, Timestamp: x.p.Timestamp}).Marshal(x.p.Reply)
			_ = x.p.Reply.Flush()
			x.p.Mutex.Unlock()
		}
	}()
	waiting := map[requestID]*defs.GPropose{}
	e := newEngine(int(r.Id), r.N, opt, time.Now())
	e.clock = time.Now
	e.batching = true
	e.trace = r.Printf
	e.execute = func(c state.Command) state.Value { return c.Execute(r.State) }
	e.send = func(to int, m message) {
		q := controlQueues[to]
		if m.Kind == accept || m.Kind == forward {
			q = queues[to]
		}
		select {
		case q <- m:
		default:
			e.stats.DroppedMessages++ // protocol tick repairs dropped messages
		}
	}
	e.reply = func(req request, v state.Value) {
		if p, ok := waiting[req.id()]; ok {
			select {
			case replies <- response{p, v}:
				delete(waiting, req.id())
			default:
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
			e.receive(*raw.(*message), time.Now())
		case p := <-r.ProposeChan:
			req := request{Proposal: *p.Propose, Origin: e.id}
			if p.Command.Op == defs.BodegaCancelRead {
				if old, ok := waiting[req.id()]; ok && old.Command.Op == state.GET {
					e.cancelRead(req.id())
					delete(waiting, req.id())
				}
				continue
			}
			if p.Command.Op > state.SCAN || len(p.Command.V) > maxFrame-4096 {
				continue
			}
			waiting[req.id()] = p
			e.submit(req, time.Now())
		case <-batchTicker.C:
			e.flushBatch(time.Now())
		case <-ticker.C:
			e.tick(time.Now())
		case done := <-r.rosterQueries:
			done <- defs.BodegaRosterReply{Ballot: e.current.Ballot, Leader: e.current.Leader, Ready: e.active(), Responders: e.current.Responders, Ranges: slices.Clone(e.current.Ranges)}
		case call := <-r.control:
			now := time.Now()
			leader := e.current.Leader
			if e.current.Ballot == 0 || now.Sub(e.seen[leader]) >= opt.Failure {
				mask := (uint64(1) << uint(e.n)) - 1
				for p, t := range e.seen {
					if now.Sub(t) >= opt.Failure {
						mask &^= bit(p)
					}
				}
				e.proposeFilteredRoster(e.id, mask, now)
				leader = e.id
			}
			call.done <- leader
		case <-metrics.C:
			var bytes, frames [11]uint64
			for i := range bytes {
				bytes[i] = wireBytes[i].Load()
				frames[i] = wireFrames[i].Load()
			}
			r.Printf("BODEGA_WIRE replica=%d encoded_bytes_by_kind=%v frames_by_kind=%v", e.id, bytes, frames)
			s, _ := json.Marshal(e.stats)
			r.Printf("BODEGA_STATS replica=%d ballot=%d leader=%d prefix=%d high=%d pending=%d held=%d counters=%s", e.id, e.current.Ballot, e.current.Leader, e.prefix, e.high, len(waiting), len(e.held), s)
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

// Counts encoded attempts, including the outer RPC tag; not acknowledged TCP bytes.
type countedWriter struct {
	w io.Writer
	n int
}

func (w *countedWriter) Write(b []byte) (int, error) { n, err := w.w.Write(b); w.n += n; return n, err }
