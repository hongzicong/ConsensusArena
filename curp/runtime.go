package curp

// Event scheduling, protocol integration, and control/status handling.
import (
	"bufio"
	"fmt"
	"io"
	"time"

	"github.com/hongzicong/ConsensusArena/protocol"
	"github.com/hongzicong/ConsensusArena/replica"
	"github.com/hongzicong/ConsensusArena/replica/defs"
	fastrpc "github.com/hongzicong/ConsensusArena/rpc"
	"github.com/hongzicong/ConsensusArena/state"
)

// runtime connects this package's core to its event loop and output queues.
type runtime struct {
	*replica.EventLoop
	core          *core
	proposals     map[defs.RequestID]*defs.GPropose
	ReplyMessage  func(Request, state.Value, bool, int32) (uint8, fastrpc.Serializable)
	RecordMessage func(Request, bool, int32) (uint8, fastrpc.Serializable)
}

func newRuntime(base *replica.Replica, leader int32) *runtime {
	r := &runtime{core: newCore(base.N, base.ID, leader, func(cmd state.Command) state.Value {
		return cmd.Execute(base.State)
	}), proposals: map[defs.RequestID]*defs.GPropose{}}
	r.EventLoop = newEventLoop(base, &protocolAdapter{r})
	code := r.Register(&Packet{})
	r.core.send = func(id int32, p *Packet) { r.Send(id, code, p) }
	r.core.broadcast = func(p *Packet) { r.SendToAll(p, code) }
	r.core.reply = func(req Request, v state.Value, fast bool) {
		if r.ReplyMessage != nil {
			code, msg := r.ReplyMessage(req, v, fast, int32(r.core.ballot))
			r.client(req.ID, code, msg, true)
		} else {
			g := r.proposals[req.ID]
			if g != nil {
				r.client(req.ID, 0, &defs.ProposeReplyTS{OK: defs.TRUE, CommandId: req.ID.Sequence, Value: v, Timestamp: g.Timestamp}, false)
			}
		}
		if !fast {
			delete(r.proposals, req.ID)
		}
	}
	r.core.replyRecord = func(req Request, ok bool) {
		if r.RecordMessage != nil {
			code, msg := r.RecordMessage(req, ok, int32(r.core.ballot))
			r.client(req.ID, code, msg, true)
		}
	}
	return r
}

func (r *runtime) client(id defs.RequestID, code uint8, msg interface{ Marshal(io.Writer) }, custom bool) {
	var writer *bufio.Writer
	if g := r.proposals[id]; g != nil {
		writer = g.Reply
		if writer == nil {
			return
		}
	}
	r.EventLoop.Reply(id.Client, writer, code, msg, custom)
}

type protocolAdapter struct{ runtime *runtime }

var _ replica.EventLoopMachine = (*protocolAdapter)(nil)

func (p *protocolAdapter) Propose(g *defs.GPropose, _ time.Time) error {
	if err := protocol.ValidateProposal(g); err != nil {
		return err
	}
	id := g.RequestID()
	p.runtime.proposals[id] = g
	p.runtime.core.submit(Request{id, g.Command})
	return nil
}

func (p *protocolAdapter) Step(event any, _ time.Time) error {
	message, ok := event.(*Packet)
	if !ok || message == nil {
		return protocol.UnsupportedEvent("curp", event)
	}
	p.runtime.core.receive(message)
	return nil
}

func (p *protocolAdapter) Tick(t protocol.Tick) error {
	if t.Kind != protocol.Maintenance {
		return protocol.UnsupportedTimer("curp", t.Kind)
	}
	if len(t.Alive) != p.runtime.core.n {
		return fmt.Errorf("curp: connectivity snapshot size mismatch")
	}
	p.runtime.core.tick(t.Now, t.Alive)
	return nil
}

func (p *protocolAdapter) Leader() int32 { return p.runtime.core.leader }

func (p *protocolAdapter) Status() string {
	c := p.runtime.core
	return fmt.Sprintf("BASELINE_RECOVERY curp=%t classic=%t ballot=%d leader=%d active=%t preparing=%t accepted=%d executed=%d pending=%d witness=%d recoveries=%d recovery_end=%d gap_accepts=%d fetch_requests=%d fetch_suppressed=%d", true, false, c.ballot, c.leader, c.active, c.preparing, c.high, c.executed, len(c.pending), len(c.witness), c.recoveries, c.recoveryEnd, c.gapAccepts, c.fetchRequests, c.fetchSuppressed)
}

// Report the protocol coordinator; the master does not bypass Phase 1.
func (r *Replica) BeTheLeader(_ *defs.BeTheLeaderArgs, reply *defs.BeTheLeaderReply) error {
	return r.runtime.LeaderHint(reply)
}

// These entry points are also used by external event drivers. Calls belong to
// this replica's event-loop owner; they do not introduce a second scheduler.
var _ protocol.Machine = (*Replica)(nil)

func (r *Replica) Propose(p *defs.GPropose, now time.Time) error {
	return r.runtime.Machine.Propose(p, now)
}
func (r *Replica) Step(event any, now time.Time) error {
	return r.runtime.Machine.Step(event, now)
}
func (r *Replica) Tick(t protocol.Tick) error {
	return r.runtime.Machine.Tick(t)
}
