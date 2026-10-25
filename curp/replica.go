package curp

import (
	"bufio"
	"fmt"
	"io"
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
	*replica.EventLoop
	core          *core
	proposals     map[defs.RequestID]*defs.GPropose
	ReplyMessage  func(Request, state.Value, bool, int32) (uint8, fastrpc.Serializable)
	RecordMessage func(Request, bool, int32) (uint8, fastrpc.Serializable)
}

func New(alias string, replicaID int, addrs []string, executeEnabled bool, f int, conf *config.Config, logger *dlog.Logger) *Replica {
	// The protocol runtime owns normal operation and recovery state.
	base := replica.New(alias, replicaID, f, addrs, executeEnabled, conf, logger)

	ballot := conf.Plan.LeaderID()

	var channels transportChannels
	registerRPCs(&channels, base.RPC)

	r := newReplica(base, ballot)
	r.ReplyMessage = func(req Request, value state.Value, fast bool, ballot int32) (uint8, fastrpc.Serializable) {
		id := req.ID
		if fast {
			return channels.replyRPC, &MReply{Replica: base.ID, Ballot: ballot, CmdId: id, Rep: value, Ok: TRUE}
		}
		return channels.syncReplyRPC, &MSyncReply{Replica: base.ID, Ballot: ballot, CmdId: id, Rep: value}
	}
	r.RecordMessage = func(req Request, positive bool, ballot int32) (uint8, fastrpc.Serializable) {
		ok := FALSE
		if positive {
			ok = TRUE
		}
		return channels.recordAckRPC, &MRecordAck{Replica: base.ID, Ballot: ballot, CmdId: req.ID, Ok: ok}
	}
	go r.Run()

	return r
}

func newReplica(base *replica.Replica, leader int32) *Replica {
	r := &Replica{Replica: base, core: newCore(base.N, base.ID, leader, func(cmd state.Command) state.Value {
		return cmd.Execute(base.State)
	}), proposals: map[defs.RequestID]*defs.GPropose{}}
	r.EventLoop = newEventLoop(base, &protocolAdapter{r})
	code := r.EventLoop.Register(&Packet{})
	r.core.send = func(id int32, p *Packet) { r.EventLoop.Send(id, code, p) }
	r.core.broadcast = func(p *Packet) { r.EventLoop.SendToAll(p, code) }
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

func (r *Replica) client(id defs.RequestID, code uint8, msg interface{ Marshal(io.Writer) }, custom bool) {
	var writer *bufio.Writer
	if g := r.proposals[id]; g != nil {
		writer = g.Reply
		if writer == nil {
			return
		}
	}
	r.EventLoop.Reply(id.Client, writer, code, msg, custom)
}

type protocolAdapter struct{ replica *Replica }

var _ replica.EventLoopMachine = (*protocolAdapter)(nil)

func (p *protocolAdapter) Propose(g *defs.GPropose, _ time.Time) error {
	if err := protocol.ValidateProposal(g); err != nil {
		return err
	}
	id := g.RequestID()
	p.replica.proposals[id] = g
	p.replica.core.submit(Request{id, g.Command})
	return nil
}

func (p *protocolAdapter) Step(event any, _ time.Time) error {
	message, ok := event.(*Packet)
	if !ok || message == nil {
		return protocol.UnsupportedEvent("curp", event)
	}
	p.replica.core.receive(message)
	return nil
}

func (p *protocolAdapter) Tick(t protocol.Tick) error {
	if t.Kind != protocol.Maintenance {
		return protocol.UnsupportedTimer("curp", t.Kind)
	}
	if len(t.Alive) != p.replica.core.n {
		return fmt.Errorf("curp: connectivity snapshot size mismatch")
	}
	p.replica.core.tick(t.Now, t.Alive)
	return nil
}

func (p *protocolAdapter) Leader() int32 { return p.replica.core.leader }

func (p *protocolAdapter) Status() string {
	c := p.replica.core
	return fmt.Sprintf("BASELINE_RECOVERY curp=%t classic=%t ballot=%d leader=%d active=%t preparing=%t accepted=%d executed=%d pending=%d witness=%d recoveries=%d recovery_end=%d gap_accepts=%d fetch_requests=%d fetch_suppressed=%d", true, false, c.ballot, c.leader, c.active, c.preparing, c.high, c.executed, len(c.pending), len(c.witness), c.recoveries, c.recoveryEnd, c.gapAccepts, c.fetchRequests, c.fetchSuppressed)
}

// Report the protocol coordinator; the master does not bypass Phase 1.
func (r *Replica) BeTheLeader(_ *defs.BeTheLeaderArgs, reply *defs.BeTheLeaderReply) error {
	return r.EventLoop.LeaderHint(reply)
}

// These entry points are also used by external event drivers. Calls belong to
// this replica's event-loop owner; they do not introduce a second scheduler.
var _ protocol.Machine = (*Replica)(nil)

func (r *Replica) Propose(p *defs.GPropose, now time.Time) error {
	return r.EventLoop.Machine.Propose(p, now)
}

func (r *Replica) Step(event any, now time.Time) error {
	return r.EventLoop.Machine.Step(event, now)
}

func (r *Replica) Tick(t protocol.Tick) error {
	return r.EventLoop.Machine.Tick(t)
}
