package paxos

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

// protocolRuntime connects this package's core to its event loop and output queues.
type protocolRuntime struct {
	*transportRuntime
	Core          *Core
	proposals     map[defs.RequestID]*defs.GPropose
	ReplyMessage  func(Request, state.Value, bool, int32) (uint8, fastrpc.Serializable)
	RecordMessage func(Request, bool, int32) (uint8, fastrpc.Serializable)
}

func newProtocolRuntime(base *replica.Replica, leader int32) *protocolRuntime {
	r := &protocolRuntime{Core: newCore(base.N, base.Id, leader, base.State), proposals: map[defs.RequestID]*defs.GPropose{}}
	r.transportRuntime = newTransportRuntime(base, &protocolAdapter{r})
	code := r.Register(&Packet{})
	r.Core.Send = func(id int32, p *Packet) { r.Send(id, code, p) }
	r.Core.Reply = func(req Request, v state.Value, fast bool) {
		if r.ReplyMessage != nil {
			code, msg := r.ReplyMessage(req, v, fast, int32(r.Core.Ballot))
			r.client(req.ID, code, msg, true)
		} else {
			// A recovered command may have no local reply stream yet. A retry
			// restores proposals, and Core.Values returns the original result.
			g := r.proposals[req.ID]
			if g != nil {
				r.client(req.ID, 0, &defs.ProposeReplyTS{OK: defs.TRUE, CommandId: req.ID.Sequence, Value: v, Timestamp: g.Timestamp}, false)
			}
		}
		if !fast {
			delete(r.proposals, req.ID)
		}
	}
	r.Core.RecordReply = func(req Request, ok bool) {
		if r.RecordMessage != nil {
			code, msg := r.RecordMessage(req, ok, int32(r.Core.Ballot))
			r.client(req.ID, code, msg, true)
		}
	}
	return r
}

func (r *protocolRuntime) client(id defs.RequestID, code uint8, msg interface{ Marshal(io.Writer) }, custom bool) {
	var writer *bufio.Writer
	if g := r.proposals[id]; g != nil {
		writer = g.Reply
		if writer == nil {
			return
		}
	}
	r.transportRuntime.Reply(id.Client, writer, code, msg, custom)
}

type protocolAdapter struct{ runtime *protocolRuntime }

var _ runtimeProtocol = (*protocolAdapter)(nil)

func (p *protocolAdapter) Propose(g *defs.GPropose, _ time.Time) error {
	if err := protocol.ValidateProposal(g); err != nil {
		return err
	}
	id := g.RequestID()
	p.runtime.proposals[id] = g
	p.runtime.Core.Propose(Request{id, g.Command})
	return nil
}

func (p *protocolAdapter) Handle(event any, _ time.Time) error {
	message, ok := event.(*Packet)
	if !ok || message == nil {
		return protocol.UnsupportedEvent("paxos", event)
	}
	p.runtime.Core.Handle(message)
	return nil
}

func (p *protocolAdapter) Tick(t protocol.Tick) error {
	if t.Kind != protocol.Maintenance {
		return protocol.UnsupportedTimer("paxos", t.Kind)
	}
	if len(t.Alive) != p.runtime.Core.N {
		return fmt.Errorf("paxos: connectivity snapshot size mismatch")
	}
	p.runtime.Core.Tick(t.Now, t.Alive)
	return nil
}

func (p *protocolAdapter) Leader() int32 { return p.runtime.Core.Leader }

func (p *protocolAdapter) Status() string {
	c := p.runtime.Core
	return fmt.Sprintf("BASELINE_RECOVERY curp=%t classic=%t ballot=%d leader=%d active=%t preparing=%t accepted=%d executed=%d results=%d replay_replies=%d pending=%d witness=%d recoveries=%d recovery_end=%d gap_accepts=%d fetch_requests=%d fetch_suppressed=%d", false, true, c.Ballot, c.Leader, c.Active, c.Preparing, c.High, c.Executed, len(c.Values), c.ReplayReplies, len(c.Pending), len(c.Witness), c.Recoveries, c.recoveryEnd, c.GapAccepts, c.FetchRequests, c.FetchSuppressed)
}

// The master discovers the ballot owner; changing a flag cannot install a leader.
func (r *Replica) BeTheLeader(args *defs.BeTheLeaderArgs, reply *defs.BeTheLeaderReply) error {
	return r.recovery.LeaderHint(reply)
}

// Protocol callbacks are serialized by Run. Sending to self invokes Handle
// synchronously on the same goroutine, preserving reentrant local transitions.
// Tick supplies connectivity observations; the protocol decides how to recover.
type runtimeProtocol interface {
	protocol.Machine
	Leader() int32
	Status() string
}

type transportRuntime struct {
	replica.Transport
	protocol runtimeProtocol
	in       chan fastrpc.Serializable
	control  chan chan int32
}

func (r *transportRuntime) Run() {
	r.Base.PeerSendOptions = replica.SenderOptions{Capacity: 4096}
	r.Base.ConnectToPeers()
	defer r.Base.CloseSenders()
	r.Peers = r.Base.PeerSenders
	r.Base.ComputeClosestPeers()
	go r.Base.WaitForClientConnections()
	ticker := time.NewTicker(2 * time.Millisecond)
	defer ticker.Stop()
	progress := time.NewTicker(5 * time.Second)
	defer progress.Stop()
	for {
		select {
		case reply := <-r.control:
			reply <- r.protocol.Leader()
		case g := <-r.Base.ProposeChan:
			protocol.Must(r.protocol.Propose(g, time.Time{}))
		case msg := <-r.in:
			protocol.Must(r.protocol.Handle(msg, time.Time{}))
		case now := <-ticker.C:
			r.Base.M.Lock()
			alive := append([]bool(nil), r.Base.Alive...)
			r.Base.M.Unlock()
			alive[r.Base.Id] = true
			protocol.Must(r.protocol.Tick(protocol.Tick{Now: now, Alive: alive}))
		case <-progress.C:
			r.Base.Printf("%s replies=%d send_drops=%d", r.protocol.Status(), r.Replies, r.SendDrops)
		}
	}
}

// LeaderHint observes the protocol leader; it never installs one.
func (r *transportRuntime) LeaderHint(reply *defs.BeTheLeaderReply) error {
	ch := make(chan int32, 1)
	timer := time.NewTimer(30 * time.Second)
	defer timer.Stop()
	select {
	case r.control <- ch:
	case <-timer.C:
		return fmt.Errorf("replica initializing")
	}
	select {
	case id := <-ch:
		reply.Leader = id
		if id == 0 {
			reply.Leader = -2
		}
		reply.NextLeader = -1
		return nil
	case <-timer.C:
		return fmt.Errorf("replica event loop unavailable")
	}
}

// These entry points are also used by external event drivers. Calls belong to
// this replica's event-loop owner; they do not introduce a second scheduler.
var _ protocol.Machine = (*Replica)(nil)

func (r *Replica) Propose(p *defs.GPropose, now time.Time) error {
	return r.recovery.protocol.Propose(p, now)
}
func (r *Replica) Handle(event any, now time.Time) error {
	return r.recovery.protocol.Handle(event, now)
}
func (r *Replica) Tick(t protocol.Tick) error {
	return r.recovery.protocol.Tick(t)
}
