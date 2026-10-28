package pql

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
	"github.com/hongzicong/ConsensusArena/state"
)

// Replica owns runtime resources and one protocol core.
type Replica struct {
	*replica.Replica
	*replica.EventLoop
	core      *core
	proposals map[defs.RequestID]*defs.GPropose
}

func New(alias string, replicaID int, addrs []string, f int, conf *config.Config, logger *dlog.Logger) *Replica {
	policy, profile, err := loadLeasePolicy(conf)
	if err != nil {
		panic(err)
	}
	if err := emitPolicyRecord(conf, profile, policy); err != nil {
		panic(err)
	}
	base := replica.New(alias, replicaID, f, addrs, true, conf, logger)
	base.ProposalPolicy.Operations = []state.Operation{state.NONE, state.PUT, state.GET}
	leader := conf.Plan.LeaderID()
	r := newReplica(base, leader, policy)

	go r.Run()
	return r
}

func newReplica(base *replica.Replica, leader int32, policy *leasePolicy) *Replica {
	r := &Replica{Replica: base, core: newCore(base.N, base.ID, leader, policy, func(cmd state.Command) state.Value {
		return cmd.Execute(base.State)
	}), proposals: map[defs.RequestID]*defs.GPropose{}}
	r.core.now = time.Now
	r.core.configLog.now = time.Now
	r.EventLoop = newEventLoop(base, &protocolAdapter{r})
	code := r.EventLoop.Register(&Packet{})
	r.core.send = func(id int32, p *Packet) { r.EventLoop.Send(id, code, p) }
	r.core.broadcast = func(p *Packet) { r.EventLoop.SendToAll(p, code) }
	r.core.sendMask = func(mask uint64, p *Packet) {
		for id := 0; id < r.core.n; id++ {
			if mask&(uint64(1)<<uint(id)) != 0 {
				r.EventLoop.Send(int32(id), code, p)
			}
		}
	}
	r.core.reply = func(req Request, v state.Value) {
		// A recovered command may have no local reply stream yet. A retry
		// restores proposals, and core.values returns the original result.
		g := r.proposals[req.ID]
		if g != nil {
			r.client(req.ID, 0, &defs.ProposeReplyTS{OK: defs.TRUE, CommandId: req.ID.Sequence, Value: v, Timestamp: req.Timestamp}, false)
		}
		delete(r.proposals, req.ID)
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

func (p *protocolAdapter) Propose(g *defs.GPropose, now time.Time) error {
	if err := protocol.ValidateProposal(g); err != nil {
		return err
	}
	id := g.RequestID()
	p.replica.proposals[id] = g
	if now.IsZero() {
		now = time.Now()
	}
	p.replica.core.submit(Request{ID: id, Command: g.Command, Origin: p.replica.core.id, Timestamp: g.Timestamp}, now)
	return nil
}

func (p *protocolAdapter) Step(event any, now time.Time) error {
	message, ok := event.(*Packet)
	if !ok || message == nil {
		return protocol.UnsupportedEvent("pql", event)
	}
	if now.IsZero() {
		now = time.Now()
	}
	p.replica.core.receive(message, now)
	return nil
}

func (p *protocolAdapter) Tick(t protocol.Tick) error {
	if t.Kind != protocol.Maintenance {
		return protocol.UnsupportedTimer("pql", t.Kind)
	}
	if len(t.Alive) != p.replica.core.n {
		return fmt.Errorf("pql: connectivity snapshot size mismatch")
	}
	p.replica.core.tick(t.Now, t.Alive)
	return nil
}

func (p *protocolAdapter) Leader() int32 { return p.replica.core.leader }

func (p *protocolAdapter) Status() string {
	c := p.replica.core
	return fmt.Sprintf("PQL_STATS ballot=%d leader=%d active=%t preparing=%t accepted=%d executed=%d results=%d pending=%d local_reads=%d forwarded_reads=%d logged_reads=%d updating_keys=%d grant_epoch=%d read_epoch=%d config_epoch=%d config_accepted=%d config_executed=%d excluded=%x lease_active=%t promise_accepts=%d promise_rejects=%d lease_rejects=%d quorum_sends=%d recoveries=%d recovery_end=%d gap_accepts=%d fetch_requests=%d fetch_suppressed=%d replay_replies=%d", c.ballot, c.leader, c.active, c.preparing, c.high, c.executed, len(c.values), len(c.pending), c.localReads, c.forwardedReads, c.loggedReads, len(c.updating), c.lease.promisedBy, c.lease.promisedTo, c.latestEpoch, c.configLog.high, c.configLog.executed, c.policy.excluded, time.Now().Before(c.lease.readUntil), c.lease.promisesAccepted, c.lease.promisesRejected, c.leaseRejects, c.quorumSends, c.recoveries, c.recoveryEnd, c.gapAccepts, c.fetchRequests, c.fetchSuppressed, c.replayReplies)
}

// The master discovers the ballot owner; changing a flag cannot install a leader.
func (r *Replica) BeTheLeader(args *defs.BeTheLeaderArgs, reply *defs.BeTheLeaderReply) error {
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
