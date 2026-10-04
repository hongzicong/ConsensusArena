package bodega

// Replica state, configuration, RPC registration, and startup.
import (
	"fmt"
	"strings"
	"time"

	"github.com/hongzicong/ConsensusArena/config"
	"github.com/hongzicong/ConsensusArena/dlog"
	"github.com/hongzicong/ConsensusArena/replica"
	"github.com/hongzicong/ConsensusArena/replica/defs"
	"github.com/hongzicong/ConsensusArena/rpc"
	"github.com/hongzicong/ConsensusArena/state"
)

type Replica struct {
	*replica.Replica
	control       chan leaderCall
	rosterQueries chan chan defs.BodegaRosterReply
	engine        *engine
	waiting       map[defs.RequestID]*defs.GPropose
}

func readOptions(c *config.Config, n int) (options, error) {
	o := options{Lease: 2500 * time.Millisecond, Margin: 100 * time.Millisecond, Heartbeat: 120 * time.Millisecond, Failure: 1200 * time.Millisecond, FailureMax: 2400 * time.Millisecond}
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
		o.FailureMax = c.BodegaFailure // Preserve the legacy fixed-timeout override.
	}
	if c.BodegaFailureMax != 0 {
		o.FailureMax = c.BodegaFailureMax
	}
	if o.Heartbeat <= 0 || o.Failure <= 2*o.Heartbeat || o.FailureMax < o.Failure || o.Lease <= o.FailureMax || o.Margin <= 0 {
		return o, fmt.Errorf("Bodega requires 0 < 2*heartbeat < failure <= failureMax < lease, positive margin")
	}
	names := strings.TrimSpace(c.BodegaResponders)
	if names == "" {
		names = "all"
	}
	members, err := c.Membership()
	if err != nil {
		return o, err
	}
	o.Responders, err = responderMask(names, members, n)
	if err != nil {
		return o, err
	}
	o.Ranges, err = responderRanges(c.BodegaResponderRanges, members, n)
	return o, err
}

func New(alias string, id int, addrs []string, isLeader bool, c *config.Config, l *dlog.Logger) *Replica {
	opt, err := readOptions(c, len(addrs))
	if err != nil {
		panic(err)
	}
	r := &Replica{Replica: replica.New(alias, id, (len(addrs)-1)/2, addrs, false, true, false, c, l), control: make(chan leaderCall, 8)}
	r.ProposalPolicy.Operations = []state.Operation{state.NONE, state.PUT, state.GET, state.SCAN, defs.BodegaCancelRead}
	r.rosterQueries = make(chan chan defs.BodegaRosterReply, 8)
	inbox := make(chan rpc.Serializable, 8192)
	code := r.RPC.Register(&message{}, inbox)
	go r.run(opt, isLeader, code, inbox)
	return r
}
