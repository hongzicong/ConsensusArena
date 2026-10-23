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

// Replica owns runtime resources and exposes the external protocol event boundary.
type Replica struct {
	*replica.Replica
	control       chan leaderCall
	rosterQueries chan chan BodegaRosterReply
	core          *core
	proposals     map[defs.RequestID]*defs.GPropose
}

func readOptions(conf *config.Config, n int) (options, error) {
	o := options{lease: 2500 * time.Millisecond, margin: 100 * time.Millisecond, heartbeat: 120 * time.Millisecond, failure: 1200 * time.Millisecond, failureMax: 2400 * time.Millisecond}
	if n < 3 || n > 63 || n%2 == 0 {
		return o, fmt.Errorf("Bodega requires odd membership of 3..63 replicas")
	}
	if conf.Noop {
		return o, fmt.Errorf("Bodega requires state-machine execution (noop: false)")
	}
	if conf.BodegaLease != 0 {
		o.lease = conf.BodegaLease
	}
	if conf.BodegaMargin != 0 {
		o.margin = conf.BodegaMargin
	}
	if conf.BodegaHeartbeat != 0 {
		o.heartbeat = conf.BodegaHeartbeat
	}
	if conf.BodegaFailure != 0 {
		o.failure = conf.BodegaFailure
		o.failureMax = conf.BodegaFailure // Preserve the legacy fixed-timeout override.
	}
	if conf.BodegaFailureMax != 0 {
		o.failureMax = conf.BodegaFailureMax
	}
	if o.heartbeat <= 0 || o.failure <= 2*o.heartbeat || o.failureMax < o.failure || o.lease <= o.failureMax || o.margin <= 0 {
		return o, fmt.Errorf("Bodega requires 0 < 2*heartbeat < failure <= failureMax < lease, positive margin")
	}
	names := strings.TrimSpace(conf.BodegaResponders)
	if names == "" {
		names = "all"
	}
	members, err := conf.Membership()
	if err != nil {
		return o, err
	}
	o.responders, err = responderMask(names, members, n)
	if err != nil {
		return o, err
	}
	o.ranges, err = responderRanges(conf.BodegaResponderRanges, members, n)
	return o, err
}

func New(alias string, replicaID int, addrs []string, isLeader bool, conf *config.Config, logger *dlog.Logger) *Replica {
	opt, err := readOptions(conf, len(addrs))
	if err != nil {
		panic(err)
	}
	r := &Replica{Replica: replica.New(alias, replicaID, (len(addrs)-1)/2, addrs, true, conf, logger), control: make(chan leaderCall, 8)}
	r.core = newCore(replicaID, len(addrs), opt)
	r.proposals = make(map[defs.RequestID]*defs.GPropose)
	r.ProposalPolicy.Operations = []state.Operation{state.NONE, state.PUT, state.GET, state.SCAN, BodegaCancelRead}
	r.rosterQueries = make(chan chan BodegaRosterReply, 8)
	inbox := make(chan rpc.Serializable, 8192)
	code := r.RPC.Register(&message{}, inbox)
	go r.run(opt, isLeader, code, inbox)
	return r
}
