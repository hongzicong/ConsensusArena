package agreement

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
	cold           *coldReplica
	coldCfg        config.DeploymentPlan
	learnerStreams map[int32][]*defs.GPropose
	ingressSince   map[defs.RequestID]time.Time
	*replica.Replica
	control       chan leaderCall
	rosterQueries chan chan RosterReply
	engine        *engine
	readPaths     replica.ReadPathMetrics
	waiting       map[defs.RequestID][]*defs.GPropose
}

func readOptions(c *config.Config, n int) (options, error) {
	o := options{KeyPolicy: newKeyPolicy(c), AllKeyLearning: c.Plan.ClientLearningAllKeys, ClientLearning: c.Writes > 0, Lease: 2500 * time.Millisecond, Margin: 100 * time.Millisecond, Heartbeat: 120 * time.Millisecond, Failure: 1200 * time.Millisecond, FailureMax: 2400 * time.Millisecond}
	o.HotDisabled = c.Plan.AgreementHotDisabled
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
	var err error
	o.Responders, err = responderMask(names, c, n)
	if err != nil {
		return o, err
	}
	o.Ranges, err = responderRanges(c.BodegaResponderRanges, c, n)
	if err != nil {
		return o, err
	}
	if len(c.Plan.WriteClientRoutes) == 0 {
		o.Learners = (uint64(1) << uint(n)) - 1
	}
	for _, alias := range c.Plan.WriteClientRoutes {
		found := false
		for id, name := range c.ReplicaAliases {
			if name == alias && id >= 0 && id < n {
				o.Learners |= bit(id)
				found = true
				break
			}
		}
		if !found {
			return o, fmt.Errorf("unknown planned learner alias %q", alias)
		}
	}
	previousEnd := int64(-1)
	for _, span := range c.Plan.CompletionRanges {
		owner := span.Owner.Rank
		if span.Start < 0 || span.End <= span.Start || span.Start < previousEnd || owner < 0 || owner >= n || c.ReplicaAliases[owner] != span.Owner.Alias || o.Responders&bit(owner) == 0 {
			return o, fmt.Errorf("invalid immutable completion range")
		}
		o.Completion = append(o.Completion, completionSpan{Start: span.Start, End: span.End, Owner: owner})
		o.Learners |= bit(owner)
		previousEnd = span.End
	}
	return o, err
}

func New(alias string, id int, addrs []string, isLeader bool, c *config.Config, l *dlog.Logger) *Replica {
	opt, err := readOptions(c, len(addrs))
	if err != nil {
		panic(err)
	}
	r := &Replica{Replica: replica.New(alias, id, (len(addrs)-1)/2, addrs, true, c, l), control: make(chan leaderCall, 8)}
	r.ProposalPolicy.Operations = proposalOperations()
	r.coldCfg = c.Plan
	r.rosterQueries = make(chan chan RosterReply, 8)
	inbox := make(chan rpc.Serializable, 8192)
	code := r.RPC.Register(&message{}, inbox)
	go r.run(opt, isLeader, code, inbox)
	return r
}

// Admit XPaxos's existing client envelopes before the shared receiver dispatches
// them to Propose. Internal bridge operations remain peer-only.
func proposalOperations() []state.Operation {
	return []state.Operation{
		state.NONE,
		state.PUT,
		state.GET,
		state.SCAN,
		cancelRead,
		coldProof,
		coldPUT,
		registerLearner,
		directPUT,
	}
}
