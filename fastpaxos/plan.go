package fastpaxos

import (
	"math"

	"github.com/hongzicong/ConsensusArena/config"
	"github.com/hongzicong/ConsensusArena/placement"
)

// PlanDeployment enables every acceptor. The fast quorum is a cardinality
// threshold, not a topology-selected fixed voter set.
func PlanDeployment(_ *config.Config, t *placement.Topology) (config.DeploymentPlan, error) {
	n := len(t.Replicas)
	q := fastQuorumSize(n)
	p := config.DeploymentPlan{
		Model:          "any-three-quarter-fast-quorum",
		FastQuorumSize: q,
		ClientRoutes:   t.NearestRoutes(),
	}
	fast := make([]float64, len(t.Clients))
	for c := range fast {
		fast[c] = math.Inf(1)
		for learner := 0; learner < n; learner++ {
			votes := make([]float64, n)
			for voter := range votes {
				votes[voter] = t.To[c][voter] + t.D[voter][learner]
			}
			// Learning can precede this learner's own payload delivery. The
			// executed result requires both, plus the final client reply hop.
			ready := placement.Max(t.To[c][learner], placement.Kth(votes, q-1))
			fast[c] = placement.Min(fast[c], ready+t.Back[c][learner])
		}
	}
	p.FastMean = t.Mean(fast)
	// No fixed collision-round estimate: fallback uses a selected classic
	// coordinator and Prepare, whose timing depends on detection and evidence.
	return p, nil
}
