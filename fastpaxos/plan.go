package fastpaxos

import (
	"github.com/hongzicong/ConsensusArena/config"
	"github.com/hongzicong/ConsensusArena/placement"
	"math"
)

// PlanDeployment preserves this implementation's fixed fast set of f+1 voters.
func PlanDeployment(_ *config.Config, t *placement.Topology) (config.DeploymentPlan, error) {
	p := config.DeploymentPlan{Model: "fixed-fast-set", ClientRoutes: t.NearestRoutes()}
	key := ""
	n := len(t.Replicas)
	placement.Subsets(n, n/2+1, func(q []int) {
		fast, slow := make([]float64, len(t.Clients)), make([]float64, len(t.Clients))
		for c := range fast {
			first := make([]float64, n)
			fast[c] = math.Inf(1)
			slow[c] = math.Inf(1)
			for j := range first {
				first[j] = t.To[c][j]
				for _, r := range q {
					first[j] = placement.Max(first[j], t.To[c][r]+t.D[r][j])
				}
				fast[c] = placement.Min(fast[c], first[j]+t.Back[c][j])
			}
			for j := range first {
				second := t.To[c][j]
				for _, r := range q {
					second = placement.Max(second, first[r]+t.D[r][j])
				}
				slow[c] = placement.Min(slow[c], second+t.Back[c][j])
			}
		}
		fm, sm, k := t.Mean(fast), t.Mean(slow), t.Key(-1, q)
		if t.Better(fm, sm, k, p, key) {
			p.FastQuorum = t.Members(q)
			p.FastMean = fm
			p.SlowMean = sm
			key = k
		}
	})
	return p, nil
}
