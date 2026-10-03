package curp

import (
	"github.com/hongzicong/ConsensusArena/config"
	"github.com/hongzicong/ConsensusArena/placement"
	"math"
)

// PlanDeployment chooses only a leader; witnesses and slow majorities remain dynamic.
func PlanDeployment(_ *config.Config, t *placement.Topology) (config.DeploymentPlan, error) {
	p := config.DeploymentPlan{Model: "witness-fast-and-commit-relay", ClientRoutes: t.NearestRoutes()}
	key := ""
	n := len(t.Replicas)
	f := n / 2
	for l := range t.Replicas {
		peers := []float64{}
		for r := range t.Replicas {
			if r != l {
				peers = append(peers, t.D[l][r]+t.D[r][l])
			}
		}
		majority := placement.Kth(peers, f-1)
		fast, slow := make([]float64, len(t.Clients)), make([]float64, len(t.Clients))
		for c := range fast {
			slow[c] = math.Inf(1)
			for j := range t.Replicas {
				acks := make([]float64, n)
				for r := range acks {
					acks[r] = t.To[c][l] + t.D[l][r] + t.D[r][j]
				}
				quorum := placement.Min(placement.Kth(acks, f), t.To[c][l]+majority+t.D[l][j])
				slow[c] = placement.Min(slow[c], placement.Max(t.To[c][j], t.To[c][l]+t.D[l][j], quorum)+t.Back[c][j])
			}
			witnesses := []float64{}
			for r := range t.Replicas {
				if r != l {
					witnesses = append(witnesses, t.RTT(c, r))
				}
			}
			fast[c] = placement.Min(slow[c], placement.Max(t.RTT(c, l), placement.Kth(witnesses, f+(f+1)/2-1)))
		}
		fm, sm, k := t.Mean(fast), t.Mean(slow), t.Key(l, nil)
		if t.Better(fm, sm, k, p, key) {
			leader := t.Replicas[l]
			p.Leader = &leader
			p.FastMean = fm
			p.SlowMean = sm
			key = k
		}
	}
	return p, nil
}
