package paxos

import (
	"github.com/hongzicong/ConsensusArena/config"
	"github.com/hongzicong/ConsensusArena/placement"
)

// PlanDeployment chooses the initial leader. Phase two still accepts any majority.
func PlanDeployment(_ *config.Config, t *placement.Topology) (config.DeploymentPlan, error) {
	p := config.DeploymentPlan{Model: "leader-roundtrip-dynamic-majority", ClientRoutes: t.NearestRoutes()}
	key := ""
	for l := range t.Replicas {
		peers := []float64{}
		for r := range t.Replicas {
			if r != l {
				peers = append(peers, t.D[l][r]+t.D[r][l])
			}
		}
		majority := placement.Kth(peers, len(t.Replicas)/2-1)
		cost := make([]float64, len(t.Clients))
		for c := range cost {
			cost[c] = t.RTT(c, l) + majority
		}
		mean, k := t.Mean(cost), t.Key(l, nil)
		if t.Better(mean, mean, k, p, key) {
			leader := t.Replicas[l]
			p.Leader = &leader
			p.FastMean = mean
			p.SlowMean = mean
			key = k
		}
	}
	return p, nil
}
