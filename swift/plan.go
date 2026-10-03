package swift

import (
	"github.com/hongzicong/ConsensusArena/config"
	"github.com/hongzicong/ConsensusArena/placement"
	"slices"
)

// PlanDeployment selects the initial C2 leader/fixed majority. Recovery can move
// to other ballots and quorums through the existing quorum system.
func PlanDeployment(c *config.Config, t *placement.Topology) (config.DeploymentPlan, error) {
	// Preserve explicit proxy routes, preferring a newly available co-located
	// replica. This placement rule previously lived in prepare-topology.sh.
	routes := t.NearestRoutes()
	for i, a := range t.Clients {
		if c.Proxy != nil {
			if route := c.Proxy.ProxyOf(c.ClientAddrs[a]); route != config.REMOTE {
				routes[a] = route
			}
		}
		for j, r := range t.Replicas {
			if r.Alias == a && t.RTT(i, j) == 0 {
				routes[a] = r.Alias
				break
			}
		}
	}
	p := config.DeploymentPlan{Model: "swift-c2-leader-fixed-majority", ClientRoutes: routes}
	key := ""
	n := len(t.Replicas)
	f := n / 2
	for l := range t.Replicas {
		slow := make([]float64, len(t.Clients))
		for c := range slow {
			acks := []float64{}
			for r := range t.Replicas {
				if r != l {
					acks = append(acks, placement.Max(t.To[c][r], t.To[c][l]+t.D[l][r])+t.Back[c][r])
				}
			}
			slow[c] = placement.Max(t.RTT(c, l), placement.Kth(acks, f-1))
		}
		sm := t.Mean(slow)
		placement.Subsets(n, f+1, func(q []int) {
			if !slices.Contains(q, l) {
				return
			}
			fast := make([]float64, len(t.Clients))
			for c := range fast {
				for _, r := range q {
					fast[c] = placement.Max(fast[c], t.RTT(c, r))
				}
				fast[c] = placement.Min(fast[c], slow[c])
			}
			fm, k := t.Mean(fast), t.Key(l, q)
			if t.Better(fm, sm, k, p, key) {
				leader := t.Replicas[l]
				p.Leader = &leader
				p.FastQuorum = t.Members(q)
				p.FastMean = fm
				p.SlowMean = sm
				key = k
			}
		})
	}
	return p, nil
}
