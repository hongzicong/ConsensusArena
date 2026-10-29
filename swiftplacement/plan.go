// Package swiftplacement bounds startup search for large SwiftPaxos deployments.
// Runtime quorum, message and recovery rules remain in the original package.
package swiftplacement

import (
	"sort"

	"github.com/hongzicong/ConsensusArena/config"
	"github.com/hongzicong/ConsensusArena/placement"
	"github.com/hongzicong/ConsensusArena/swift"
)

func PlanDeployment(c *config.Config, t *placement.Topology) (config.DeploymentPlan, error) {
	n := len(t.Replicas)
	if n <= 17 {
		return swift.PlanDeployment(c, t)
	}
	routes := t.NearestRoutes()
	for _, alias := range t.Clients {
		if c.Proxy != nil {
			if route := c.Proxy.ProxyOf(c.ClientAddrs[alias]); route != config.REMOTE {
				routes[alias] = route
			}
		}
	}
	p := config.DeploymentPlan{Model: "swift-c2-bounded-majorities-above-17", ClientRoutes: routes}
	bestKey := ""
	for leader := range t.Replicas {
		slow := make([]float64, len(t.Clients))
		for client := range slow {
			acks := []float64{}
			for r := range t.Replicas {
				if r != leader {
					acks = append(acks, placement.Max(t.To[client][r], t.To[client][leader]+t.D[leader][r])+t.Back[client][r])
				}
			}
			slow[client] = placement.Max(t.RTT(client, leader), placement.Kth(acks, n/2-1))
		}
		slowMean := t.Mean(slow)
		// Explore each client/replica anchor plus aggregate client distance.
		for anchor := -1; anchor < n+len(t.Clients); anchor++ {
			cost := func(r int) float64 {
				if anchor < 0 {
					values := make([]float64, len(t.Clients))
					for client := range values {
						values[client] = t.RTT(client, r)
					}
					return t.Mean(values)
				}
				if anchor < n {
					return t.D[anchor][r] + t.D[r][anchor]
				}
				return t.RTT(anchor-n, r)
			}
			peers := []int{}
			for r := range t.Replicas {
				if r != leader {
					peers = append(peers, r)
				}
			}
			sort.Slice(peers, func(i, j int) bool {
				a, b := cost(peers[i]), cost(peers[j])
				return a < b || (a == b && peers[i] < peers[j])
			})
			q := append([]int{leader}, peers[:n/2]...)
			sort.Ints(q)
			fast := make([]float64, len(t.Clients))
			for client := range fast {
				for _, r := range q {
					fast[client] = placement.Max(fast[client], t.RTT(client, r))
				}
				fast[client] = placement.Min(fast[client], slow[client])
			}
			mean, key := t.Mean(fast), t.Key(leader, q)
			if t.Better(mean, slowMean, key, p, bestKey) {
				l := t.Replicas[leader]
				p.Leader, p.FastQuorum, p.FastMean, p.SlowMean = &l, t.Members(q), mean, slowMean
				bestKey = key
			}
		}
	}
	return p, nil
}
