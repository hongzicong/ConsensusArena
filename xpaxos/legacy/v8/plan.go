package xpaxosv8

import (
	"fmt"
	"github.com/hongzicong/ConsensusArena/config"
	"github.com/hongzicong/ConsensusArena/placement"
	"math"
	"sort"
	"strings"
)

// PlanDeployment jointly chooses read authority and write coverage with leader.
func basePlanDeployment(c *config.Config, t *placement.Topology) (config.DeploymentPlan, error) {
	n := len(t.Replicas)
	if n < 3 || n > 13 || n%2 == 0 {
		return config.DeploymentPlan{}, fmt.Errorf("XPaxos v0 exhaustive placement requires odd 3..13 replicas")
	}
	writes := float64(c.Writes) / 100
	best, bestLeader, bestMask := math.Inf(1), 0, uint64(1)
	bestRead, bestWrite := 0.0, 0.0
	for leader := 0; leader < n; leader++ {
		distances := make([]float64, n)
		for r := 0; r < n; r++ {
			distances[r] = t.D[leader][r] + t.D[r][leader]
		}
		sorted := append([]float64(nil), distances...)
		sort.Float64s(sorted)
		quorum := sorted[n/2]
		for mask := uint64(1); mask < uint64(1)<<uint(n); mask++ {
			if mask&bit(leader) == 0 {
				continue
			}
			coverage := 0.0
			for r := 0; r < n; r++ {
				if mask&bit(r) != 0 {
					coverage = math.Max(coverage, distances[r])
				}
			}
			readCosts, writeCosts := make([]float64, len(t.Clients)), make([]float64, len(t.Clients))
			tail := 0.0
			for client := range t.Clients {
				readCosts[client] = math.Inf(1)
				for r := 0; r < n; r++ {
					if mask&bit(r) != 0 {
						readCosts[client] = math.Min(readCosts[client], t.RTT(client, r))
					}
				}
				writeCosts[client] = t.RTT(client, leader) + math.Max(quorum, coverage)
				if writes > 0 {
					tail = math.Max(tail, writeCosts[client])
				} else {
					tail = math.Max(tail, readCosts[client])
				}
			}
			rd, wr := t.Mean(readCosts), t.Mean(writeCosts)
			score := (1-writes)*rd + writes*wr + .5*tail
			if score < best {
				best, bestLeader, bestMask, bestRead, bestWrite = score, leader, mask, rd, wr
			}
		}
	}
	leader := t.Replicas[bestLeader]
	p := config.DeploymentPlan{Model: "joint-leader-responder-mean-plus-half-tail", Leader: &leader, ClientRoutes: map[string]string{}, FastMean: bestRead, SlowMean: bestWrite}
	for r, replica := range t.Replicas {
		if bestMask&bit(r) != 0 {
			p.Responders = append(p.Responders, replica.Alias)
		}
	}
	for client, alias := range t.Clients {
		nearest := -1
		for r := range t.Replicas {
			if bestMask&bit(r) != 0 && (nearest < 0 || t.RTT(client, r) < t.RTT(client, nearest)) {
				nearest = r
			}
		}
		p.ClientRoutes[alias] = t.Replicas[nearest].Alias
	}
	c.BodegaResponders = strings.Join(p.Responders, ",")
	c.BodegaResponderRanges = ""
	writeCosts := make([]float64, len(t.Clients))
	p.WriteClientRoutes = map[string]string{}
	for client, alias := range t.Clients {
		bestIngress, bestCost := bestLeader, math.Inf(1)
		for ingress := 0; ingress < n; ingress++ {
			arrivals := make([]float64, n)
			coverage := 0.0
			for voter := 0; voter < n; voter++ {
				arrivals[voter] = t.D[bestLeader][voter] + t.D[voter][ingress]
				if bestMask&bit(voter) != 0 {
					coverage = math.Max(coverage, arrivals[voter])
				}
			}
			sort.Float64s(arrivals)
			cost := t.To[client][ingress] + t.D[ingress][bestLeader] + math.Max(arrivals[n/2], coverage) + t.Back[client][ingress]
			if cost < bestCost {
				bestIngress, bestCost = ingress, cost
			}
		}
		p.WriteClientRoutes[alias] = t.Replicas[bestIngress].Alias
		writeCosts[client] = bestCost
	}
	p.SlowMean = t.Mean(writeCosts)
	p.Model = "covered-quorum-origin-learners"
	planCompletionDelegation(c, t, &p, bestLeader, bestMask, false)
	return p, nil
}
