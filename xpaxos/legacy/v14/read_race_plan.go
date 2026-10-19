package xpaxosv14

import (
	"github.com/hongzicong/ConsensusArena/config"
	"github.com/hongzicong/ConsensusArena/placement"
	"github.com/hongzicong/ConsensusArena/xpaxos/heatpolicy"
	"math"
	"sort"
)

// Delivery geometry and correlated invalidation proxy, never read authority.
func parentColdPlan(c *config.Config, t *placement.Topology) (config.DeploymentPlan, error) {
	p, err := parentPlanDeploymentLocal(c, t)
	if err != nil || c.Writes <= 0 || c.KeyCount <= 0 || c.UniqueKeys {
		return p, err
	}
	n := len(t.Replicas)
	l := p.Leader.Rank
	roles := []int{}
	rank := map[string]int{}
	for r, v := range t.Replicas {
		rank[v.Alias] = r
		for _, a := range p.Responders {
			if a == v.Alias {
				roles = append(roles, r)
			}
		}
	}
	e := make([]float64, n)
	win := make([]float64, n)
	for r := 0; r < n; r++ {
		a := make([]float64, n)
		cover := 0.0
		for v := 0; v < n; v++ {
			a[v] = t.D[l][v] + t.D[v][r]
		}
		for _, v := range roles {
			cover = max(cover, a[v])
		}
		sort.Float64s(a)
		e[r] = max(a[n/2], cover)
		win[r] = max(0, e[r]-t.D[l][r])
	}
	prob := c.HeatProfile.Probabilities
	offered := c.ArrivalRate * float64(len(t.Clients)) * float64(max(1, c.Clones)) * float64(c.Writes) / 100
	remote := 0.0
	for _, alias := range t.Clients {
		if rank[p.WriteClientRoutes[alias]] != l {
			remote++
		}
	}
	remote /= float64(len(t.Clients))
	p.ReadRaceRanges = map[string][]config.PlannedReadRaceRange{}
	for client, alias := range t.Clients {
		primary := rank[p.ClientRoutes[alias]]
		baseRTT := t.RTT(client, primary)
		for key := 0; key < heatpolicy.HotCutoff(c); key++ {
			owner := -1
			for _, span := range p.CompletionRanges {
				if int64(key) >= span.Start && int64(key) < span.End {
					owner = span.Owner.Rank
					break
				}
			}
			scale := func(r int) float64 {
				if owner >= 0 {
					if r == owner {
						return 0
					}
					return 1
				}
				if r == l {
					return remote
				}
				return 1
			}
			rate := offered * prob[key] / 1000
			best, target := .25, primary
			for _, secondary := range roles {
				if secondary == primary {
					continue
				}
				gain := 0.0
				for i := 0; i < 256; i++ {
					x := baseRTT + (float64(i)+.5)/256*win[primary]
					if x < t.RTT(client, secondary) {
						continue
					}
					a := max(0, win[primary]+baseRTT-x)
					b := max(0, win[secondary]+t.RTT(client, secondary)-x)
					overlap := max(0, min(t.To[client][primary]-t.D[l][primary], t.To[client][secondary]-t.D[l][secondary])-max(x-t.Back[client][primary]-e[primary], x-t.Back[client][secondary]-e[secondary]))
					union := scale(primary)*a + scale(secondary)*b - min(scale(primary), scale(secondary))*overlap
					gain += max(0, math.Exp(-rate*scale(secondary)*b)-math.Exp(-rate*max(0, union))) * win[primary] / 256
				}
				if gain > best+1e-10 {
					best, target = gain, secondary
				}
			}
			if target == primary {
				continue
			}
			spans := p.ReadRaceRanges[alias]
			if len(spans) > 0 && spans[len(spans)-1].End == int64(key) && spans[len(spans)-1].Target == target {
				spans[len(spans)-1].End++
			} else {
				spans = append(spans, config.PlannedReadRaceRange{Start: int64(key), End: int64(key + 1), Target: target})
			}
			p.ReadRaceRanges[alias] = spans
		}
	}
	p.Model = "cold-client-learning-hot-covered-read-racing"
	return p, nil
}
