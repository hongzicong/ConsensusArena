package agreement

import (
	"github.com/hongzicong/ConsensusArena/agreement/heatpolicy"
	"github.com/hongzicong/ConsensusArena/config"
	"github.com/hongzicong/ConsensusArena/placement"
	"math"
)

// Delivery geometry and correlated invalidation proxy, never read authority.
func planJointReadRaces(c *config.Config, t *placement.Topology, p config.DeploymentPlan) config.DeploymentPlan {
	if hotAblated(c) || c.Writes <= 0 || c.KeyCount <= 0 || c.UniqueKeys {
		return p
	}
	mask := uint64(0)
	l := p.Leader.Rank
	roles := []int{}
	rank := map[string]int{}
	for r, v := range t.Replicas {
		rank[v.Alias] = r
		for _, a := range p.Responders {
			if a == v.Alias {
				roles = append(roles, r)
				mask |= bit(r)
			}
		}
	}
	g := newVisibilityGeometry(t, l, mask)
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
			scenarios := g.scenarios(owner, remote, p.ClientLearningAllKeys)
			profile := g.holdProfile(scenarios, primary)
			window := profile.upper()
			rate := offered * prob[key] / 1000
			best, target := .25, primary
			for _, secondary := range roles {
				if secondary == primary {
					continue
				}
				gain := 0.0
				for i := 0; i < 256; i++ {
					x := baseRTT + (float64(i)+.5)/256*window
					if x < t.RTT(client, secondary) {
						continue
					}
					gain += max(0, g.raceCDF(scenarios, client, primary, secondary, rate, x)-math.Exp(-rate*profile.residual(x-baseRTT))) * window / 256
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
	return p
}
