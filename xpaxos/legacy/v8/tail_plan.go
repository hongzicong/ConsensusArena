package xpaxosv8

import (
	"github.com/hongzicong/ConsensusArena/config"
	"github.com/hongzicong/ConsensusArena/placement"
	parentv6 "github.com/hongzicong/ConsensusArena/xpaxos/legacy/v6"
	"math"
	"sort"
	"strings"
)

type placementCandidate struct {
	leader      int
	mask        uint64
	networkMean float64
}

func coupledMean(m *completionModel, p config.DeploymentPlan) float64 {
	mean := 0.0
	for c, r := range m.readers {
		mean += (1 - m.writes) * m.readNetwork[c] / float64(len(m.readers))
		mean += m.writes * m.writeCosts[c][m.ingresses[c]] / float64(len(m.readers))
		_ = r
	}
	for r, weight := range m.weights {
		if weight == 0 {
			continue
		}
		scale := m.parentScale(r)
		for _, prob := range m.prob {
			mean += (1 - m.writes) * weight * prob * residualHold(m.windows[r], m.offeredWrites*prob*scale)
		}
	}
	return mean - p.CompletionModelMeanSaving
}

// geometry computes original-voter evidence at each learner, never a leader-only
// return approximation. Its output is immutable startup metadata only.
func geometry(c *config.Config, t *placement.Topology, l int, mask uint64) (config.DeploymentPlan, float64, float64) {
	n := len(t.Replicas)
	w := float64(c.Writes) / 100
	evidence := make([]float64, n)
	for ingress := 0; ingress < n; ingress++ {
		arrivals := make([]float64, n)
		coverage := 0.0
		for voter := 0; voter < n; voter++ {
			arrivals[voter] = t.D[l][voter] + t.D[voter][ingress]
			if mask&bit(voter) != 0 {
				coverage = max(coverage, arrivals[voter])
			}
		}
		sort.Float64s(arrivals)
		evidence[ingress] = max(arrivals[n/2], coverage)
	}
	leader := t.Replicas[l]
	p := config.DeploymentPlan{Leader: &leader, ClientRoutes: map[string]string{}, WriteClientRoutes: map[string]string{}}
	for r, v := range t.Replicas {
		if mask&bit(r) != 0 {
			p.Responders = append(p.Responders, v.Alias)
		}
	}
	rd, wr := make([]float64, len(t.Clients)), make([]float64, len(t.Clients))
	tail := 0.0
	for client, alias := range t.Clients {
		reader, ingress := -1, 0
		rc, wc := math.Inf(1), math.Inf(1)
		for r := 0; r < n; r++ {
			if mask&bit(r) != 0 && t.RTT(client, r) < rc {
				reader, rc = r, t.RTT(client, r)
			}
			cost := admissionCost(t, client, r, l, evidence[r])
			if cost < wc {
				ingress, wc = r, cost
			}
		}
		p.ClientRoutes[alias] = t.Replicas[reader].Alias
		p.WriteClientRoutes[alias] = t.Replicas[ingress].Alias
		rd[client], wr[client] = rc, wc
		tail = max(tail, wc)
	}
	p.FastMean, p.SlowMean = t.Mean(rd), t.Mean(wr)
	return p, (1-w)*p.FastMean + w*p.SlowMean, tail
}

// Charge 1ms new direct admission overhead; preserve the home fallback.
func admissionCost(t *placement.Topology, c, h, l int, evidence float64) float64 {
	start := t.To[c][h] + t.D[h][l]
	if h != l {
		start = min(start, t.To[c][l]+1)
	}
	return max(t.To[c][h], start+evidence) + t.Back[c][h]
}

func parentPlanDeploymentLocal(c *config.Config, t *placement.Topology) (config.DeploymentPlan, error) {
	parent, err := parentv6.PlanDeployment(c, t)
	if err != nil || c.Writes <= 0 {
		return parent, err
	}
	n := len(t.Replicas)
	l := parent.Leader.Rank
	mask := uint64(0)
	for r, v := range t.Replicas {
		for _, alias := range parent.Responders {
			if alias == v.Alias {
				mask |= bit(r)
			}
		}
	}
	baseline := parent
	model := planCompletionDelegation(c, t, &baseline, l, mask, false)
	if model == nil {
		return parent, nil
	}
	pm := coupledMean(model, baseline)
	pd := len(baseline.CompletionRanges) > 0
	qs := []float64{.9898, .99, .9902}
	pb := make([]float64, 3)
	for i, q := range qs {
		pb[i] = model.quantile(pd, q)
	}
	cap := 0.0
	for c, h := range model.ingresses {
		cap = max(cap, model.writeCosts[c][h])
	}
	shortlist := []placementCandidate{}
	for leader := 0; leader < n; leader++ {
		local := []placementCandidate{}
		for cm := uint64(1); cm < uint64(1)<<uint(n); cm++ {
			if cm&bit(leader) == 0 {
				continue
			}
			_, mean, tail := geometry(c, t, leader, cm)
			if tail > cap+.02 {
				continue
			}
			local = append(local, placementCandidate{leader, cm, mean})
		}
		sort.SliceStable(local, func(i, j int) bool { return local[i].networkMean < local[j].networkMean })
		if len(local) > 12 {
			local = local[:12]
		}
		shortlist = append(shortlist, local...)
	}
	sort.SliceStable(shortlist, func(i, j int) bool { return shortlist[i].networkMean < shortlist[j].networkMean })
	if len(shortlist) > 24 {
		shortlist = shortlist[:24]
	}
	// Include the parent roster with its new admission-optimal homes explicitly.
	shortlist = append(shortlist, placementCandidate{l, mask, 0})
	chosen := parent
	bestMean, bestP99 := pm, pb[1]
	changed := false
	for _, candidate := range shortlist {
		p, _, _ := geometry(c, t, candidate.leader, candidate.mask)
		m := planCompletionDelegation(c, t, &p, candidate.leader, candidate.mask, true)
		if m == nil {
			continue
		}
		mean := coupledMean(m, p)
		if mean > pm+.02 {
			continue
		}
		delegated := len(p.CompletionRanges) > 0
		band := make([]float64, 3)
		admit := true
		for i, q := range qs {
			band[i] = m.quantile(delegated, q)
			if band[i] > pb[i]+.02 {
				admit = false
			}
		}
		if admit && (band[1] < bestP99-.02 || (math.Abs(band[1]-bestP99) <= .02 && mean < bestMean-.02)) {
			chosen, bestMean, bestP99 = p, mean, band[1]
			changed = true
		}
	}
	chosen.Model = "direct-admission-immutable-home-placement"
	chosen.PlacementParentMean, chosen.PlacementSelectedMean = pm, bestMean
	chosen.PlacementParentP99, chosen.PlacementSelectedP99 = pb[1], bestP99
	chosen.PlacementShortlist, chosen.PlacementChanged = len(shortlist), changed
	c.BodegaResponders = strings.Join(chosen.Responders, ",")
	c.BodegaResponderRanges = ""
	return chosen, nil
}
