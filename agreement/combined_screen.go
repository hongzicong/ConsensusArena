package agreement

import (
	"encoding/json"
	"fmt"
	"github.com/hongzicong/ConsensusArena/config"
	"github.com/hongzicong/ConsensusArena/placement"
	"math"
	"os"
)

// Startup-only geometry approximation. It grants no protocol authority.
func screenCDF(m *coldPlanModel, p config.DeploymentPlan, x float64) float64 {
	f := m.cdf(x)
	if p.ColdEnabled {
		f += m.delta(m.candidate(p.ColdQuorum), int(p.ColdCutoff), x)
	}
	return min(1., max(0., f))
}
func screenMean(m *coldPlanModel, p config.DeploymentPlan) float64 {
	upper := screenUpper(m, p)
	steps := min(40000, max(1, int(math.Ceil(upper/.25))))
	step := upper / float64(steps)
	total := 0.
	for i := 0; i < steps; i++ {
		total += (1 - screenCDF(m, p, (float64(i)+.5)*step)) * step
	}
	return total
}
func screenTail(m *coldPlanModel, p config.DeploymentPlan, q float64) float64 {
	lo, hi := 0., screenUpper(m, p)
	for i := 0; i < 35; i++ {
		mid := (lo + hi) / 2
		if screenCDF(m, p, mid) >= q {
			hi = mid
		} else {
			lo = mid
		}
	}
	return hi
}

// Bound the actual modeled support, including cold collision recovery. Fixed
// 1s/10s search horizons silently turn slower plans into equal-cost candidates.
func screenUpper(m *coldPlanModel, p config.DeploymentPlan) float64 {
	upper := 1.
	for _, b := range m.Bins {
		for ci, r := range m.Readers {
			upper = max(upper, b.Write[ci])
			for _, reader := range []int{r, b.Targets[ci]} {
				hold := m.Visibility.holdProfile(b.Scenarios, reader).upper()
				upper = max(upper, m.T.RTT(ci, reader)+hold)
			}
		}
	}
	if p.ColdEnabled {
		upper = max(upper, coldCandidateUpper(m.candidate(p.ColdQuorum)))
	}
	return upper
}

func coldCandidateUpper(c coldPlanCandidate) float64 {
	write, network := 0., 0.
	for i, v := range c.Times {
		write = max(write, v+c.Extra)
		network = max(network, c.ReadNet[i])
	}
	return write + network
}
func importedPlanDeployment(c *config.Config, t *placement.Topology) (config.DeploymentPlan, error) {
	beforeConfig := *c
	before, err := baselineHotPlan(&beforeConfig, t)
	if err != nil {
		return before, err
	}
	before = planColdPlacement(&beforeConfig, t, before)
	afterConfig := *c
	after, err := combinedScreenPlan(&afterConfig, t)
	if err != nil {
		return before, err
	}
	row := map[string]any{"changed": after.ClientLearningAllKeys, "parent": before, "candidate": after, "scope": "XPaxos owner-first ACCEPTED geometry including non-owner read holds, cold delta, all-key read fences and hot races; no processing/batching/jitter or empirical p99 guarantee"}
	if after.ClientLearningAllKeys {
		bm := newColdPlanModel(c, t, before)
		am := newColdPlanModel(c, t, after)
		b, a := screenMean(bm, before), screenMean(am, after)
		row["parent_mean_ms"] = b
		row["candidate_mean_ms"] = a
		row["mean_gain_ms"] = b - a
		tails := []map[string]float64{}
		for _, q := range []float64{.9898, .99, .9902} {
			bt, at := screenTail(bm, before, q), screenTail(am, after, q)
			tails = append(tails, map[string]float64{"q": q, "parent": bt, "candidate": at, "gain": bt - at})
		}
		row["tails"] = tails
	}
	selected := false
	if after.ClientLearningAllKeys {
		gain := row["mean_gain_ms"].(float64)
		worst := math.Inf(1)
		for _, v := range row["tails"].([]map[string]float64) {
			worst = min(worst, v["gain"])
		}
		selected = gain > .05 && worst >= -5*gain
	}
	row["selected"] = selected
	data, _ := json.Marshal(row)
	fmt.Fprintln(os.Stderr, "COMBINED_SELECTION "+string(data))
	if selected {
		c.BodegaResponders = afterConfig.BodegaResponders
		c.BodegaResponderRanges = afterConfig.BodegaResponderRanges
		after.Model = "joint-hot-client-certificates-cold-placement-with-prefix-handoff"
		return after, nil
	}
	c.BodegaResponders = beforeConfig.BodegaResponders
	c.BodegaResponderRanges = beforeConfig.BodegaResponderRanges
	return before, nil
}
