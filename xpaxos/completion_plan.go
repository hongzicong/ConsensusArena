package xpaxos

import (
	"math"
	"sort"

	"github.com/hongzicong/ConsensusArena/config"
	"github.com/hongzicong/ConsensusArena/placement"
	"github.com/hongzicong/ConsensusArena/xpaxos/heatpolicy"
)

func min[T ~int | ~float64](a, b T) T {
	if a < b {
		return a
	}
	return b
}
func max[T ~int | ~float64](a, b T) T {
	if a > b {
		return a
	}
	return b
}

// This is a workload/network proxy used only before a process starts. It cannot
// authorize a vote, a read, or a completion, and contains no topology ID table.
type completionModel struct {
	prob                               []float64
	whole                              probabilitySpan
	regions                            []probabilitySpan
	ranges                             []config.PlannedCompletionRange
	writeCosts                         [][]float64
	certifiedWriteCosts                [][]float64
	ownerWriteCosts                    [][]float64
	ingresses, readers                 []int
	readNetwork, windows               []float64
	weights                            []float64
	leader                             int
	writes, offeredWrites, remoteShare float64
	hotCutoff                          int
	hot, cold                          probabilitySpan
	parentHolds                        []readHoldProfile
	ownerHolds                         [][]readHoldProfile
}

// The 1024-rank split below only controls exact evaluation versus moment
// approximation; it does not define hot/cold membership.
type probabilitySpan struct {
	start, end int
	mass       float64
	moments    [17]float64
}

func (m *completionModel) span(start, end int) probabilitySpan {
	s := probabilitySpan{start: start, end: end}
	for k := start; k < end; k++ {
		p := m.prob[k]
		s.mass += p
		if k < 1024 {
			continue
		}
		power := p
		for j := range s.moments {
			s.moments[j] += power
			power *= p
		}
	}
	return s
}

func (m *completionModel) exponentialSum(a float64, s probabilitySpan) float64 {
	if a <= 0 {
		return s.mass
	}
	limit := min(s.end, 1024)
	result := 0.0
	for k := s.start; k < limit; k++ {
		result += m.prob[k] * math.Exp(-a*m.prob[k])
	}
	tailStart := max(s.start, 1024)
	if tailStart >= s.end {
		return result
	}
	// Series only where its remainder is negligible. Large/different WAN
	// inputs fall back to the exact expression; no probabilistic gate is used.
	if a*m.prob[tailStart] > .5 {
		for k := tailStart; k < s.end; k++ {
			result += m.prob[k] * math.Exp(-a*m.prob[k])
		}
		return result
	}
	coefficient := 1.0
	for j, moment := range s.moments {
		if j > 0 {
			coefficient *= -a / float64(j)
		}
		result += coefficient * moment
	}
	return min(s.mass, max(0, result))
}

func residualHold(window, rate float64) float64 {
	if rate <= 0 || window <= 0 {
		return 0
	}
	return max(0, window+1000*math.Expm1(-rate*window/1000)/rate)
}

func (m *completionModel) parentHold(key, reader int, rate float64) float64 {
	if key < m.hotCutoff {
		return m.parentHolds[reader].mean(rate)
	}
	return residualHold(m.windows[reader], rate)
}

func (m *completionModel) cdf(x float64, delegated bool) float64 {
	value := 0.0
	clients := float64(len(m.readers))
	for c, reader := range m.readers {
		oldWrite := m.writeCosts[c][m.ingresses[c]]
		oldCertifiedWrite := m.certifiedWriteCosts[c][m.ingresses[c]]
		if oldWrite <= x {
			value += m.writes * m.hot.mass / clients
		}
		if oldCertifiedWrite <= x {
			value += m.writes * m.cold.mass / clients
		}
		slack := x - m.readNetwork[c]
		if x >= m.readNetwork[c] {
			hot := m.exponentialSum(m.offeredWrites*m.parentHolds[reader].residual(slack)/1000, m.hot)
			cold := m.exponentialSum(m.offeredWrites*max(0, m.windows[reader]-slack)/1000, m.cold)
			value += (1 - m.writes) * (hot + cold) / clients
		}
		if !delegated {
			continue
		}
		for i, region := range m.ranges {
			s := m.regions[i]
			newWrite := m.ownerWriteCosts[c][region.Owner.Rank]
			previousWrite := oldWrite
			if s.start >= m.hotCutoff {
				previousWrite, newWrite = oldCertifiedWrite, m.certifiedWriteCosts[c][region.Owner.Rank]
			}
			oldChosen, newChosen := 0.0, 0.0
			if previousWrite <= x {
				oldChosen = 1
			}
			if newWrite <= x {
				newChosen = 1
			}
			value += m.writes * s.mass * (newChosen - oldChosen) / clients
			if slack < 0 || s.start >= m.hotCutoff {
				continue
			}
			old := m.exponentialSum(m.offeredWrites*m.parentHolds[reader].residual(slack)/1000, s)
			new := m.exponentialSum(m.offeredWrites*m.ownerHolds[region.Owner.Rank][reader].residual(slack)/1000, s)
			value += (1 - m.writes) * (new - old) / clients
		}
	}
	return min(1, max(0, value))
}

func (m *completionModel) quantile(delegated bool, percentile float64) float64 {
	high := 0.0
	for c, reader := range m.readers {
		high = max(high, m.readNetwork[c]+m.windows[reader])
		high = max(high, m.readNetwork[c]+m.parentHolds[reader].upper())
		for _, profiles := range m.ownerHolds {
			high = max(high, m.readNetwork[c]+profiles[reader].upper())
		}
		for _, cost := range m.writeCosts[c] {
			high = max(high, cost)
		}
		for _, cost := range m.ownerWriteCosts[c] {
			high = max(high, cost)
		}
	}
	low := 0.0
	for i := 0; i < 50; i++ {
		mid := (low + high) / 2
		if m.cdf(mid, delegated) >= percentile {
			high = mid
		} else {
			low = mid
		}
	}
	return high
}

func planCompletionDelegation(c *config.Config, t *placement.Topology, plan *config.DeploymentPlan, leader int, mask uint64, direct bool) *completionModel {
	plan.Model = "owner-first-tail-guarded-completion-delegation"
	plan.CompletionRanges = nil
	plan.CompletionTailRejected = false
	plan.CompletionModelMeanSaving, plan.CompletionModelP99Delta, plan.CompletionModelTailBandDelta = 0, 0, 0
	if c.Writes <= 0 || c.UniqueKeys || c.KeyCount <= 0 || len(t.Clients) == 0 {
		return nil
	}
	n := len(t.Replicas)
	aliasRanks := map[string]int{}
	responders := []int{}
	for r, replica := range t.Replicas {
		aliasRanks[replica.Alias] = r
		if mask&bit(r) != 0 {
			responders = append(responders, r)
		}
	}
	m := completionModel{leader: leader, writes: float64(c.Writes) / 100, weights: make([]float64, n), windows: make([]float64, n), hotCutoff: heatpolicy.HotCutoff(c)}
	m.offeredWrites = c.ArrivalRate * float64(len(t.Clients)) * float64(max(1, c.Clones)) * m.writes
	g := newVisibilityGeometry(t, leader, mask)
	evidence := g.ordinary
	for i := 0; i < n; i++ {
		m.windows[i] = max(0, evidence[i]-t.D[leader][i])
	}
	parentWriteMean := 0.0
	parentCertifiedWriteMean := 0.0
	ownerWriteMeans := make([]float64, n)
	ownerCertifiedWriteMeans := make([]float64, n)
	for client, alias := range t.Clients {
		ingress := aliasRanks[plan.WriteClientRoutes[alias]]
		reader := aliasRanks[plan.ClientRoutes[alias]]
		m.ingresses = append(m.ingresses, ingress)
		m.readers = append(m.readers, reader)
		m.readNetwork = append(m.readNetwork, t.RTT(client, reader))
		m.weights[reader] += 1 / float64(len(t.Clients))
		if ingress != leader {
			m.remoteShare += 1 / float64(len(t.Clients))
		}
		costs := make([]float64, n)
		ownerCosts := make([]float64, n)
		certifiedCosts := make([]float64, n)
		votes := make([]float64, n)
		coverage := 0.
		for voter := range votes {
			votes[voter] = t.D[leader][voter] + t.Back[client][voter]
			if mask&bit(voter) != 0 {
				coverage = max(coverage, votes[voter])
			}
		}
		sort.Float64s(votes)
		certificate := max(votes[n/2], coverage)
		for i := 0; i < n; i++ {
			costs[i] = t.To[client][i] + t.D[i][leader] + evidence[i] + t.Back[client][i]
			ownerCosts[i] = t.To[client][i] + t.D[i][leader] + g.owners[i][i] + t.Back[client][i]
			if direct {
				costs[i] = admissionCost(t, client, i, leader, evidence[i])
				ownerCosts[i] = admissionCost(t, client, i, leader, g.owners[i][i])
			}
			ownerWriteMeans[i] += ownerCosts[i] / float64(len(t.Clients))
			start := t.To[client][i] + t.D[i][leader]
			if direct && i != leader {
				start = min(start, t.To[client][leader]+1)
			}
			certifiedCosts[i] = min(costs[i], start+certificate+2)
			ownerCertifiedWriteMeans[i] += certifiedCosts[i] / float64(len(t.Clients))
		}
		parentWriteMean += costs[ingress] / float64(len(t.Clients))
		parentCertifiedWriteMean += certifiedCosts[ingress] / float64(len(t.Clients))
		m.writeCosts = append(m.writeCosts, costs)
		m.ownerWriteCosts = append(m.ownerWriteCosts, ownerCosts)
		m.certifiedWriteCosts = append(m.certifiedWriteCosts, certifiedCosts)
	}
	m.parentHolds = make([]readHoldProfile, n)
	m.ownerHolds = make([][]readHoldProfile, n)
	for reader := 0; reader < n; reader++ {
		m.parentHolds[reader] = g.holdProfile(g.scenarios(-1, m.remoteShare, false), reader)
	}
	for owner := 0; owner < n; owner++ {
		m.ownerHolds[owner] = make([]readHoldProfile, n)
		for reader := 0; reader < n; reader++ {
			m.ownerHolds[owner][reader] = g.holdProfile(g.scenarios(owner, 0, false), reader)
		}
	}
	m.prob = c.HeatProfile.Probabilities
	previous, start := -1, 0
	headroom := 0.0
	appendRange := func(end int) {
		if previous >= 0 {
			m.ranges = append(m.ranges, config.PlannedCompletionRange{Start: int64(start), End: int64(end), Owner: t.Replicas[previous]})
		}
	}
	for key, probability := range m.prob {
		// Keep CDF regions on one side of the runtime's certificate boundary.
		if key == m.hotCutoff {
			appendRange(key)
			previous, start = -1, key
		}
		best, owner := 0.0, -1
		rate := m.offeredWrites * probability
		for _, candidate := range responders {
			readGain := 0.
			if key < m.hotCutoff {
				for reader, weight := range m.weights {
					readGain += weight * (m.parentHolds[reader].mean(rate) - m.ownerHolds[candidate][reader].mean(rate))
				}
			}
			writeDelta := ownerWriteMeans[candidate] - parentWriteMean
			if key >= m.hotCutoff {
				writeDelta = ownerCertifiedWriteMeans[candidate] - parentCertifiedWriteMean
			}
			gain := (1-m.writes)*readGain - m.writes*writeDelta
			if gain > best+1e-10 {
				best, owner = gain, candidate
			}
		}
		headroom += probability * best
		if owner != previous {
			appendRange(key)
			previous, start = owner, key
		}
	}
	appendRange(len(m.prob))
	m.whole = m.span(0, len(m.prob))
	m.hot, m.cold = m.span(0, m.hotCutoff), m.span(m.hotCutoff, len(m.prob))
	if len(m.ranges) == 0 {
		return &m
	}
	for _, region := range m.ranges {
		m.regions = append(m.regions, m.span(int(region.Start), int(region.End)))
	}
	parentP99, newP99 := m.quantile(false, .99), m.quantile(true, .99)
	plan.CompletionModelP99Delta = newP99 - parentP99
	worst := newP99 - parentP99
	for _, q := range []float64{.9898, .9902} {
		worst = max(worst, m.quantile(true, q)-m.quantile(false, q))
	}
	plan.CompletionModelTailBandDelta = worst
	if worst > .02 {
		plan.CompletionTailRejected = true
		return &m
	}
	plan.CompletionRanges = m.ranges
	plan.CompletionModelMeanSaving = headroom
	return &m
}
