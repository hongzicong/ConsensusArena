package agreement

import (
	"github.com/hongzicong/ConsensusArena/agreement/heatpolicy"
	"github.com/hongzicong/ConsensusArena/config"
	"github.com/hongzicong/ConsensusArena/placement"
	"math"
	"math/bits"
	"sort"
)

type coldBin struct {
	Key        int
	Mass, Rate float64
	Owner      int
	Write      []float64
	Targets    []int
	Scenarios  []readScenario
}
type coldPlanModel struct {
	HotCutoff        int
	T                *placement.Topology
	P                config.DeploymentPlan
	W                float64
	Leader           int
	Roles            uint64
	Bins             []coldBin
	Readers          []int
	Evidence, Window []float64
	Remote           float64
	Visibility       visibilityGeometry
}

func newColdPlanModel(c *config.Config, t *placement.Topology, p config.DeploymentPlan) *coldPlanModel {
	m := &coldPlanModel{HotCutoff: heatpolicy.HotCutoff(c), T: t, P: p, W: float64(c.Writes) / 100, Leader: p.Leader.Rank}
	n, clients := len(t.Replicas), len(t.Clients)
	rank := map[string]int{}
	for r, v := range t.Replicas {
		rank[v.Alias] = r
		for _, a := range p.Responders {
			if a == v.Alias {
				m.Roles |= bit(r)
			}
		}
	}
	m.Visibility = newVisibilityGeometry(t, m.Leader, m.Roles)
	m.Evidence = m.Visibility.ordinary
	m.Window = make([]float64, n)
	for r := 0; r < n; r++ {
		m.Window[r] = max(0, m.Evidence[r]-t.D[m.Leader][r])
	}
	m.Readers = make([]int, clients)
	for i, alias := range t.Clients {
		m.Readers[i] = rank[p.ClientRoutes[alias]]
		if rank[p.WriteClientRoutes[alias]] != m.Leader {
			m.Remote += 1 / float64(clients)
		}
	}
	edges := map[int]bool{0: true, c.KeyCount: true, m.HotCutoff: true}
	for i := 0; i <= min(c.KeyCount, 256); i++ {
		edges[i] = true
	}
	for i := 0; i <= 64; i++ {
		k := int(256 * math.Pow(4, float64(i)/64))
		if k < c.KeyCount {
			edges[k] = true
		}
	}
	if c.KeyCount > 1024 {
		for i := 0; i <= 256; i++ {
			k := int(1024 * math.Pow(float64(c.KeyCount)/1024, float64(i)/256))
			if k < c.KeyCount {
				edges[k] = true
			}
		}
	}
	for _, k := range heatpolicy.ColdCuts(c) {
		if k < c.KeyCount {
			edges[k] = true
		}
	}
	for _, s := range p.CompletionRanges {
		if int(s.Start) < c.KeyCount {
			edges[int(s.Start)] = true
		}
		if int(s.End) < c.KeyCount {
			edges[int(s.End)] = true
		}
	}
	for _, ranges := range p.ReadRaceRanges {
		for _, s := range ranges {
			if int(s.Start) < c.KeyCount {
				edges[int(s.Start)] = true
			}
			if int(s.End) < c.KeyCount {
				edges[int(s.End)] = true
			}
		}
	}
	sorted := make([]int, 0, len(edges))
	for k := range edges {
		sorted = append(sorted, k)
	}
	sort.Ints(sorted)
	prob := c.HeatProfile.Probabilities
	offered := c.ArrivalRate * float64(clients) * float64(max(1, c.Clones)) * m.W
	direct := make([]float64, clients)
	for ci := 0; ci < clients; ci++ {
		a := make([]float64, n)
		cover := 0.0
		for v := 0; v < n; v++ {
			a[v] = t.D[m.Leader][v] + t.Back[ci][v]
			if m.Roles&bit(v) != 0 {
				cover = max(cover, a[v])
			}
		}
		sort.Float64s(a)
		direct[ci] = max(a[n/2], cover)
	}
	for i := 0; i < len(sorted)-1; i++ {
		a, b := sorted[i], sorted[i+1]
		mass, squared := 0.0, 0.0
		for k := a; k < b; k++ {
			mass += prob[k]
			squared += prob[k] * prob[k]
		}
		if mass == 0 {
			continue
		}
		bin := coldBin{Key: a, Mass: mass, Rate: offered * squared / mass / 1000, Owner: -1, Write: make([]float64, clients), Targets: make([]int, clients)}
		for _, s := range p.CompletionRanges {
			if int64(a) >= s.Start && int64(a) < s.End {
				bin.Owner = s.Owner.Rank
				break
			}
		}
		for ci, alias := range t.Clients {
			home := rank[p.WriteClientRoutes[alias]]
			if bin.Owner >= 0 {
				home = bin.Owner
			}
			start := t.To[ci][home] + t.D[home][m.Leader]
			if home != m.Leader {
				start = min(start, t.To[ci][m.Leader]+1)
			}
			certified := p.ClientLearningAllKeys || a >= m.HotCutoff
			commit := m.Evidence[home]
			if bin.Owner >= 0 && !certified {
				commit = m.Visibility.owners[bin.Owner][home]
			}
			cost := max(t.To[ci][home], start+commit) + t.Back[ci][home]
			if certified {
				cost = min(cost, start+direct[ci]+2)
			}
			bin.Write[ci] = cost
			bin.Targets[ci] = m.Readers[ci]
			if a < m.HotCutoff {
				for _, s := range p.ReadRaceRanges[alias] {
					if int64(a) >= s.Start && int64(a) < s.End {
						bin.Targets[ci] = s.Target
						break
					}
				}
			}
		}
		// Hot-off uses the ordinary read fence even without hot client votes.
		fenced := p.ClientLearningAllKeys || a >= m.HotCutoff || hotAblated(c)
		bin.Scenarios = m.Visibility.scenarios(bin.Owner, m.Remote, fenced)
		m.Bins = append(m.Bins, bin)
	}
	return m
}
func (m *coldPlanModel) cdf(x float64) float64 {
	result := 0.0
	clients := float64(len(m.T.Clients))
	for _, b := range m.Bins {
		read, write := 0.0, 0.0
		for ci, r := range m.Readers {
			if b.Write[ci] <= x {
				write += 1 / clients
			}
			s := b.Targets[ci]
			read += m.Visibility.raceCDF(b.Scenarios, ci, r, s, b.Rate, x) / clients
		}
		result += b.Mass * (m.W*write + (1-m.W)*read)
	}
	return result
}

type coldPlanCandidate struct {
	Mask           uint64
	Times, ReadNet []float64
	Extra, Floor   float64
}

func (m *coldPlanModel) candidate(mask uint64) coldPlanCandidate {
	t := m.T
	n := len(t.Replicas)
	out := coldPlanCandidate{Mask: mask, Times: make([]float64, len(t.Clients)), ReadNet: make([]float64, len(t.Clients))}
	rt := make([]float64, n)
	ready := 0.0
	maxClient := 0.0
	for r := 0; r < n; r++ {
		rt[r] = t.D[m.Leader][r] + t.D[r][m.Leader]
		if mask&bit(r) != 0 {
			ready = max(ready, rt[r])
		}
	}
	sort.Float64s(rt)
	prepare := rt[n/2]
	classic := max(prepare, ready)
	for ci := range t.Clients {
		fast, read := 0.0, math.Inf(1)
		for r := 0; r < n; r++ {
			rtt := t.RTT(ci, r)
			maxClient = max(maxClient, rtt)
			if mask&bit(r) != 0 {
				fast = max(fast, rtt)
				read = min(read, rtt)
			}
		}
		out.Times[ci] = fast + 2
		out.ReadNet[ci] = read
		out.Floor += (m.W*(fast+2) + (1-m.W)*read) / float64(len(t.Clients))
	}
	out.Extra = 2*prepare + 2*classic + ready + maxClient + 20
	return out
}
func (m *coldPlanModel) gain(c coldPlanCandidate, cut int) float64 {
	maxTime, avgTime, avgTime2, avgSlow2, avgRead := 0.0, 0.0, 0.0, 0.0, 0.0
	clients := float64(len(c.Times))
	for i, v := range c.Times {
		maxTime = max(maxTime, v)
		avgTime += v / clients
		avgTime2 += v * v / clients
		avgSlow2 += (v + c.Extra) * (v + c.Extra) / clients
		avgRead += c.ReadNet[i] / clients
	}
	result := 0.0
	for _, b := range m.Bins {
		if b.Key < cut {
			continue
		}
		collision := min(1, b.Rate*(maxTime+200))
		oldWrite, oldRead, oldHold := 0.0, 0.0, 0.0
		for ci, r := range m.Readers {
			oldWrite += b.Write[ci] / clients
			oldRead += m.T.RTT(ci, r) / clients
			oldHold += residualHold(m.Window[r], b.Rate*1000) / clients
		}
		newHold := b.Rate / 2 * ((1-collision)*avgTime2 + collision*avgSlow2)
		result += b.Mass * (m.W*(oldWrite-avgTime-collision*c.Extra) + (1-m.W)*(oldRead-avgRead+oldHold-newHold))
	}
	return result
}
func (m *coldPlanModel) delta(c coldPlanCandidate, cut int, x float64) float64 {
	maxTime := 0.0
	for _, v := range c.Times {
		maxTime = max(maxTime, v)
	}
	clients := float64(len(c.Times))
	result := 0.0
	for _, b := range m.Bins {
		if b.Key < cut {
			continue
		}
		collision := min(1, b.Rate*(maxTime+200))
		oldw, neww, oldr, newr := 0.0, 0.0, 0.0, 0.0
		for ci, r := range m.Readers {
			if b.Write[ci] <= x {
				oldw += 1 / clients
			}
			if c.Times[ci] <= x {
				neww += (1 - collision) / clients
			}
			if c.Times[ci]+c.Extra <= x {
				neww += collision / clients
			}
			net := m.T.RTT(ci, r)
			if x >= net {
				oldr += math.Exp(-b.Rate*max(0, m.Window[r]+net-x)) / clients
			}
			newnet := c.ReadNet[ci]
			if x < newnet {
				continue
			}
			normal, slow := 0.0, 0.0
			for _, v := range c.Times {
				normal += max(0, v+newnet-x) / clients
				slow += max(0, v+c.Extra+newnet-x) / clients
			}
			newr += math.Exp(-b.Rate*((1-collision)*normal+collision*slow)) / clients
		}
		result += b.Mass * (m.W*(neww-oldw) + (1-m.W)*(newr-oldr))
	}
	return result
}
func combinedScreenPlan(c *config.Config, t *placement.Topology) (config.DeploymentPlan, error) {
	p, err := parentColdPlan(c, t)
	if err != nil {
		return p, err
	}
	return planColdPlacement(c, t, p), nil
}

// Exhaustive at evaluation-suite sizes; above that, keep nearby majorities
// around each replica and client. Iterating 2^n masks cannot support n up to 63.
func coldQuorumMasks(t *placement.Topology) []uint64 {
	n := len(t.Replicas)
	majority := n/2 + 1
	var masks []uint64
	if n <= 17 {
		for mask := uint64(1); mask < bit(n); mask++ {
			if bits.OnesCount64(mask) == majority {
				masks = append(masks, mask)
			}
		}
		return masks
	}
	seen := map[uint64]bool{}
	addNearest := func(cost func(int) float64) {
		order := make([]int, n)
		for r := range order {
			order[r] = r
		}
		sort.SliceStable(order, func(i, j int) bool { return cost(order[i]) < cost(order[j]) })
		mask := uint64(0)
		for _, r := range order[:majority] {
			mask |= bit(r)
		}
		if !seen[mask] {
			seen[mask] = true
			masks = append(masks, mask)
		}
	}
	for anchor := range t.Replicas {
		addNearest(func(r int) float64 { return t.D[anchor][r] + t.D[r][anchor] })
	}
	for ci := range t.Clients {
		addNearest(func(r int) float64 { return t.RTT(ci, r) })
	}
	return masks
}

func planColdPlacement(c *config.Config, t *placement.Topology, p config.DeploymentPlan) config.DeploymentPlan {
	if coldAblated(c) {
		p.ColdEnabled = false
		return p
	}
	if c.Writes <= 0 || len(c.HeatProfile.File.Keys) <= heatpolicy.HotCutoff(c) || c.UniqueKeys {
		return p
	}
	m := newColdPlanModel(c, t, p)
	all := []coldPlanCandidate{}
	for _, mask := range coldQuorumMasks(t) {
		all = append(all, m.candidate(mask))
	}
	sort.SliceStable(all, func(i, j int) bool { return all[i].Floor < all[j].Floor })
	if len(all) > 64 {
		all = all[:64]
	}
	qs := []float64{.9898, .99, .9902}
	parent := make([]float64, 3)
	limit := screenUpper(m, p)
	for _, candidate := range all {
		limit = max(limit, coldCandidateUpper(candidate))
	}
	for i, q := range qs {
		lo, hi := 0.0, limit
		for k := 0; k < 40; k++ {
			mid := (lo + hi) / 2
			if m.cdf(mid) >= q {
				hi = mid
			} else {
				lo = mid
			}
		}
		parent[i] = hi
	}
	bestMean, bestTail := 0.0, 0.0
	for _, candidate := range all {
		for _, cut := range heatpolicy.ColdCuts(c) {
			if cut >= c.KeyCount {
				continue
			}
			gain := m.gain(candidate, cut)
			if gain < -.02 {
				continue
			}
			okay := true
			for i, q := range qs {
				x := parent[i] + .02
				if m.cdf(x)+m.delta(candidate, cut, x) < q-1e-9 {
					okay = false
					break
				}
			}
			if !okay {
				continue
			}
			lo, hi := 0.0, limit
			for k := 0; k < 36; k++ {
				mid := (lo + hi) / 2
				if m.cdf(mid)+m.delta(candidate, cut, mid) >= .99 {
					hi = mid
				} else {
					lo = mid
				}
			}
			tail := parent[1] - hi
			if tail > bestTail+.02 || math.Abs(tail-bestTail) <= .02 && gain > bestMean+.02 {
				p.ColdEnabled = true
				p.ColdCutoff = int64(cut)
				p.ColdQuorum = candidate.Mask
				bestMean, bestTail = gain, tail
			}
		}
	}
	p.Model = "selective-fixed-fast-majority-cold-read-plane"
	return p
}
