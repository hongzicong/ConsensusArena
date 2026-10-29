package agreement

import (
	"github.com/hongzicong/ConsensusArena/agreement/heatpolicy"
	"github.com/hongzicong/ConsensusArena/config"
	"github.com/hongzicong/ConsensusArena/placement"
	"math"
	"sort"
	"strings"
)

// Every calculation here is a startup performance proxy. It supplies no read,
// lease, acceptance or output authority and contains no topology-ID table.
type keyCohort struct {
	key               int
	mass, probability float64
}
type baselinePath struct {
	mass, network, window, write, rate, weight float64
	holds                                      readHoldProfile
}
type certificateGeometry struct {
	leader                 int
	mask                   uint64
	readers, homes         []int
	network, window, write []float64
	mean, tail             float64
}
type probabilityCurve struct {
	step        float64
	g, integral []float64
}

func certificateCohorts(c *config.Config, plans ...config.DeploymentPlan) []keyCohort {
	p := c.HeatProfile.Probabilities
	edges := []int{0}
	for k := 1; k <= min(1024, c.KeyCount); k++ {
		edges = append(edges, k)
	}
	if c.KeyCount > 1024 {
		for j := 1; j <= 128; j++ {
			edge := int(math.Exp(math.Log(1024) + float64(j)/128*math.Log(float64(c.KeyCount)/1024)))
			if j == 128 {
				edge = c.KeyCount
			}
			if edge > edges[len(edges)-1] {
				edges = append(edges, edge)
			}
		}
	}
	edges = append(edges, heatpolicy.HotCutoff(c))
	for _, p := range plans {
		for _, span := range p.CompletionRanges {
			edges = append(edges, int(span.Start), int(span.End))
		}
	}
	sort.Ints(edges)
	out := make([]keyCohort, 0, len(edges)-1)
	for j := 0; j+1 < len(edges); j++ {
		m, s := 0., 0.
		for k := edges[j]; k < edges[j+1]; k++ {
			m += p[k]
			s += p[k] * p[k]
		}
		if m > 0 {
			out = append(out, keyCohort{edges[j], m, s / m})
		}
	}
	return out
}

func directEvidenceAt(t *placement.Topology, l int, mask uint64) []float64 {
	n := len(t.Replicas)
	e := make([]float64, n)
	for r := 0; r < n; r++ {
		a := make([]float64, n)
		cover := 0.
		for v := 0; v < n; v++ {
			a[v] = t.D[l][v] + t.D[v][r]
			if mask&bit(v) != 0 {
				cover = max(cover, a[v])
			}
		}
		sort.Float64s(a)
		e[r] = max(t.D[l][r], max(a[n/2], cover))
	}
	return e
}

func evidenceAt(t *placement.Topology, l int, mask uint64) []float64 {
	e := directEvidenceAt(t, l, mask|bit(l))
	leaderCommit := e[l]
	for r := range e {
		e[r] = min(e[r], max(t.D[l][r], leaderCommit+t.D[l][r]))
	}
	return e
}

func candidateCertificateGeometry(t *placement.Topology, l int, mask uint64, w float64) certificateGeometry {
	n, nc := len(t.Replicas), len(t.Clients)
	e := evidenceAt(t, l, mask)
	x := certificateGeometry{leader: l, mask: mask, readers: make([]int, nc), homes: make([]int, nc), network: make([]float64, nc), window: make([]float64, nc), write: make([]float64, nc)}
	for client := 0; client < nc; client++ {
		votes := make([]float64, n)
		cover := 0.
		reader := -1
		for v := 0; v < n; v++ {
			votes[v] = t.D[l][v] + t.Back[client][v]
			if mask&bit(v) != 0 {
				cover = max(cover, votes[v])
				if reader < 0 || t.RTT(client, v) < t.RTT(client, reader) {
					reader = v
				}
			}
		}
		sort.Float64s(votes)
		cv := max(votes[n/2], cover)
		best, home := math.Inf(1), 0
		for h := 0; h < n; h++ {
			start := min(t.To[client][h]+t.D[h][l], t.To[client][l]+1)
			normal := max(t.To[client][h], start+e[h]) + t.Back[client][h]
			cost := min(normal, start+cv+2)
			if cost < best {
				best, home = cost, h
			}
		}
		x.readers[client], x.homes[client] = reader, home
		x.network[client] = t.RTT(client, reader) + 1
		x.window[client] = max(0, e[reader]-t.D[l][reader])
		x.write[client] = best + 1
	}
	return x
}

func parentCertificatePaths(c *config.Config, t *placement.Topology, p config.DeploymentPlan, cohorts []keyCohort, offered, w float64) []baselinePath {
	l := p.Leader.Rank
	rank := map[string]int{}
	mask := uint64(0)
	for i, v := range t.Replicas {
		rank[v.Alias] = i
	}
	for _, a := range p.Responders {
		mask |= bit(rank[a])
	}
	e := evidenceAt(t, l, mask)
	g := newVisibilityGeometry(t, l, mask)
	remote := 0.
	for _, alias := range t.Clients {
		if rank[p.WriteClientRoutes[alias]] != l {
			remote += 1 / float64(len(t.Clients))
		}
	}
	out := make([]baselinePath, 0, len(cohorts)*len(t.Clients))
	for ci, a := range t.Clients {
		r := rank[p.ClientRoutes[a]]
		votes := make([]float64, len(t.Replicas))
		cover := 0.
		for v := range votes {
			votes[v] = t.D[l][v] + t.Back[ci][v]
			if mask&bit(v) != 0 {
				cover = max(cover, votes[v])
			}
		}
		sort.Float64s(votes)
		cv := max(votes[len(votes)/2], cover)
		for _, k := range cohorts {
			h := rank[p.WriteClientRoutes[a]]
			owner := -1
			for _, span := range p.CompletionRanges {
				if int64(k.key) >= span.Start && int64(k.key) < span.End {
					h = span.Owner.Rank
					owner = h
					break
				}
			}
			start := min(t.To[ci][h]+t.D[h][l], t.To[ci][l]+1)
			certified := p.ClientLearningAllKeys || k.key >= heatpolicy.HotCutoff(c)
			commit := e[h]
			if owner >= 0 && !certified {
				commit = g.owners[owner][h]
			}
			write := max(t.To[ci][h], start+commit) + t.Back[ci][h]
			if certified {
				write = min(write, start+cv+2)
			}
			holds := g.holdProfile(g.scenarios(owner, remote, certified), r)
			out = append(out, baselinePath{k.mass, t.RTT(ci, r), holds.upper(), write, offered * k.probability / 1000, t.Weights[ci], holds})
		}
	}
	return out
}

func baselineCDF(paths []baselinePath, w, x float64) float64 {
	total := 0.
	for _, p := range paths {
		rd := 0.
		if x >= p.network {
			rd = math.Exp(-p.rate * p.holds.residual(x-p.network))
		}
		wr := 0.
		if p.write <= x {
			wr = 1
		}
		total += p.mass * p.weight * ((1-w)*rd + w*wr)
	}
	return total
}
func baselineMean(paths []baselinePath, w float64) float64 {
	total := 0.
	for _, p := range paths {
		hold := p.holds.mean(p.rate * 1000)
		total += p.mass * p.weight * ((1-w)*(p.network+hold) + w*p.write)
	}
	return total
}

func newProbabilityCurve(cohorts []keyCohort, through float64) probabilityCurve {
	step := .5
	n := int(math.Ceil(through/step)) + 2
	q := probabilityCurve{step: step, g: make([]float64, n), integral: make([]float64, n)}
	for i := range q.g {
		u := float64(i) * step
		for _, k := range cohorts {
			q.g[i] += k.mass * math.Exp(-u*k.probability)
		}
		q.g[i] = min(1., max(0., q.g[i]))
		if i == 0 {
			q.g[i] = 1
		}
		if i > 0 {
			q.integral[i] = q.integral[i-1] + step*(q.g[i-1]+q.g[i])/2
		}
	}
	return q
}
func (q probabilityCurve) at(values []float64, u float64) float64 {
	if u <= 0 {
		return values[0]
	}
	x := u / q.step
	i := int(x)
	if i+1 >= len(values) {
		return values[len(values)-1]
	}
	return values[i] + (x-float64(i))*(values[i+1]-values[i])
}
func (q probabilityCurve) cdf(x certificateGeometry, t *placement.Topology, lambda, w, value float64) float64 {
	total := 0.
	for ci := range x.network {
		rd := 0.
		if value >= x.network[ci] {
			rd = q.at(q.g, lambda*max(0, x.window[ci]+x.network[ci]-value))
		}
		wr := 0.
		if x.write[ci] <= value {
			wr = 1
		}
		total += t.Weights[ci] * ((1-w)*rd + w*wr)
	}
	return total
}
func (q probabilityCurve) mean(x certificateGeometry, t *placement.Topology, lambda, w float64) float64 {
	total := 0.
	for ci := range x.network {
		hold := max(0, x.window[ci]-q.at(q.integral, lambda*x.window[ci])/lambda)
		total += t.Weights[ci] * ((1-w)*(x.network[ci]+hold) + w*x.write[ci])
	}
	return total
}

func exploratoryHotPlan(c *config.Config, t *placement.Topology) (config.DeploymentPlan, error) {
	p, err := baselineHotPlan(c, t)
	if err != nil || hotAblated(c) || c.Writes <= 0 || c.KeyCount <= 0 || c.UniqueKeys {
		return p, err
	}
	n := len(t.Replicas)
	if n > 13 {
		return p, nil
	}
	w := float64(c.Writes) / 100
	offered := c.ArrivalRate * float64(len(t.Clients)) * float64(max(1, c.Clones)) * w
	lambda := offered / 1000
	if lambda <= 0 {
		return p, nil
	}
	cohorts := certificateCohorts(c, p)
	paths := parentCertificatePaths(c, t, p, cohorts, offered, w)
	beforeMean := baselineMean(paths, w)
	upper := 1000.
	for _, v := range paths {
		upper = max(upper, max(v.write, v.network+v.window)+100)
	}
	lo, hi := 0., upper
	for k := 0; k < 40; k++ {
		mid := (lo + hi) / 2
		if baselineCDF(paths, w, mid) >= .99-1e-12 {
			hi = mid
		} else {
			lo = mid
		}
	}
	beforeTail := hi
	candidates := make([]certificateGeometry, 0, n*(1<<(n-1)))
	maxWindow := 0.
	for l := 0; l < n; l++ {
		for mask := uint64(1); mask < uint64(1)<<uint(n); mask++ {
			if mask&bit(l) == 0 {
				continue
			}
			x := candidateCertificateGeometry(t, l, mask, w)
			for ci := range x.window {
				maxWindow = max(maxWindow, x.window[ci])
				upper = max(upper, max(x.write[ci], x.network[ci]+x.window[ci])+100)
			}
			candidates = append(candidates, x)
		}
	}
	curve := newProbabilityCurve(cohorts, lambda*maxWindow+1)
	bestMean := -1
	eligible := []int{}
	for i := range candidates {
		candidates[i].mean = curve.mean(candidates[i], t, lambda, w)
		if candidates[i].mean <= beforeMean+.02 {
			eligible = append(eligible, i)
		}
		if bestMean < 0 || candidates[i].mean < candidates[bestMean].mean {
			bestMean = i
		}
	}
	policies := []certificateGeometry{}
	if bestMean >= 0 {
		x := &candidates[bestMean]
		lo, hi = 0, upper
		for k := 0; k < 36; k++ {
			mid := (lo + hi) / 2
			if curve.cdf(*x, t, lambda, w, mid) >= .99-1e-12 {
				hi = mid
			} else {
				lo = mid
			}
		}
		x.tail = hi
		policies = append(policies, *x)
	}
	if false && len(eligible) > 0 {
		lo, hi = 0, upper
		for k := 0; k < 36; k++ {
			mid := (lo + hi) / 2
			found := false
			for _, i := range eligible {
				if curve.cdf(candidates[i], t, lambda, w, mid) >= .99-1e-12 {
					found = true
					break
				}
			}
			if found {
				hi = mid
			} else {
				lo = mid
			}
		}
		best := -1
		for _, i := range eligible {
			if curve.cdf(candidates[i], t, lambda, w, hi+.02) >= .99-1e-12 && (best < 0 || candidates[i].mean < candidates[best].mean) {
				best = i
			}
		}
		if best >= 0 {
			candidates[best].tail = hi + .02
			policies = append(policies, candidates[best])
		}
	}
	selected := -1
	score := math.Inf(-1)
	for i, x := range policies {
		if false {
			continue
		}
		gain := (beforeMean-x.mean)/beforeMean + .5*(beforeTail-x.tail)/beforeTail
		if gain > score {
			score, selected = gain, i
		}
	}
	p.PlacementParentMean, p.PlacementParentP99 = beforeMean, beforeTail
	p.PlacementShortlist = len(candidates)
	if selected < 0 {
		return p, nil
	}
	x := policies[selected]
	leader := t.Replicas[x.leader]
	p.Leader = &leader
	p.ClientLearningAllKeys = true
	p.PlacementChanged = true
	p.PlacementSelectedMean, p.PlacementSelectedP99 = x.mean, x.tail
	p.CompletionRanges = nil
	p.ResponderRanges = ""
	p.CompletionModelMeanSaving, p.CompletionModelP99Delta, p.CompletionModelTailBandDelta = 0, 0, 0
	p.Responders = nil
	p.ClientRoutes = map[string]string{}
	p.WriteClientRoutes = map[string]string{}
	for i, v := range t.Replicas {
		if x.mask&bit(i) != 0 {
			p.Responders = append(p.Responders, v.Alias)
		}
	}
	for ci, a := range t.Clients {
		p.ClientRoutes[a] = t.Replicas[x.readers[ci]].Alias
		p.WriteClientRoutes[a] = t.Replicas[x.homes[ci]].Alias
	}
	c.BodegaResponders = strings.Join(p.Responders, ",")
	c.BodegaResponderRanges = ""
	p = planJointReadRaces(c, t, p)
	p.Model = "joint-all-key-client-certificates-and-covered-read-roles"
	return p, nil
}
