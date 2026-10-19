package xpaxos

import (
	"math"
	"sort"

	"github.com/hongzicong/ConsensusArena/placement"
)

// Startup latency proxy for the runtime's delayed ACCEPTED. The owner can
// commit once it directly gathers covered-quorum evidence; every other reader must also
// receive the owner's subsequently released vote or a leader commit notice.
// Client-certified and recovered writes have no delayed voter.
// This models network time, not queueing/batching.
type visibilityGeometry struct {
	t        *placement.Topology
	leader   int
	ordinary []float64
	owners   [][]float64
}

func newVisibilityGeometry(t *placement.Topology, leader int, mask uint64) visibilityGeometry {
	ordinary := evidenceAt(t, leader, mask|bit(leader))
	direct := directEvidenceAt(t, leader, mask|bit(leader))
	g := visibilityGeometry{t: t, leader: leader, ordinary: ordinary, owners: make([][]float64, len(ordinary))}
	for owner := range ordinary {
		g.owners[owner] = make([]float64, len(ordinary))
		for reader := range ordinary {
			g.owners[owner][reader] = direct[owner]
			if reader != owner {
				g.owners[owner][reader] = max(direct[reader], direct[owner]+t.D[owner][reader])
			}
		}
		leaderCommit := g.owners[owner][leader]
		for reader := range ordinary {
			if reader != owner {
				g.owners[owner][reader] = min(g.owners[owner][reader], max(t.D[leader][reader], leaderCommit+t.D[leader][reader]))
			}
		}
	}
	return g
}

type readScenario struct {
	share    float64
	owner    int // -1: all responders fence reads on acceptance
	evidence []float64
}

func (g visibilityGeometry) scenarios(owner int, remoteShare float64, certified bool) []readScenario {
	if certified {
		return []readScenario{{1, -1, g.ordinary}}
	}
	if owner >= 0 {
		return []readScenario{{1, owner, g.owners[owner]}}
	}
	return []readScenario{{1 - remoteShare, g.leader, g.owners[g.leader]}, {remoteShare, -1, g.ordinary}}
}

type holdWindow struct{ window, share float64 }
type readHoldProfile []holdWindow

func (g visibilityGeometry) holdProfile(scenarios []readScenario, reader int) readHoldProfile {
	p := readHoldProfile{}
	for _, s := range scenarios {
		if s.owner != reader && s.share > 0 {
			p = append(p, holdWindow{max(0, s.evidence[reader]-g.t.D[g.leader][reader]), s.share})
		}
	}
	sort.Slice(p, func(i, j int) bool { return p[i].window < p[j].window })
	return p
}

func (p readHoldProfile) residual(slack float64) float64 {
	v := 0.
	for _, h := range p {
		v += h.share * max(0, h.window-slack)
	}
	return v
}

func (p readHoldProfile) upper() float64 {
	if len(p) == 0 {
		return 0
	}
	return p[len(p)-1].window
}

// Integrate the Poisson hold CDF exactly between window endpoints. A mixture
// of write origins changes the exposure windows, not just the write rate.
func (p readHoldProfile) mean(rate float64) float64 {
	if rate <= 0 {
		return 0
	}
	lo, result := 0., 0.
	for _, h := range p {
		hi := h.window
		if hi <= lo {
			continue
		}
		a, b := rate*p.residual(lo)/1000, rate*p.residual(hi)/1000
		factor := 1.
		if a > b {
			factor = -math.Expm1(-(a - b)) / (a - b)
		}
		result += (hi - lo) * (1 - math.Exp(-b)*factor)
		lo = hi
	}
	return max(0, result)
}

// Correlate two raced reads separately for each write-origin scenario; the
// owner has no invalidation interval, while other readers wait for its vote.
func (g visibilityGeometry) raceCDF(scenarios []readScenario, client, r, s int, rate, x float64) float64 {
	t := g.t
	a, b, union := 0., 0., 0.
	for _, scenario := range scenarios {
		xr, xs := 0., 0.
		if scenario.owner != r {
			xr = max(0, scenario.evidence[r]-t.D[g.leader][r]+t.RTT(client, r)-x)
		}
		if scenario.owner != s {
			xs = max(0, scenario.evidence[s]-t.D[g.leader][s]+t.RTT(client, s)-x)
		}
		overlap := 0.
		if scenario.owner != r && scenario.owner != s {
			overlap = max(0, min(t.To[client][r]-t.D[g.leader][r], t.To[client][s]-t.D[g.leader][s])-max(x-t.Back[client][r]-scenario.evidence[r], x-t.Back[client][s]-scenario.evidence[s]))
		}
		a += scenario.share * xr
		b += scenario.share * xs
		union += scenario.share * max(0, xr+xs-overlap)
	}
	f1, f2, both := 0., 0., 0.
	if x >= t.RTT(client, r) {
		f1 = math.Exp(-rate * a)
	}
	if x >= t.RTT(client, s) {
		f2 = math.Exp(-rate * b)
	}
	if x >= t.RTT(client, r) && x >= t.RTT(client, s) {
		both = math.Exp(-rate * union)
	}
	return min(1., max(0., f1+f2-both))
}
