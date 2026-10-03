package kcensus

// Topology-aware delegate and budget synthesis.
// Based on KCensus Algorithms 1 and 4 and LPD-EPFL/kcensus (b232c332),
// src/consensus/kcensus/propagation.rs. See LICENSE.upstream for its MIT license.
import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"github.com/hongzicong/ConsensusArena/config"
	"github.com/hongzicong/ConsensusArena/placement"
	"math"
	"path/filepath"
	"sort"
	"time"
)

// PlanDeployment prepares ingress and the input to the native per-process
// delegate/budget synthesis below. There is no global leader or fixed C2 set.
func PlanDeployment(c *config.Config, t *placement.Topology) (config.DeploymentPlan, error) {
	if c.KCensusTopology == "" {
		c.KCensusTopology = c.Topology
	} else if !filepath.IsAbs(c.KCensusTopology) {
		c.KCensusTopology = filepath.Join(t.ConfigDir, c.KCensusTopology)
	}
	return config.DeploymentPlan{Model: "native-process-synthesis", ClientRoutes: t.NearestRoutes()}, nil
}

type Plan struct {
	Voters       int
	Leaders      []int
	Returns      []time.Duration
	Requirements []Requirement
	Budgets      []time.Duration
	Mean         time.Duration
	Digest       [32]byte
	Graphs       []propagationGraph
}

type candidate struct {
	req    Requirement
	budget time.Duration
	leader int
	cost   time.Duration
}

// Upstream leader_prio is stable ascending synthesized proposal latency.
// This order selects shared-backlog proposers, not initial per-slot leaders.
func (p Plan) leaderPriority() []int {
	ids := make([]int, len(p.Requirements))
	for i := range ids {
		ids[i] = i
	}
	if len(p.Budgets) == len(ids) {
		sort.SliceStable(ids, func(i, j int) bool {
			a, b := p.Budgets[ids[i]], p.Budgets[ids[j]]
			if len(p.Returns) == len(ids) {
				a += p.Returns[ids[i]]
				b += p.Returns[ids[j]]
			}
			return a < b
		})
	}
	return ids
}

// Synthesize retains the voter-only API for callers without configured clients.
func Synthesize(latency [][]time.Duration, weights []float64) (Plan, error) {
	return SynthesizeProcesses(latency, len(latency), weights)
}

// Each client chooses a voting delegate jointly with all compatible requirements.
// The objective includes the direct delegate-to-proposer executed-result delay.
func SynthesizeProcesses(latency [][]time.Duration, voters int, weights []float64) (Plan, error) {
	m := len(latency)
	if voters < 3 || voters > m || voters%2 != 1 || m > 63 {
		return Plan{}, fmt.Errorf("invalid voter/process membership %d/%d", voters, m)
	}
	if len(weights) == 0 {
		weights = make([]float64, m)
		for i := range weights {
			weights[i] = 1
		}
	}
	if len(weights) != m {
		return Plan{}, fmt.Errorf("weight count differs from membership")
	}
	var weightSum float64
	for i, row := range latency {
		if len(row) != m {
			return Plan{}, fmt.Errorf("non-square latency table")
		}
		for j, v := range row {
			if v < 0 || v > 24*time.Hour || (i == j && v != 0) {
				return Plan{}, fmt.Errorf("invalid latency %d/%d", i, j)
			}
		}
		if weights[i] < 0 || math.IsNaN(weights[i]) || math.IsInf(weights[i], 0) {
			return Plan{}, fmt.Errorf("invalid weight")
		}
		weightSum += weights[i]
	}
	if weightSum == 0 {
		return Plan{}, fmt.Errorf("all weights zero")
	}
	d, _ := planningPaths(latency)
	levels := make([][]candidate, m)
	signature := func(r Requirement) string {
		b := make([]byte, 8*len(r))
		for i, v := range r {
			binary.LittleEndian.PutUint64(b[8*i:], v)
		}
		return string(b)
	}
	for p := 0; p < m; p++ {
		byRequirement := map[string]candidate{}
		for leader := 0; leader < voters; leader++ {
			if p < voters && p != leader {
				continue
			}
			// Upstream searches between the fastest majority arrival and that
			// arrival plus the delegate's majority round trip (classic upper bound).
			first, classic := make([]time.Duration, voters), make([]time.Duration, voters)
			for v := 0; v < voters; v++ {
				first[v] = d[p][v] + d[v][leader]
				classic[v] = d[leader][v] + d[v][leader]
			}
			sort.Slice(first, func(i, j int) bool { return first[i] < first[j] })
			sort.Slice(classic, func(i, j int) bool { return classic[i] < classic[j] })
			minBudget := first[voters/2]
			maxBudget := minBudget + classic[voters/2]
			type arrival struct {
				t    time.Duration
				a, w int
			}
			arrivals := make([]arrival, 0, voters*voters)
			for a := 0; a < voters; a++ {
				for w := 0; w < voters; w++ {
					arrivals = append(arrivals, arrival{d[p][a] + d[a][w] + d[w][leader], a, w})
				}
			}
			sort.Slice(arrivals, func(i, j int) bool {
				if arrivals[i].t != arrivals[j].t {
					return arrivals[i].t < arrivals[j].t
				}
				if arrivals[i].w != arrivals[j].w {
					return arrivals[i].w < arrivals[j].w
				}
				return arrivals[i].a < arrivals[j].a
			})
			r := make(Requirement, m)
			for i, e := range arrivals {
				if e.t > maxBudget {
					break
				}
				r[e.w] |= 1 << e.a
				if i+1 < len(arrivals) && arrivals[i+1].t == e.t {
					continue
				}
				if e.t < minBudget || !r.valid(m, voters/2) || r[leader] != r.Quorum() {
					continue
				}
				c := candidate{req: append(Requirement(nil), r...), budget: e.t, leader: leader, cost: e.t + latency[leader][p]}
				sig := signature(r)
				old, ok := byRequirement[sig]
				if !ok || c.cost < old.cost || (c.cost == old.cost && c.leader < old.leader) {
					byRequirement[sig] = c
				}
			}
		}
		for _, c := range byRequirement {
			levels[p] = append(levels[p], c)
		}
		sort.Slice(levels[p], func(i, j int) bool {
			a, b := levels[p][i], levels[p][j]
			if a.cost != b.cost {
				return a.cost < b.cost
			}
			if a.leader != b.leader {
				return a.leader < b.leader
			}
			return signature(a.req) < signature(b.req)
		})
		if len(levels[p]) == 0 {
			return Plan{}, fmt.Errorf("no valid level for proposer %d", p)
		}
	}
	// A superset of compatible knowledge at no greater cost dominates a level.
	for p := range levels {
		keep := []candidate{}
		for _, c := range levels[p] {
			dominated := false
			for _, a := range keep {
				superset := true
				for w := range a.req {
					if c.req[w] & ^a.req[w] != 0 {
						superset = false
						break
					}
				}
				if superset {
					dominated = true
					break
				}
			}
			if !dominated {
				keep = append(keep, c)
			}
		}
		levels[p] = keep
	}
	// Different proposers/delegates often produce the same voter matrix.
	// Cache compatibility by matrix identity instead of recomputing it per branch.
	requirementIDs := map[string]int{}
	var matrices []Requirement
	ids := make([][]int, m)
	for p, ls := range levels {
		ids[p] = make([]int, len(ls))
		for l, c := range ls {
			sig := signature(c.req)
			id, ok := requirementIDs[sig]
			if !ok {
				id = len(matrices)
				requirementIDs[sig] = id
				matrices = append(matrices, c.req)
			}
			ids[p][l] = id
		}
	}
	compatible := make([][]bool, len(matrices))
	for i := range matrices {
		compatible[i] = make([]bool, len(matrices))
		for j := 0; j <= i; j++ {
			ok := Compatible(matrices[i], matrices[j], voters/2)
			compatible[i][j] = ok
			compatible[j][i] = ok
		}
	}
	// Exact branch and bound. Filter domains against each chosen requirement;
	// unlike a monotone budget search this also permits different delegates.
	domains := make([][]int, m)
	best := make([]int, m)
	bestCost := 0.0
	full := uint64(1)<<voters - 1
	for p := range levels {
		for i, c := range levels[p] {
			domains[p] = append(domains[p], i)
			if c.req.Quorum() == full {
				all := true
				for w := 0; w < voters; w++ {
					if c.req[w] != full {
						all = false
						break
					}
				}
				if all {
					best[p] = i
				}
			}
		}
		// Largest retained knowledge is compatible with every normal-form level.
		if levels[p][best[p]].req.Quorum() != full {
			best[p] = len(levels[p]) - 1
		}
		bestCost += weights[p] * float64(levels[p][best[p]].cost)
	}
	feasibleBest := true
	for p := range best {
		for q := 0; q < p; q++ {
			if !Compatible(levels[p][best[p]].req, levels[q][best[q]].req, voters/2) {
				feasibleBest = false
			}
		}
	}
	if !feasibleBest {
		bestCost = math.Inf(1)
	}
	chosen := make([]int, m)
	for i := range chosen {
		chosen[i] = -1
	}
	var search func([][]int, int, float64)
	search = func(ds [][]int, left int, cost float64) {
		if left == 0 {
			if cost < bestCost {
				bestCost = cost
				copy(best, chosen)
			}
			return
		}
		bound := cost
		p := -1
		for q := 0; q < m; q++ {
			if chosen[q] >= 0 {
				continue
			}
			if len(ds[q]) == 0 {
				return
			}
			bound += weights[q] * float64(levels[q][ds[q][0]].cost)
			if p < 0 || len(ds[q]) < len(ds[p]) {
				p = q
			}
		}
		if bound >= bestCost {
			return
		}
		for _, l := range ds[p] {
			chosen[p] = l
			next := make([][]int, m)
			ok := true
			for q := 0; q < m; q++ {
				if chosen[q] >= 0 {
					continue
				}
				for _, j := range ds[q] {
					if compatible[ids[p][l]][ids[q][j]] {
						next[q] = append(next[q], j)
					}
				}
				if len(next[q]) == 0 {
					ok = false
					break
				}
			}
			if ok {
				search(next, left-1, cost+weights[p]*float64(levels[p][l].cost))
			}
			chosen[p] = -1
		}
	}
	search(domains, m, 0)
	if math.IsInf(bestCost, 1) {
		return Plan{}, fmt.Errorf("no compatible plan")
	}
	plan := Plan{Voters: voters, Leaders: make([]int, m), Returns: make([]time.Duration, m), Requirements: make([]Requirement, m), Budgets: make([]time.Duration, m), Mean: time.Duration(bestCost / weightSum)}
	h := sha256.New()
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], uint64(voters))
	h.Write(b[:])
	for p, l := range best {
		c := levels[p][l]
		plan.Requirements[p], plan.Budgets[p], plan.Leaders[p], plan.Returns[p] = c.req, c.budget, c.leader, latency[c.leader][p]
		binary.LittleEndian.PutUint64(b[:], uint64(c.leader))
		h.Write(b[:])
		for _, v := range c.req {
			binary.LittleEndian.PutUint64(b[:], v)
			h.Write(b[:])
		}
	}
	for _, id := range plan.leaderPriority() {
		binary.LittleEndian.PutUint64(b[:], uint64(id))
		h.Write(b[:])
	}
	var err error
	plan.Graphs, err = buildPropagation(latency, plan.Requirements, plan.Budgets, plan.Leaders, voters)
	if err != nil {
		return Plan{}, err
	}
	// Peers must agree on causal dependencies as well as requirements.
	for _, g := range plan.Graphs {
		for node, states := range g.States {
			binary.LittleEndian.PutUint64(b[:], uint64(len(states)))
			h.Write(b[:])
			for _, s := range states {
				binary.LittleEndian.PutUint64(b[:], uint64(s.Time))
				h.Write(b[:])
				for _, k := range s.Knowledge {
					binary.LittleEndian.PutUint64(b[:], k)
					h.Write(b[:])
				}
				for _, at := range s.Remote {
					binary.LittleEndian.PutUint64(b[:], uint64(at))
					h.Write(b[:])
				}
				deps := make([]graphEdge, 0, len(s.Dependencies))
				for e := range s.Dependencies {
					deps = append(deps, e)
				}
				sort.Slice(deps, func(i, j int) bool {
					if deps[i].Time != deps[j].Time {
						return deps[i].Time < deps[j].Time
					}
					return deps[i].From < deps[j].From
				})
				binary.LittleEndian.PutUint64(b[:], uint64(len(deps)))
				h.Write(b[:])
				for _, e := range deps {
					binary.LittleEndian.PutUint64(b[:], uint64(e.From))
					h.Write(b[:])
					binary.LittleEndian.PutUint64(b[:], uint64(e.Time))
					h.Write(b[:])
				}
				binary.LittleEndian.PutUint64(b[:], uint64(len(s.Outgoing)))
				h.Write(b[:])
				for _, e := range s.Outgoing {
					binary.LittleEndian.PutUint64(b[:], uint64(node))
					h.Write(b[:])
					binary.LittleEndian.PutUint64(b[:], uint64(e.To))
					h.Write(b[:])
					flag := uint64(0)
					if g.Payload[e] {
						flag = 1
					}
					binary.LittleEndian.PutUint64(b[:], flag)
					h.Write(b[:])
				}
			}
		}
	}
	copy(plan.Digest[:], h.Sum(nil))
	return plan, validateRequirements(plan.Requirements, voters)
}
