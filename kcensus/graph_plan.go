package kcensus

// Dependency-graph construction follows upstream propagation.rs (b232c332).
// State times label causal events; runtime execution is driven by dependencies.
import (
	"fmt"
	"sort"
	"time"
)

type graphEdge struct {
	From, To int
	Time     time.Duration
}

type graphState struct {
	Time         time.Duration
	Knowledge    []uint64
	Remote       []time.Duration
	Dependencies map[graphEdge]bool
	Outgoing     []graphEdge
}

type propagationGraph struct {
	States  [][]*graphState
	Payload map[graphEdge]bool
	Edges   map[graphEdge]bool
}

func planningPaths(latency [][]time.Duration) ([][]time.Duration, [][]int) {
	n := len(latency)
	d, next := make([][]time.Duration, n), make([][]int, n)
	for i := range d {
		d[i] = append([]time.Duration(nil), latency[i]...)
		next[i] = make([]int, n)
		for j := range d[i] {
			next[i][j] = j
			if i != j && d[i][j] == 0 {
				d[i][j] = time.Nanosecond
			}
		}
	}
	for k := 0; k < n; k++ {
		for i := 0; i < n; i++ {
			for j := 0; j < n; j++ {
				if d[i][k]+d[k][j] < d[i][j] {
					d[i][j], next[i][j] = d[i][k]+d[k][j], next[i][k]
				}
			}
		}
	}
	return d, next
}

func buildPropagation(latency [][]time.Duration, rs []Requirement, budgets []time.Duration, leaders []int, voters int) ([]propagationGraph, error) {
	n := len(rs)
	d, next := planningPaths(latency)
	graphs := make([]propagationGraph, n)
	type triangle struct {
		a, w int
		cost time.Duration
	}
	for p := 0; p < n; p++ {
		leader := leaders[p]
		g := &graphs[p]
		g.Payload, g.Edges = make(map[graphEdge]bool), make(map[graphEdge]bool)
		states := make([]map[time.Duration]*graphState, n)
		for v := range states {
			s := &graphState{Knowledge: make([]uint64, n), Remote: make([]time.Duration, n), Dependencies: make(map[graphEdge]bool)}
			if v == p && p < voters {
				s.Knowledge[p] = 1 << p
			}
			states[v] = map[time.Duration]*graphState{0: s}
		}
		previous := func(v int, at time.Duration) *graphState {
			var best *graphState
			for t, s := range states[v] {
				if t <= at && (best == nil || t > best.Time) {
					best = s
				}
			}
			return best
		}
		walk := func(checkpoints []int, cost time.Duration, payload bool) error {
			node, at := p, time.Duration(0)
			slack := time.Duration(0)
			if cost <= budgets[p] {
				slack = budgets[p] - cost
			}
			for _, target := range checkpoints {
				for hops := 0; node != target; hops++ {
					if hops >= n {
						return fmt.Errorf("cyclic shortest route %d/%d", node, target)
					}
					src, dst := node, next[node][target]
					deadline, sendAt := at+slack, at
					found := false
					for edge := range g.Edges {
						if edge.From == src && edge.To == dst && edge.Time >= at && edge.Time <= deadline && (!found || edge.Time < sendAt) {
							sendAt, found = edge.Time, true
						}
					}
					edge := graphEdge{src, dst, sendAt}
					source := states[src][sendAt]
					if source == nil {
						return fmt.Errorf("missing graph source state")
					}
					if !found {
						g.Edges[edge] = true
						source.Outgoing = append(source.Outgoing, edge)
						if payload {
							g.Payload[edge] = true
						}
					}
					link := latency[src][dst]
					if link == 0 {
						link = time.Nanosecond
					}
					at, slack, node = sendAt+link, deadline-sendAt, dst
					dest := states[dst][at]
					if dest == nil {
						prev := previous(dst, at)
						dest = &graphState{Time: at, Knowledge: append([]uint64(nil), prev.Knowledge...), Remote: append([]time.Duration(nil), prev.Remote...), Dependencies: make(map[graphEdge]bool)}
						if dst < voters {
							dest.Knowledge[dst] |= 1 << dst
						}
						dest.Remote[dst] = at
						states[dst][at] = dest
					}
					dest.Dependencies[edge] = true
					dest.Knowledge[dst] |= source.Knowledge[src]
					for w := 0; w < n; w++ {
						dest.Knowledge[w] |= source.Knowledge[w]
						if source.Remote[w] > dest.Remote[w] {
							dest.Remote[w] = source.Remote[w]
						}
					}
					for t, future := range states[dst] {
						if t <= at {
							continue
						}
						for w := 0; w < n; w++ {
							future.Knowledge[w] |= dest.Knowledge[w]
							if dest.Remote[w] > future.Remote[w] {
								future.Remote[w] = dest.Remote[w]
							}
						}
					}
				}
			}
			return nil
		}
		// Install the value tree first. Long paths establish reusable relay edges.
		var values, triangles []triangle
		for a := 0; a < n; a++ {
			if a != p {
				values = append(values, triangle{a: a, cost: d[p][a]})
			}
			for w := 0; w < n; w++ {
				if rs[p][w]&(1<<a) != 0 {
					triangles = append(triangles, triangle{a, w, d[p][a] + d[a][w] + d[w][leader]})
				}
			}
		}
		order := func(xs []triangle) {
			sort.Slice(xs, func(i, j int) bool {
				if xs[i].cost != xs[j].cost {
					return xs[i].cost > xs[j].cost
				}
				if xs[i].w != xs[j].w {
					return xs[i].w > xs[j].w
				}
				return xs[i].a > xs[j].a
			})
		}
		order(values)
		order(triangles)
		for _, t := range values {
			if err := walk([]int{t.a}, t.cost, true); err != nil {
				return nil, err
			}
		}
		for _, t := range triangles {
			if previous(leader, budgets[p]).Knowledge[t.w]&(1<<t.a) != 0 {
				continue
			}
			if err := walk([]int{t.a, t.w, leader}, t.cost, false); err != nil {
				return nil, err
			}
		}
		g.States = make([][]*graphState, n)
		for v := range states {
			for _, s := range states[v] {
				g.States[v] = append(g.States[v], s)
			}
			sort.Slice(g.States[v], func(i, j int) bool { return g.States[v][i].Time < g.States[v][j].Time })
			for _, s := range g.States[v] {
				sort.Slice(s.Outgoing, func(i, j int) bool { return s.Outgoing[i].To < s.Outgoing[j].To })
			}
		}
		last := g.States[leader][len(g.States[leader])-1]
		if last.Time != budgets[p] {
			return nil, fmt.Errorf("graph commit time %s differs from budget %s", last.Time, budgets[p])
		}
		for w, required := range rs[p] {
			if required & ^last.Knowledge[w] != 0 {
				return nil, fmt.Errorf("graph misses witness %d for proposer %d", w, p)
			}
			ss := g.States[w]
			final := ss[len(ss)-1]
			if w < voters && ((required != 0 && last.Remote[w] != final.Time) || (required == 0 && len(ss) != 2 && w != p)) {
				return nil, fmt.Errorf("graph cannot use final-state adoption for proposer %d witness %d", p, w)
			}
		}
	}
	return graphs, nil
}

func (g propagationGraph) state(node int, at time.Duration) *graphState {
	ss := g.States[node]
	i := sort.Search(len(ss), func(i int) bool { return ss[i].Time >= at })
	if i < len(ss) && ss[i].Time == at {
		return ss[i]
	}
	return nil
}
