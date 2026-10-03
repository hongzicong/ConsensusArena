package kcensus

import (
	"math/bits"
	"sort"
	"time"

	"github.com/hongzicong/ConsensusArena/state"
)

type graphRun struct {
	Cursor   int
	Started  bool
	NewValue bool
	Received map[graphEdge]bool
	Pending  []message
}

// A replica that has already executed a slot still forwards a late command
// along the payload tree. This preserves dissemination without casting a vote
// in an old slot; it is upstream's SpreadValueOnly behavior.
func (c *core) relayPayload(m message) {
	p := m.Proposer
	if p < 0 || p >= c.m || m.Value == nil || m.Value.Proposer != p {
		return
	}
	g := c.plan.Graphs[p]
	edge := graphEdge{m.From, c.id, time.Duration(m.GraphTime)}
	if !g.Payload[edge] {
		return
	}
	if m.Slot == 0 {
		id := payloadEdgeID{p, edge, m.ID}
		if c.payloadSeen == nil {
			c.payloadSeen = make(map[payloadEdgeID]bool)
		}
		if c.payloadSeen[id] {
			return
		}
		c.payloadSeen[id] = true
	} else {
		x := c.slot(m.Key, m.Slot)
		if x.payloadRelayed == nil {
			x.payloadRelayed = make(map[graphEdge]bool)
		}
		if x.payloadRelayed[edge] {
			return
		}
		x.payloadRelayed[edge] = true
	}
	for _, s := range g.States[c.id] {
		if !s.Dependencies[edge] {
			continue
		}
		for _, out := range s.Outgoing {
			if g.Payload[out] {
				c.send(out.To, message{Kind: offer, Key: m.Key, Slot: m.Slot, ID: m.ID, Proposer: p, GraphTime: int64(out.Time), Value: m.Value})
			}
		}
		break
	}
}

func (x *slotState) graph(p int) *graphRun {
	if x.graphs == nil {
		x.graphs = make(map[int]*graphRun)
	}
	if x.graphs[p] == nil {
		x.graphs[p] = &graphRun{Received: make(map[graphEdge]bool)}
	}
	return x.graphs[p]
}

func (c *core) graphValue(k state.Key, i uint64, x *slotState, v *Value) {
	if x.values == nil {
		x.values = make(map[int]*Value)
	}
	if old := x.values[v.Proposer]; old != nil && !sameValue(old, v) {
		panic("proposer changed its fast value within a slot")
	}
	x.values[v.Proposer] = v
	x.participants |= 1 << v.Proposer
	for _, r := range v.Records {
		c.remember(r)
	}
	if x.fast == nil && !x.frozen && bits.OnesCount64(x.participants) == 1 {
		x.fast = v
		copy(x.knowledge, c.plan.Graphs[v.Proposer].States[c.id][0].Knowledge)
		c.saveOwnState(x)
	}
	if bits.OnesCount64(x.participants) > 1 {
		c.freezeConflict(k, i, x)
	}
}

func (c *core) startGraph(k state.Key, i uint64, x *slotState, v *Value, newValue bool) {
	c.graphValue(k, i, x, v)
	r := x.graph(v.Proposer)
	if r.Started {
		return
	}
	r.Started = true
	r.NewValue = newValue
	c.emitGraph(k, i, x, v.Proposer, c.plan.Graphs[v.Proposer].States[c.id][0])
	c.advanceGraph(k, i, x, v.Proposer)
}

func (c *core) receiveGraph(m message, x *slotState) {
	p := m.Proposer
	if p < 0 || p >= c.m {
		return
	}
	edge := graphEdge{m.From, c.id, time.Duration(m.GraphTime)}
	g := c.plan.Graphs[p]
	if !g.Edges[edge] {
		return
	}
	r := x.graph(p)
	if r.Received[edge] {
		return
	}
	if m.Value != nil {
		if m.Value.Proposer != p {
			return
		}
		// Like upstream, discover the sender's conflict before trying the
		// first local acceptance, rather than accepting and then freezing it.
		if m.Explicit {
			x.participants |= (m.Mask & ((uint64(1) << c.m) - 1)) | (uint64(1) << p)
			for _, report := range m.Reports {
				c.mergeReport(x, report)
			}
			c.freezeConflict(m.Key, m.Slot, x)
		}
		c.graphValue(m.Key, m.Slot, x, m.Value)
	}
	if x.values[p] == nil {
		// Evidence cannot authorize acceptance, commitment or inferred reports
		// before the payload has arrived. Keep each edge once while waiting.
		for _, pending := range r.Pending {
			if pending.From == m.From && pending.GraphTime == m.GraphTime {
				return
			}
		}
		r.Pending = append(r.Pending, m)
		return
	}
	if !r.Started {
		r.NewValue = m.NewValue
	}
	r.Started = true
	c.consumeGraph(m, x, edge)
	// Payload-tree and evidence edges can arrive in either order.
	pending := r.Pending
	r.Pending = nil
	for _, old := range pending {
		e := graphEdge{old.From, c.id, time.Duration(old.GraphTime)}
		if !r.Received[e] {
			c.consumeGraph(old, x, e)
		}
	}
	c.advanceGraph(m.Key, m.Slot, x, p)
	c.tryGraphCensus(m.Key, m.Slot, x)
}

func (c *core) consumeGraph(m message, x *slotState, edge graphEdge) {
	p := m.Proposer
	x.graph(p).Received[edge] = true
	x.participants |= 1 << p
	if m.Explicit {
		x.participants |= m.Mask & ((uint64(1) << c.m) - 1)
		for _, report := range m.Reports {
			c.mergeReport(x, report)
		}
		// An explicit snapshot means the sender left this graph's predicted
		// knowledge schedule. Freeze before advancing local knowledge.
		c.freezeConflict(m.Key, m.Slot, x)
	} else {
		g := c.plan.Graphs[p]
		source := g.state(m.From, edge.Time)
		for w, at := range source.Remote {
			if w >= c.n {
				continue
			}
			ss := g.States[w]
			if at != 0 || w == p {
				state := g.state(w, at)
				ballot := uint64(0)
				if at == ss[len(ss)-1].Time {
					ballot = uint64(c.n + c.plan.Leaders[p])
				}
				c.mergeReport(x, nodeReport{From: w, Proposer: p, Ballot: ballot, Mask: state.Knowledge[w], Time: int64(at), Value: x.values[p]})
			}
		}
		if bits.OnesCount64(x.participants) > 1 {
			c.freezeConflict(m.Key, m.Slot, x)
		}
	}
}

func (c *core) advanceGraph(k state.Key, i uint64, x *slotState, p int) {
	r, g := x.graph(p), c.plan.Graphs[p]
	states := g.States[c.id]
	for r.Cursor+1 < len(states) {
		next := states[r.Cursor+1]
		ready := true
		for dependency := range next.Dependencies {
			if !r.Received[dependency] {
				ready = false
				break
			}
		}
		if !ready {
			break
		}
		r.Cursor++
		c.stats.GraphStates++
		if !x.frozen && x.fast != nil && x.fast.Proposer == p {
			copy(x.knowledge, next.Knowledge)
			x.knowledgeTime = next.Time
			c.saveOwnState(x)
			c.stats.KnowledgeUpdates++
			if r.Cursor == len(states)-1 {
				c.freezeFor(x, uint64(c.n+c.plan.Leaders[p]))
			}
		}
		// Propagation progresses even when acceptance knowledge is frozen.
		c.emitGraph(k, i, x, p, next)
	}
	if c.id == c.plan.Leaders[p] && r.Cursor == len(states)-1 && !x.conflictAnnounced && bits.OnesCount64(x.participants) == 1 && x.promised <= uint64(c.n+c.plan.Leaders[p]) && c.canCommit(x) {
		v := x.fast
		c.stats.FastCommits++
		c.broadcastCommit(message{Kind: commit, Key: k, Slot: i, Value: v, References: true})
		c.learn(k, i, v)
	}
}

func (c *core) emitGraph(k state.Key, i uint64, x *slotState, p int, s *graphState) {
	// Normal completion-freezing is implied by the schedule. Conflicts,
	// explicit Prepare and phase 2 require real frozen-state snapshots.
	explicit := x.conflictAnnounced || x.acceptedBallot != 0 || x.fast == nil || x.fast.Proposer != p || x.promised > uint64(c.n+c.plan.Leaders[p])
	var reports []nodeReport
	if explicit {
		ids := make([]int, 0, len(x.nodeReports))
		for id := range x.nodeReports {
			ids = append(ids, id)
		}
		sort.Ints(ids)
		for _, id := range ids {
			reports = append(reports, x.nodeReports[id])
		}
	}
	for _, edge := range s.Outgoing {
		newValue := x.graph(p).NewValue
		m := message{Kind: spread, Key: k, Slot: i, Proposer: p, GraphTime: int64(edge.Time), Explicit: explicit, Reports: reports, Mask: x.participants, Value: x.values[p], NewValue: newValue, References: true}
		if newValue && c.plan.Graphs[p].Payload[edge] {
			m.References = false
			c.stats.GraphPayloads++
		}
		c.stats.GraphMessages++
		if explicit {
			c.stats.NodeStateRelays += uint64(len(reports))
			for _, r := range reports {
				if r.Ballot > 0 {
					c.stats.FrozenRelays++
				}
			}
		}
		c.send(edge.To, m)
	}
}
