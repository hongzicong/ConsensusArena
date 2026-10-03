package kcensus

// Protocol recovery transitions and recovery-specific helpers.
// Node-state ordering and adoption follow upstream node_state.rs and
// round_state.rs at b232c332. See LICENSE.upstream for the MIT license.
import (
	"encoding/json"
	"math/bits"
	"sort"
	"time"

	"github.com/hongzicong/ConsensusArena/replicaset"
	"github.com/hongzicong/ConsensusArena/state"
)

type heartbeatProbe struct {
	sequence uint64
	sent     time.Duration
}

// Receipt of queued protocol traffic is not proof of current liveness. Only a
// response to our own recent probe refreshes suspicion, at the probe's send time.
func (c *core) receiveHeartbeat(m message) {
	if m.Digest != c.plan.Digest {
		panic("KCensus requirement fingerprint mismatch")
	}
	if !m.Explicit {
		c.send(m.From, message{Kind: heartbeat, Digest: c.plan.Digest, High: m.High, Explicit: true})
		return
	}
	p := c.probes[m.High%uint64(len(c.probes))]
	if m.High == 0 || p.sequence != m.High || c.now-p.sent >= c.failure || c.peerFailed(m.From) {
		c.stats.StaleHeartbeatReplies++
		return
	}
	if p.sent > c.heard[m.From] {
		c.heard[m.From] = p.sent
	}
	c.stats.HeartbeatReplies++
}

func (c *core) peerFailed(id int) bool { return len(c.failed) != 0 && c.failed[id] }

// In the deployed crash-stop, lossless-stream model, an established stream
// remains eligible until its reader/writer observes failure. Suspicion based on
// a congested data stream must not elect a competing fallback leader. The pure
// transition model (without established transports) uses matched probes.
func (c *core) markConnected(id int) {
	if c.connected == nil {
		return
	}
	if id >= 0 && id < c.m && !c.peerFailed(id) {
		c.connected[id] = true
	}
}

func (c *core) peerEligible(id int) bool {
	if c.peerFailed(id) {
		return false
	}
	if c.connected != nil && c.connected[id] {
		return true
	}
	return c.now-c.heard[id] < c.failure
}

// The deployed model is crash-stop: a definitively failed stream cannot be
// revived by frames already queued before its failure. It never changes votes.
func (c *core) markFailed(id int) {
	if id == c.id || id < 0 || id >= c.m {
		return
	}
	if c.failed == nil {
		c.failed = make([]bool, c.m)
	}
	if !c.failed[id] {
		c.failed[id] = true
		c.stats.FailedPeers++
		if c.failedGraphs == nil {
			c.failedGraphs = make([]bool, c.m)
		}
		for p, g := range c.plan.Graphs {
			if c.plan.Leaders[p] == id || c.plan.Requirements[p].Quorum().Contains(id) {
				c.failedGraphs[p] = true
				continue
			}
			for edge := range g.Edges {
				if edge.From == id || edge.To == id {
					c.failedGraphs[p] = true
					break
				}
			}
		}
	}
}

func (c *core) graphUsable(p int) bool { return c.failedGraphs == nil || !c.failedGraphs[p] }

// Periodic diagnostics only; never used as protocol evidence.
func (c *core) recoverySnapshot() string {
	type evidence struct {
		Voter                        int
		Promise, Accepted, UID, Mask uint64
		Proposer                     int
		Time                         int64
	}
	type head struct {
		Key                   state.Key `json:"key"`
		Slot                  uint64    `json:"slot"`
		AgeMS                 int64     `json:"age_ms"`
		Phase                 uint8     `json:"phase"`
		Ballot, Promise       uint64
		Reports, Frozen, Acks int
		Fast, Selected        uint64
		SelectedSlot          uint64
		Participants          uint64
		Evidence              []evidence
	}
	status := struct {
		Coordinator  int     `json:"coordinator"`
		HeardMS      []int64 `json:"heard_ms"`
		Phases       [3]int  `json:"head_phases"`
		Quorums      int     `json:"head_quorums"`
		PayloadWaits int     `json:"payload_waits"`
		Oldest       []head  `json:"oldest"`
	}{Coordinator: c.coordinator(), PayloadWaits: len(c.payloadWaiting)}
	for _, at := range c.heard {
		status.HeardMS = append(status.HeardMS, (c.now - at).Milliseconds())
	}
	for id, x := range c.active {
		if id.Slot != c.executed(id.Key)+1 {
			continue
		}
		status.Phases[x.phase]++
		if c.graphPromiseQuorum(x) {
			status.Quorums++
		}
		h := head{Key: id.Key, Slot: id.Slot, AgeMS: (c.now - x.created).Milliseconds(), Phase: x.phase, Ballot: x.ballot, Promise: x.promised, Reports: len(x.nodeReports), Acks: bits.OnesCount64(x.acks), Participants: x.participants}
		for _, r := range x.nodeReports {
			if r.Ballot == x.ballot {
				h.Frozen++
			}
		}
		if x.fast != nil {
			h.Fast = x.fast.UID
		}
		if x.selected != nil {
			h.Selected = x.selected.UID
			if x.selected.Batch != nil {
				h.SelectedSlot = x.selected.Batch.Slot
			}
		}
		at := 0
		for at < len(status.Oldest) && (status.Oldest[at].AgeMS > h.AgeMS || status.Oldest[at].AgeMS == h.AgeMS && status.Oldest[at].Key < h.Key) {
			at++
		}
		if at < 8 {
			for voter, r := range x.nodeReports {
				e := evidence{Voter: voter, Promise: r.Ballot, Accepted: r.AcceptedBallot, Mask: r.Mask, Proposer: r.Proposer, Time: r.Time}
				if r.Value != nil {
					e.UID = r.Value.UID
				}
				h.Evidence = append(h.Evidence, e)
			}
			sort.Slice(h.Evidence, func(i, j int) bool { return h.Evidence[i].Voter < h.Evidence[j].Voter })
			status.Oldest = append(status.Oldest, head{})
			copy(status.Oldest[at+1:], status.Oldest[at:])
			status.Oldest[at] = h
			if len(status.Oldest) > 8 {
				status.Oldest = status.Oldest[:8]
			}
		}
	}
	b, _ := json.Marshal(status)
	return string(b)
}

func (c *core) reproposer() int {
	for _, i := range c.priority {
		if i == c.id || c.peerEligible(i) {
			return i
		}
	}
	return c.id
}

func (c *core) coordinator() int {
	for _, i := range c.priority {
		if i >= c.n {
			continue
		}
		if i == c.id || c.peerEligible(i) {
			return i
		}
	}
	return c.priority[0]
}

func (c *core) startRecovery(k state.Key, i uint64) {
	if c.id >= c.n {
		return
	}
	x := c.slot(k, i)
	if x.decided != nil {
		return
	}
	b := uint64(c.n + c.id)
	if x.promised > b {
		b = (x.promised/uint64(c.n)+1)*uint64(c.n) + uint64(c.id)
	}
	if b <= x.ballot {
		b = (x.ballot/uint64(c.n)+1)*uint64(c.n) + uint64(c.id)
	}
	c.beginCensus(k, i, x, b)
	c.stats.TimeoutCensuses++
	c.sendMissingPrepare(k, i, x)
}

func completeReportPayload(v *Value) bool { return v == nil || !v.Reference && len(v.Records) > 0 }

// Retry only missing responses. Graph-derived UID-only reports still need a
// full response; every new ballot resets reports and therefore this filter.
func (c *core) sendMissingPrepare(k state.Key, i uint64, x *slotState) {
	for voter := 0; voter < c.n; voter++ {
		r, received := x.reports[voter]
		if c.peerFailed(voter) || received && r.Ballot == x.ballot && completeReportPayload(r.Value) && completeReportPayload(r.Fast) {
			c.stats.SuppressedRecoveryRequests++
			continue
		}
		c.send(voter, message{Kind: prepare, Key: k, Slot: i, Ballot: x.ballot})
	}
}

func (c *core) sendMissingAccept(k state.Key, i uint64, x *slotState) {
	m := c.valuePayload(message{Kind: accept, Key: k, Slot: i, Ballot: x.ballot, Value: x.selected})
	for voter := 0; voter < c.n; voter++ {
		if c.peerFailed(voter) || x.acks&(uint64(1)<<voter) != 0 {
			c.stats.SuppressedRecoveryRequests++
			continue
		}
		c.send(voter, m)
	}
}

func (c *core) beginCensus(k state.Key, i uint64, x *slotState, b uint64) {
	x.ballot = b
	x.phase = 1
	x.reports = make(map[int]message)
	x.acks = 0
	x.selected = nil
	x.lastSend = c.now
	x.nextRetry = c.now + c.retry
	x.retryDelay = c.retry
	c.stats.Prepares++
}

// Conflict dissemination already solicits the initial frozen census. Do not
// reset an active census or reuse an initial ballot after it was preempted.
func (c *core) initialCensus(k state.Key, i uint64, x *slotState) {
	b := uint64(c.n + c.id)
	if x.phase == 0 && x.ballot == 0 && x.promised <= b {
		c.beginCensus(k, i, x, b)
		c.stats.InitialCensuses++
	}
}

func (c *core) repairPrefix(k state.Key, high uint64, waitingRead bool) {
	s := c.shard(k)
	if high > s.high {
		s.high = high
	}
	if s.executed < s.high {
		c.markGap(k)
	}
	if s.executed >= high {
		return
	}
	i := s.executed + 1
	x := c.slot(k, i)
	immediate := waitingRead && !x.fetchSent && c.now < x.nextRetry
	if immediate && (x.local != nil || x.fast != nil || x.classic != nil || x.phase > 0) {
		// A ready-read fence names a prefix still in flight; it is not proof
		// that its ordinary lossless Commit was lost. Let the existing slot
		// timer expire before requesting another complete catch-up object.
		c.stats.DeferredInFlightFetches++
		immediate = false
	}
	if (immediate || c.now >= x.nextRetry) && c.retryBudget > 0 {
		c.deferRetry(x)
		if c.fetchPrefix(k, i, x, c.coordinator(), immediate) {
			c.retryBudget--
		}
	}
}

// The deployed transport retains a frame until it is written or the stream
// definitively fails; there is no reconnect. One Fetch makes this slot active at
// the coordinator, which drives its own census timer and returns a full decision.
// Repeating it while that same stream is live only creates catch-up backlog.
// Pure transition models without established streams retain timed retries.
func (c *core) reliableTarget(id int) bool {
	return c.connected != nil && !c.peerFailed(id) && (id == c.id || c.connected[id])
}

func (c *core) fetchPrefix(k state.Key, i uint64, x *slotState, target int, immediate bool) bool {
	if x.fetchSent && x.fetchTarget == target && c.reliableTarget(target) {
		c.stats.SuppressedFetches++
		return false
	}
	x.fetchSent, x.fetchTarget = true, target
	c.stats.Fetches++
	if immediate {
		c.stats.ImmediateFetches++
	} else {
		c.stats.DeadlineFetches++
	}
	c.send(target, message{Kind: fetch, Key: k, Slot: i})
	return true
}

func (c *core) markGap(k state.Key) {
	c.gaps[k] = true
	if c.id >= c.n {
		return
	}
	s := c.shard(k)
	if !s.gapScheduled {
		s.gapScheduled = true
		c.gapQueue = append(c.gapQueue, k)
	}
}

func (c *core) deferRetry(x *slotState) {
	x.lastSend = c.now
	x.nextRetry = c.now + x.retryDelay
	x.retryDelay *= 2
	if x.retryDelay > 2*time.Second {
		x.retryDelay = 2 * time.Second
	}
}

// Upstream NodeState ordering: classic acceptance, promise, graph state, value.
func newerReport(a, b nodeReport) bool {
	if a.AcceptedBallot != b.AcceptedBallot {
		return a.AcceptedBallot > b.AcceptedBallot
	}
	if a.Ballot != b.Ballot {
		return a.Ballot > b.Ballot
	}
	if a.Time != b.Time {
		return a.Time > b.Time
	}
	return a.Value != nil && b.Value == nil
}

func promiseFromReport(k state.Key, slot uint64, r nodeReport) message {
	m := message{Kind: promise, From: r.From, Key: k, Slot: slot, Ballot: r.Ballot, AcceptedBallot: r.AcceptedBallot, Mask: r.Mask, StateTime: r.Time}
	if r.AcceptedBallot > 0 {
		m.Value = r.Value
	} else {
		m.Fast = r.Value
	}
	return m
}

// The native runtime uses every known state for selection after a majority has
// promised this ballot. Unfrozen states discover candidates but cannot refute one.
func (c *core) adoptGraph(x *slotState) *Value {
	var highest uint64
	var chosen *Value
	for _, r := range x.nodeReports {
		if r.AcceptedBallot > highest {
			highest, chosen = r.AcceptedBallot, r.Value
		} else if highest > 0 && r.AcceptedBallot == highest && !sameValue(chosen, r.Value) {
			panic("two values accepted at one Paxos ballot")
		}
	}
	if highest > 0 {
		return chosen
	}
	proposers := make([]int, 0, len(x.values))
	for p := range x.values {
		proposers = append(proposers, p)
	}
	sort.Ints(proposers)
	for _, p := range proposers {
		g := c.plan.Graphs[p]
		leader := c.plan.Leaders[p]
		end := g.States[leader][len(g.States[leader])-1]
		quorum := end.Knowledge[leader]
		possible := true
		for voter, r := range x.nodeReports {
			if r.Ballot == 0 {
				continue
			}
			if r.Proposer != p {
				knowledge := replicaset.New(voter)
				if r.Proposer >= 0 {
					knowledge = c.plan.Graphs[r.Proposer].state(voter, time.Duration(r.Time)).Knowledge[voter]
				}
				if knowledge&quorum != 0 {
					possible = false
					break
				}
			} else if r.Time != int64(g.States[voter][len(g.States[voter])-1].Time) {
				possible = false
				break
			}
		}
		if possible {
			return x.values[p]
		}
	}
	return nil
}

func (c *core) graphPromiseQuorum(x *slotState) bool {
	var voters uint64
	for voter, r := range x.nodeReports {
		if voter < c.n && r.Ballot == x.ballot {
			voters |= uint64(1) << voter
		}
	}
	return bits.OnesCount64(voters) >= c.n-c.f
}

func (c *core) freezeFor(x *slotState, ballot uint64) {
	x.frozen = true
	if ballot > x.promised {
		x.promised = ballot
	}
	c.saveOwnState(x)
}

func (c *core) saveOwnState(x *slotState) {
	if c.id >= c.n {
		return
	}
	r := nodeReport{From: c.id, Proposer: -1, Ballot: x.promised, AcceptedBallot: x.acceptedBallot}
	if x.acceptedBallot > 0 {
		r.Value = x.classic
	} else if x.fast != nil {
		r.Proposer, r.Value, r.Mask = x.fast.Proposer, x.fast, uint64(x.knowledge[c.id])
		r.Time = int64(x.knowledgeTime)
	}
	if x.nodeReports == nil {
		x.nodeReports = make(map[int]nodeReport)
	}
	x.nodeReports[c.id] = r
}

func (c *core) mergeReport(x *slotState, r nodeReport) {
	if r.From < 0 || r.From >= c.n || r.From == c.id || r.Proposer < -1 || r.Proposer >= c.m || r.AcceptedBallot > r.Ballot {
		return
	}
	if r.Proposer >= 0 {
		s := c.plan.Graphs[r.Proposer].state(r.From, time.Duration(r.Time))
		if s == nil || uint64(s.Knowledge[r.From]) != r.Mask || r.Value == nil || r.Value.Proposer != r.Proposer || r.AcceptedBallot != 0 {
			return
		}
	} else if r.Mask != 0 || r.Time != 0 || (r.AcceptedBallot > 0 && r.Value == nil) {
		return
	}
	if x.nodeReports == nil {
		x.nodeReports = make(map[int]nodeReport)
	}
	old, exists := x.nodeReports[r.From]
	if exists && !newerReport(r, old) {
		return
	}
	x.nodeReports[r.From] = r
	c.enqueueValue(r.Value)
	if r.Proposer >= 0 {
		if x.values == nil {
			x.values = make(map[int]*Value)
		}
		if old := x.values[r.Proposer]; old != nil && !sameValue(old, r.Value) {
			panic("fast proposer changed value")
		}
		x.values[r.Proposer] = r.Value
		x.participants |= 1 << r.Proposer
	} else if r.AcceptedBallot > 0 {
		c.cacheClassic(x, message{AcceptedBallot: r.AcceptedBallot, Value: r.Value})
	}
}

func (c *core) cacheClassic(x *slotState, m message) {
	ballot := m.AcceptedBallot
	if m.Kind == accept {
		ballot = m.Ballot
	}
	if ballot == 0 || m.Value == nil {
		return
	}
	if x.classicValues == nil {
		x.classicValues = make(map[uint64]*Value)
	}
	if old := x.classicValues[ballot]; old != nil && !sameValue(old, m.Value) {
		panic("classic ballot changed value")
	}
	x.classicValues[ballot] = m.Value
}

func (c *core) cachePromise(x *slotState, m message) {
	if m.Fast != nil {
		if x.values == nil {
			x.values = make(map[int]*Value)
		}
		if old := x.values[m.Fast.Proposer]; old != nil && !sameValue(old, m.Fast) {
			panic("fast proposer changed value")
		}
		x.values[m.Fast.Proposer] = m.Fast
	}
	p := -1
	if m.Fast != nil {
		p = m.Fast.Proposer
	}
	v := m.Fast
	if m.AcceptedBallot > 0 {
		v = m.Value
		p = -1
	}
	c.mergeReport(x, nodeReport{From: m.From, Proposer: p, Ballot: m.Ballot, AcceptedBallot: m.AcceptedBallot, Mask: m.Mask, Time: m.StateTime, Value: v})
}

func (c *core) tryGraphCensus(k state.Key, i uint64, x *slotState) {
	if x.decided != nil || !x.conflictAnnounced || x.participants == 0 || c.conflictLeader(x.participants) != c.id {
		return
	}
	b := uint64(c.n + c.id)
	if x.promised > b || x.ballot > b || x.phase == 2 {
		return
	}
	c.initialCensus(k, i, x)
	if x.phase != 1 || x.ballot != b {
		return
	}
	for voter, r := range x.nodeReports {
		if r.Ballot != b {
			continue
		}
		m := promiseFromReport(k, i, r)
		if old, ok := x.reports[voter]; ok && old.AcceptedBallot >= r.AcceptedBallot {
			continue
		}
		x.reports[voter] = m
	}
	c.completeCensus(k, i, x)
}

func (c *core) completeCensus(k state.Key, i uint64, x *slotState) {
	if x.phase != 1 || x.promised > x.ballot || !c.graphPromiseQuorum(x) {
		return
	}
	v := c.adoptGraph(x)
	if v == nil {
		v = c.conflictBatch(k, x)
	}
	if v == nil || len(v.Records) == 0 && !v.Reference {
		return
	}
	resolved, ready := c.resolveValue(k, v)
	if !ready {
		c.waitPayload(message{Kind: resumeCensus, From: c.id, Key: k, Slot: i, Ballot: x.ballot, Value: v})
		return
	}
	v = resolved
	x.selected, x.phase, x.acks, x.lastSend = v, 2, 0, c.now
	c.sendMissingAccept(k, i, x)
}

func (c *core) freezeConflict(k state.Key, i uint64, x *slotState) {
	if !x.conflictAnnounced {
		c.stats.Conflicts++
	}
	x.conflictAnnounced = true
	if x.participants == 0 {
		c.freezeFor(x, x.promised)
		return
	}
	c.freezeFor(x, uint64(c.n+c.conflictLeader(x.participants)))
}

// Called only after the census rules out every fast value and has no classic
// acceptance. An adopted value must never be extended or replaced.
func (c *core) conflictBatch(k state.Key, x *slotState) *Value {
	c.enqueueValue(x.local)
	for p := 0; p < c.m; p++ {
		c.enqueueValue(x.values[p])
		c.enqueueValue(x.reports[p].Fast)
	}
	return c.makePendingValue(k, c.pendingRecords(k, nil))
}

func (c *core) conflictLeader(participants uint64) int {
	leader := -1
	for p := 0; p < c.m; p++ {
		if participants&(uint64(1)<<p) != 0 && c.plan.Leaders[p] > leader {
			leader = c.plan.Leaders[p]
		}
	}
	return leader
}
