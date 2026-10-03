package fastpaxos

// Protocol state and normal-case transitions.
import (
	"bytes"
	"container/list"
	"fmt"
	"math/bits"
	"time"

	"github.com/hongzicong/ConsensusArena/replica/defs"
	"github.com/hongzicong/ConsensusArena/replicaset"
	"github.com/hongzicong/ConsensusArena/state"
)

type slotState struct {
	Accepted *entry
	Proposed *entry // coordinator plan; not a vote until Accept is processed
	Chosen   *entry
	Votes    map[uint64]map[int]entry
	Acks     replicaset.Set
	Local    *defs.RequestID
	Deferred *entry
}

type snapshot struct {
	Pages int
	High  int
	Parts map[int][]entry
}

type pendingCommand struct {
	ID      defs.RequestID
	RetryAt time.Duration
}

type core struct {
	id, n, fastSize                                    int
	fastMask                                           uint64
	fixed                                              bool
	promise                                            uint64
	active, preparing                                  bool
	start, high, next, executed                        int
	slots                                              map[int]*slotState
	known                                              map[defs.RequestID]record
	pending                                            map[defs.RequestID]*list.Element
	forwardOrder                                       list.List
	results                                            map[defs.RequestID]state.Value
	assigned                                           map[defs.RequestID]bool
	queue                                              []defs.RequestID
	snapshots                                          map[int]*snapshot
	frozen                                             []message
	frozenStart                                        int
	out                                                []envelope
	heard                                              []time.Duration
	now, lastProgress, lastBeat, lastRetry, lastRepair time.Duration
	retryCursor                                        int
	stalled                                            bool
	windowFallbacks, ageFallbacks                      uint64
	execute                                            func(record) state.Value
	complete                                           func(defs.RequestID, state.Value)
	elections, fastCommits, classicCommits, repaired   uint64
}

func newCore(id, n int, mask uint64, size int, fixed bool) *core {
	if n < 3 || n > 63 || n%2 == 0 || id < 0 || id >= n || size <= n/2 || size > n {
		panic("invalid FastPaxos membership/quorum")
	}
	if fixed && bits.OnesCount64(mask) != size || !fixed && size <= 3*n/4 {
		panic("invalid FastPaxos fast quorum")
	}
	return &core{id: id, n: n, fastMask: mask, fastSize: size, fixed: fixed,
		high: -1, executed: -1, slots: make(map[int]*slotState), known: make(map[defs.RequestID]record),
		pending: make(map[defs.RequestID]*list.Element), results: make(map[defs.RequestID]state.Value), assigned: make(map[defs.RequestID]bool), heard: make([]time.Duration, n)}
}

func (c *core) slot(s int) *slotState {
	if s < 0 {
		panic("negative slot")
	}
	if s > c.high {
		c.high = s
	}
	if c.slots[s] == nil {
		c.slots[s] = &slotState{Votes: make(map[uint64]map[int]entry)}
	}
	return c.slots[s]
}

func (c *core) send(to int, m message) { m.From = c.id; c.out = append(c.out, envelope{to, m}) }

func (c *core) broadcast(m message) {
	for i := 0; i < c.n; i++ {
		c.send(i, m)
	}
}

func (c *core) owner() int { return int(c.promise % uint64(c.n)) }

func (c *core) remember(v record) {
	if v.Noop {
		return
	}
	if old, ok := c.known[v.ID]; ok {
		if old.Command.Op != v.Command.Op || old.Command.K != v.Command.K || !bytes.Equal(old.Command.V, v.Command.V) {
			panic("command ID reused with different data")
		}
		return
	}
	c.known[v.ID] = v
}

func (c *core) submit(v record) {
	c.remember(v)
	// A learner may receive the decision before the client's payload.
	c.apply()
	if result, ok := c.results[v.ID]; ok {
		if c.complete != nil {
			c.complete(v.ID, result)
		}
		return
	}
	if c.pending[v.ID] != nil {
		return
	}
	if len(c.pending) == 0 {
		c.lastProgress = c.now
	}
	c.pending[v.ID] = c.forwardOrder.PushBack(pendingCommand{v.ID, c.now + failureTimeout})
	if c.promise == 0 {
		c.proposeFast(v)
	} else if c.active && c.owner() == c.id {
		c.enqueue(v.ID)
	}
}

func (c *core) forgetPending(id defs.RequestID) {
	if e := c.pending[id]; e != nil {
		c.forwardOrder.Remove(e)
		delete(c.pending, id)
	}
}

func (c *core) enqueue(id defs.RequestID) {
	if !c.assigned[id] {
		c.assigned[id] = true
		c.queue = append(c.queue, id)
	}
}

func (c *core) proposeFast(v record) {
	if c.next-c.executed-1 >= fastWindow {
		if !c.stalled {
			c.windowFallbacks++
		}
		c.stalled = true
		return // remains pending for the higher classic ballot
	}
	s := c.next
	c.next++
	st := c.slot(s)
	id := v.ID
	st.Local = &id
	if c.fastMask&(1<<c.id) != 0 {
		c.vote(entry{Slot: s, Round: 1, Value: v, Payload: true})
	}
}

func (c *core) vote(e entry) {
	if c.promise != 0 || c.fastMask&(1<<c.id) == 0 {
		return
	}
	s := c.slot(e.Slot)
	if s.Accepted != nil && !newer(e, *s.Accepted) {
		return
	}
	if !e.Value.Noop {
		v, ok := c.known[e.Value.ID]
		if !ok {
			s.Deferred = &e
			c.broadcast(message{Kind: msgFetch, Entries: []entry{e}})
			return
		}
		e.Value = v
	}
	e.Payload = true
	s.Accepted = &e
	s.Deferred = nil
	wire := e
	wire.Payload = false
	wire.Value.Command = state.Command{}
	c.broadcast(message{Kind: msgVote, Entries: []entry{wire}})
}

func (c *core) learn(e entry) {
	s := c.slot(e.Slot)
	if s.Chosen != nil && !sameValue(s.Chosen.Value, e.Value) {
		panic(fmt.Sprintf("conflicting decision in slot %d", e.Slot))
	}
	if e.Payload {
		c.remember(e.Value)
	}
	e.Decided = true
	s.Chosen = &e
	if !e.Value.Noop {
		c.forgetPending(e.Value.ID)
	}
	c.apply()
	if c.promise == 0 && s.Local != nil && !sameValue(record{ID: *s.Local}, e.Value) && c.pending[*s.Local] != nil {
		v := c.known[*s.Local]
		s.Local = nil
		c.proposeFast(v)
	}
}

func (c *core) flush() {
	if !c.active || c.owner() != c.id {
		return
	}
	var batch []entry
	for len(c.queue) > 0 && len(batch) < pageSize && c.next-c.executed < inflightWindow {
		id := c.queue[0]
		c.queue = c.queue[1:]
		if _, ok := c.results[id]; ok {
			continue
		}
		e := entry{Slot: c.next, Epoch: c.promise, Value: c.known[id], Payload: true}
		c.next++
		st := c.slot(e.Slot)
		st.Proposed = &e
		st.Acks = 0
		batch = append(batch, e)
	}
	if len(batch) > 0 {
		c.broadcast(message{Kind: msgAccept, Epoch: c.promise, Entries: batch})
	}
}

func (c *core) step(m message) {
	if m.From < 0 || m.From >= c.n {
		return
	}
	c.heard[m.From] = c.now
	switch m.Kind {
	case msgVote:
		if c.promise != 0 {
			return
		}
		for _, e := range m.Entries {
			if e.Epoch != 0 || e.Round == 0 || c.fastMask&(1<<m.From) == 0 {
				continue
			}
			s := c.slot(e.Slot)
			if s.Chosen != nil {
				continue
			}
			if s.Votes[e.Round] == nil {
				s.Votes[e.Round] = make(map[int]entry)
			}
			vs := s.Votes[e.Round]
			if _, ok := vs[m.From]; ok {
				continue
			}
			vs[m.From] = e
			if len(vs) < c.fastSize {
				continue
			}
			same := true
			maxID := -1
			var v entry
			for id, x := range vs {
				if !sameValue(x.Value, e.Value) {
					same = false
				}
				if id > maxID {
					maxID = id
					v = x
				}
			}
			if same {
				c.fastCommits++
				c.learn(e)
			} else if c.fixed {
				v.Round++
				c.vote(v)
			} else {
				c.stalled = true
			}
		}
	case msgPrepare:
		c.prepare(m)
	case msgPromise:
		c.promisePage(m)
	case msgAccept:
		if m.Epoch == 0 || int(m.Epoch%uint64(c.n)) != m.From {
			return
		}
		if m.Epoch < c.promise {
			c.send(m.From, message{Kind: msgNack, Epoch: c.promise})
			return
		}
		c.adopt(m.Epoch)
		c.active = true
		var acks []entry
		for _, e := range m.Entries {
			if e.Epoch != m.Epoch || !e.Payload {
				continue
			}
			c.remember(e.Value)
			s := c.slot(e.Slot)
			if s.Chosen != nil && !sameValue(s.Chosen.Value, e.Value) {
				panic("accept contradicts chosen")
			}
			if s.Accepted != nil && s.Accepted.Epoch == e.Epoch && !sameValue(s.Accepted.Value, e.Value) {
				panic("coordinator equivocation")
			}
			s.Accepted = &entry{Slot: e.Slot, Epoch: e.Epoch, Value: e.Value, Payload: true}
			acks = append(acks, entry{Slot: e.Slot, Epoch: e.Epoch})
		}
		c.send(m.From, message{Kind: msgAccepted, Epoch: m.Epoch, Entries: acks})
	case msgAccepted:
		if !c.active || c.owner() != c.id || m.Epoch != c.promise {
			return
		}
		var chosen []entry
		for _, e := range m.Entries {
			s := c.slots[e.Slot]
			if s == nil || s.Proposed == nil || s.Proposed.Epoch != m.Epoch || s.Chosen != nil {
				continue
			}
			s.Acks = s.Acks.With(m.From)
			if s.Acks.Size() >= c.n/2+1 {
				v := *s.Proposed
				if !v.Value.Noop {
					v.Value = c.known[v.Value.ID]
				}
				v.Payload = true
				v.Decided = true
				c.classicCommits++
				c.learn(v)
				chosen = append(chosen, v)
			}
		}
		if len(chosen) > 0 {
			c.broadcast(message{Kind: msgCommit, Entries: chosen})
		}
	case msgCommit:
		for _, e := range m.Entries {
			c.learn(e)
		}
	case msgHeartbeat:
		if m.Epoch > c.promise {
			c.adopt(m.Epoch)
		}
		// Ask the current leader for missing decisions, including old prefix slots.
		if c.promise > 0 && m.From == c.owner() && m.High > c.executed {
			c.send(m.From, message{Kind: msgFetch, Start: c.executed + 1, High: m.High})
		}
	case msgFetch:
		var es []entry
		for _, e := range m.Entries {
			if v, ok := c.known[e.Value.ID]; ok {
				es = append(es, entry{Value: v, Payload: true})
			}
		}
		if len(es) > 0 {
			c.send(m.From, message{Kind: msgData, Entries: es})
		}
		if len(m.Entries) == 0 {
			var ds []entry
			for i := m.Start; i <= m.High && i < m.Start+repairBudget; i++ {
				if s := c.slots[i]; s != nil && s.Chosen != nil {
					e := *s.Chosen
					if !e.Value.Noop {
						v, ok := c.known[e.Value.ID]
						if !ok {
							continue
						}
						e.Value = v
					}
					e.Payload = true
					ds = append(ds, e)
					if len(ds) == pageSize {
						c.send(m.From, message{Kind: msgCommit, Entries: ds})
						ds = nil
					}
				}
			}
			if len(ds) > 0 {
				c.send(m.From, message{Kind: msgCommit, Entries: ds})
			}
		}
	case msgData:
		for _, e := range m.Entries {
			if e.Payload {
				c.remember(e.Value)
			}
		}
		for _, s := range c.slots {
			if s.Deferred != nil {
				c.vote(*s.Deferred)
			}
		}
		c.apply()
	case msgForward:
		if c.promise == 0 || c.owner() != c.id || !c.active {
			return
		}
		for _, e := range m.Entries {
			if e.Payload {
				c.submit(e.Value)
			}
		}
	case msgNack:
		if m.Epoch > c.promise {
			c.adopt(m.Epoch)
		}
	case msgRecover:
		if c.promise == 0 {
			c.stalled = true
		}
	}
}

func (c *core) tick(now time.Duration) {
	c.now = now
	c.heard[c.id] = now
	if now-c.lastBeat >= 250*time.Millisecond {
		c.broadcast(message{Kind: msgHeartbeat, Epoch: c.promise, High: c.executed})
		c.lastBeat = now
	}
	alive := uint64(0)
	candidate := -1
	for i, last := range c.heard {
		if now-last < failureTimeout {
			alive |= 1 << i
			if candidate < 0 {
				candidate = i
			}
		}
	}
	// Before classic forwarding rotates the list, its head is the oldest
	// outstanding request. Execution of unrelated slots must not reset its age.
	if c.promise == 0 && !c.stalled {
		if e := c.forwardOrder.Front(); e != nil && now >= e.Value.(pendingCommand).RetryAt {
			c.stalled = true
			c.ageFallbacks++
		}
	}
	fastLive := bits.OnesCount64(alive&c.fastMask) >= c.fastSize
	need := c.promise == 0 && (!fastLive || c.stalled || (len(c.pending) > 0 || c.high > c.executed) && now-c.lastProgress > failureTimeout)
	if c.promise > 0 && alive&(1<<c.owner()) == 0 {
		need = true
	}
	if need && candidate == c.id && bits.OnesCount64(alive) >= c.n/2+1 {
		c.begin()
	}
	if need && candidate >= 0 && candidate != c.id {
		c.send(candidate, message{Kind: msgRecover, Epoch: c.promise})
	}
	if c.preparing && now-c.lastRetry >= time.Second {
		c.broadcast(message{Kind: msgPrepare, Epoch: c.promise, Start: c.start})
		c.lastRetry = now
	}
	if c.active && c.owner() == c.id {
		c.flush()
		if now-c.lastRepair >= 100*time.Millisecond {
			c.resend()
			c.lastRepair = now
		}
	}
	if now-c.lastRetry >= time.Second {
		c.lastRetry = now
		if s := c.slots[c.executed+1]; s != nil && s.Chosen != nil && !s.Chosen.Value.Noop {
			if _, ok := c.known[s.Chosen.Value.ID]; !ok {
				c.broadcast(message{Kind: msgFetch, Entries: []entry{*s.Chosen}})
			}
		}
		if c.promise > 0 && c.owner() != c.id && len(c.pending) > 0 {
			var batch []entry
			// Fair, bounded retries of old requests. Re-forwarding the entire
			// pending set every second can flood an already recovering leader.
			for sent := 0; sent < repairBudget; sent++ {
				e := c.forwardOrder.Front()
				if e == nil {
					break
				}
				p := e.Value.(pendingCommand)
				if p.RetryAt > now {
					break
				}
				p.RetryAt = now + failureTimeout
				e.Value = p
				c.forwardOrder.MoveToBack(e)
				batch = append(batch, entry{Value: c.known[p.ID], Payload: true})
				if len(batch) == pageSize {
					c.send(c.owner(), message{Kind: msgForward, Entries: batch})
					batch = nil
				}
			}
			if len(batch) > 0 {
				c.send(c.owner(), message{Kind: msgForward, Entries: batch})
			}
		}
	}
}
