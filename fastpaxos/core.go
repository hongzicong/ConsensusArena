package fastpaxos

// The state machine is single-threaded. Network delivery, timers and client
// submissions enter through the same event loop; tests drive those events too.
import (
	"bytes"
	"container/list"
	"fmt"
	"math/bits"
	"sort"
	"time"

	"github.com/hongzicong/ConsensusArena/state"
)

const (
	msgVote uint8 = iota
	msgPrepare
	msgPromise
	msgAccept
	msgAccepted
	msgCommit
	msgHeartbeat
	msgFetch
	msgData
	msgForward
	msgNack
	msgRecover
)

const pageSize = 128
const repairBudget = 32 * pageSize
const inflightWindow = 16384
const fastWindow = 8192
const failureTimeout = 3 * time.Second

type record struct {
	ID      CommandId
	Command state.Command
	Noop    bool
}

func sameValue(a, b record) bool { return a.Noop == b.Noop && (a.Noop || a.ID == b.ID) }

type entry struct {
	Slot             int
	Epoch, Round     uint64
	Value            record
	Payload, Decided bool
}

type message struct {
	Kind                     uint8
	From                     int
	Epoch                    uint64
	Start, High, Page, Pages int
	Entries                  []entry
}

type envelope struct {
	To      int
	Message message
}
type slotState struct {
	Accepted *entry
	Proposed *entry // coordinator plan; not a vote until Accept is processed
	Chosen   *entry
	Votes    map[uint64]map[int]entry
	Acks     uint64
	Local    *CommandId
	Deferred *entry
}
type snapshot struct {
	Pages int
	High  int
	Parts map[int][]entry
}

type pendingCommand struct {
	ID      CommandId
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
	known                                              map[CommandId]record
	pending                                            map[CommandId]*list.Element
	forwardOrder                                       list.List
	results                                            map[CommandId]state.Value
	assigned                                           map[CommandId]bool
	queue                                              []CommandId
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
	complete                                           func(CommandId, state.Value)
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
		high: -1, executed: -1, slots: make(map[int]*slotState), known: make(map[CommandId]record),
		pending: make(map[CommandId]*list.Element), results: make(map[CommandId]state.Value), assigned: make(map[CommandId]bool), heard: make([]time.Duration, n)}
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
func (c *core) forgetPending(id CommandId) {
	if e := c.pending[id]; e != nil {
		c.forwardOrder.Remove(e)
		delete(c.pending, id)
	}
}
func (c *core) enqueue(id CommandId) {
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
func newer(a, b entry) bool { return a.Epoch > b.Epoch || a.Epoch == b.Epoch && a.Round > b.Round }
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
func (c *core) apply() {
	for {
		s := c.slots[c.executed+1]
		if s == nil || s.Chosen == nil {
			return
		}
		v := s.Chosen.Value
		if !v.Noop {
			full, ok := c.known[v.ID]
			if !ok {
				return
			}
			result, done := c.results[v.ID]
			if !done {
				if c.execute != nil {
					result = c.execute(full)
				}
				result = append(state.Value(nil), result...)
				c.results[v.ID] = result
				if c.complete != nil {
					c.complete(v.ID, result)
				}
			}
			c.forgetPending(v.ID)
		}
		c.executed++
		c.lastProgress = c.now
	}
}
func (c *core) adopt(epoch uint64) {
	if epoch <= c.promise {
		return
	}
	c.promise = epoch
	c.active = false
	c.preparing = false
	c.frozen = nil
	c.queue = nil
	c.assigned = make(map[CommandId]bool)
}
func (c *core) begin() {
	e := (c.promise/uint64(c.n)+1)*uint64(c.n) + uint64(c.id)
	c.adopt(e)
	c.preparing = true
	c.start = c.executed + 1
	c.snapshots = make(map[int]*snapshot)
	c.elections++
	c.lastRetry = c.now
	c.broadcast(message{Kind: msgPrepare, Epoch: e, Start: c.start})
}
func (c *core) prepare(m message) {
	if m.Epoch == 0 || int(m.Epoch%uint64(c.n)) != m.From {
		return
	}
	if m.Epoch < c.promise {
		c.send(m.From, message{Kind: msgNack, Epoch: c.promise})
		return
	}
	c.adopt(m.Epoch)
	if c.frozen != nil && c.frozenStart != m.Start {
		return
	}
	if c.frozen == nil {
		var entries []entry
		for s := m.Start; s <= c.high; s++ {
			st := c.slots[s]
			if st == nil {
				continue
			}
			var e *entry
			if st.Chosen != nil {
				e = st.Chosen
			} else {
				e = st.Accepted
			}
			if e != nil {
				v := *e
				if !v.Value.Noop {
					if full, ok := c.known[v.Value.ID]; ok {
						v.Value = full
						v.Payload = true
					}
				}
				entries = append(entries, v)
			}
		}
		pages := (len(entries) + pageSize - 1) / pageSize
		if pages == 0 {
			pages = 1
		}
		for p := 0; p < pages; p++ {
			end := (p + 1) * pageSize
			if end > len(entries) {
				end = len(entries)
			}
			c.frozen = append(c.frozen, message{Kind: msgPromise, Epoch: c.promise, Start: m.Start, High: c.high, Page: p, Pages: pages, Entries: entries[p*pageSize : end]})
		}
		c.frozenStart = m.Start
	}
	for _, page := range c.frozen {
		c.send(m.From, page)
	}
}

// selectValue implements Figure 2, including the fixed-family O4 predicate.
func selectValue(reports map[int]entry, voters uint64, fixed bool, mask uint64, fastSize, n int) (record, bool) {
	var top entry
	found := false
	for _, e := range reports {
		if e.Decided {
			return e.Value, true
		}
		if !found || newer(e, top) {
			top = e
			found = true
		}
	}
	if !found {
		return record{Noop: true}, false
	}
	values := make(map[CommandId]record)
	supports := make(map[CommandId]uint64)
	for id, e := range reports {
		if e.Epoch == top.Epoch && e.Round == top.Round {
			if e.Value.Noop {
				return e.Value, true
			}
			values[e.Value.ID] = e.Value
			supports[e.Value.ID] |= 1 << id
		}
	}
	if len(values) == 1 {
		return top.Value, true
	}
	if top.Epoch > 0 {
		panic("multiple classic values in one epoch")
	}
	for id, v := range values {
		if fixed && supports[id]&(voters&mask) == voters&mask || !fixed && bits.OnesCount64(supports[id]) >= fastSize+bits.OnesCount64(voters)-n {
			return v, true
		}
	}
	return record{Noop: true}, false
}
func (c *core) promisePage(m message) {
	if !c.preparing || m.Epoch != c.promise || m.Start != c.start || m.Pages < 1 || m.Page < 0 || m.Page >= m.Pages {
		return
	}
	s := c.snapshots[m.From]
	if s == nil {
		s = &snapshot{Pages: m.Pages, High: m.High, Parts: make(map[int][]entry)}
		c.snapshots[m.From] = s
	}
	if s.Pages != m.Pages || s.High != m.High {
		return
	}
	if _, ok := s.Parts[m.Page]; !ok {
		s.Parts[m.Page] = m.Entries
	}
	var voters uint64
	maxSlot := c.start - 1
	for id, s := range c.snapshots {
		if len(s.Parts) == s.Pages {
			voters |= 1 << id
			if s.High > maxSlot {
				maxSlot = s.High
			}
		}
	}
	if bits.OnesCount64(voters) < c.n/2+1 {
		return
	}
	bySlot := make(map[int]map[int]entry)
	for id, s := range c.snapshots {
		if voters&(1<<id) == 0 {
			continue
		}
		for _, page := range s.Parts {
			for _, e := range page {
				if e.Payload {
					c.remember(e.Value)
				}
				if bySlot[e.Slot] == nil {
					bySlot[e.Slot] = make(map[int]entry)
				}
				bySlot[e.Slot][id] = e
			}
		}
	}
	c.preparing = false
	c.active = true
	c.high = maxSlot
	c.next = maxSlot + 1
	c.retryCursor = c.start
	for slot := c.start; slot <= maxSlot; slot++ {
		v, _ := selectValue(bySlot[slot], voters, c.fixed, c.fastMask, c.fastSize, c.n)
		st := c.slot(slot)
		st.Proposed = &entry{Slot: slot, Epoch: c.promise, Value: v}
		st.Acks = 0
		if !v.Noop {
			c.assigned[v.ID] = true
		}
		c.repaired++
	}
	ids := make([]CommandId, 0, len(c.pending))
	for id := range c.pending {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool {
		if ids[i].ClientId != ids[j].ClientId {
			return ids[i].ClientId < ids[j].ClientId
		}
		return ids[i].SeqNum < ids[j].SeqNum
	})
	for _, id := range ids {
		c.enqueue(id)
	}
	c.resend()
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
func (c *core) resend() {
	if !c.active || c.owner() != c.id {
		return
	}
	floor := c.start
	if floor < c.executed+1 {
		floor = c.executed + 1
	}
	ceiling := c.high
	if ceiling >= floor+inflightWindow {
		ceiling = floor + inflightWindow - 1
	}
	if c.retryCursor < floor || c.retryCursor > ceiling {
		c.retryCursor = floor
	}
	var accepted, committed []entry
	for scanned := 0; c.retryCursor <= ceiling && scanned < repairBudget; scanned++ {
		st := c.slots[c.retryCursor]
		c.retryCursor++
		if st == nil {
			continue
		}
		if st.Chosen != nil {
			e := *st.Chosen
			if !e.Value.Noop {
				if v, ok := c.known[e.Value.ID]; ok {
					e.Value = v
					e.Payload = true
				}
			}
			committed = append(committed, e)
		} else if st.Proposed != nil && st.Proposed.Epoch == c.promise {
			e := *st.Proposed
			if !e.Value.Noop {
				v, ok := c.known[e.Value.ID]
				if !ok {
					c.broadcast(message{Kind: msgFetch, Entries: []entry{e}})
					continue
				}
				e.Value = v
			}
			e.Payload = true
			accepted = append(accepted, e)
		}
		if len(accepted) == pageSize {
			c.broadcast(message{Kind: msgAccept, Epoch: c.promise, Entries: accepted})
			accepted = nil
		}
		if len(committed) == pageSize {
			c.broadcast(message{Kind: msgCommit, Entries: committed})
			committed = nil
		}
	}
	if len(accepted) > 0 {
		c.broadcast(message{Kind: msgAccept, Epoch: c.promise, Entries: accepted})
	}
	if len(committed) > 0 {
		c.broadcast(message{Kind: msgCommit, Entries: committed})
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
			s.Acks |= 1 << m.From
			if bits.OnesCount64(s.Acks) >= c.n/2+1 {
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
