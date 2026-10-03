package fastpaxos

// Protocol recovery transitions and recovery-specific helpers.
import (
	"math/bits"
	"sort"
)

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
