package bodega

import (
	"container/heap"
	"slices"
	"time"

	"github.com/hongzicong/ConsensusArena/state"
)

func (c *core) localReadAuthority(now time.Time) bool {
	return (c.current.Leader != c.id || c.prepared) && c.stable(now)
}

func (c *core) finishLocalRead(r request, value state.Value) {
	c.stats.LocalReads++
	if r.Origin == c.id && c.observeLocalRead != nil {
		c.observeLocalRead(r.id())
	}
	if c.current.Leader == c.id {
		c.stats.StableLeaderReads++
	}
	c.finish(r, value)
}

// A majority-accepted prefix fixes earlier values, preventing an old minority
// duplicate from later changing the first occurrence of a fresh request ID.
// This is separate from all-responder commitment and from state-machine execution.
func (c *core) updateReadPrefix() {
	var prefixes [63]uint64
	copy(prefixes[:], c.acceptedProgress)
	slices.Sort(prefixes[:c.n])
	if p := prefixes[c.n-c.majority]; p > c.readPrefix {
		c.readPrefix = p
	}
}

// Read from a chosen slot only. Majority acceptance alone is not enough to
// expose a write while another leased responder can still serve an older value.
func (c *core) readValue(s uint64, r request) (state.Value, bool) {
	if s <= c.prefix {
		return c.execute(r.Proposal.Command), true
	}
	v, ok := c.log[s]
	if !ok || !c.committed[s] || !v.ReadFresh || v.Ballot != c.current.Ballot || s > c.readPrefix {
		return nil, false
	}
	c.stats.OutOfOrderReads++
	return v.lastWrite(r.Proposal.Command.K)
}

func (c *core) queueFastSlot(s uint64) {
	if len(c.holdWaiters[s]) > 0 && !c.fastQueued[s] {
		c.fastQueued[s] = true
		heap.Push(&c.fastSlots, s)
	}
}

// False means lease authority expired mid-release; retain the queue for renewal.
// A not-yet-committed slot keeps its waiters and is requeued on commit arrival.
func (c *core) releaseSlot(s uint64, now time.Time) bool {
	for _, id := range c.holdWaiters[s] {
		h, ok := c.held[id]
		if !ok || h.slot != s {
			continue
		}
		if !c.localReadAuthority(now) || c.current.respondersFor(h.request.Proposal.Command.K)&bit(c.id) == 0 {
			return false
		}
		value, ready := c.readValue(s, h.request)
		if !ready {
			return true
		}
		c.finishLocalRead(h.request, value)
	}
	delete(c.holdWaiters, s)
	return true
}

// readSlots orders hold targets, so a protocol message need not scan every
// blocked reader while the executed prefix has not reached its slot.
type readSlots []uint64

func (q readSlots) Len() int { return len(q) }

func (q readSlots) Less(i, j int) bool { return q[i] < q[j] }

func (q readSlots) Swap(i, j int) { q[i], q[j] = q[j], q[i] }

func (q *readSlots) Push(x any) { *q = append(*q, x.(uint64)) }

func (q *readSlots) Pop() any { old := *q; x := old[len(old)-1]; *q = old[:len(old)-1]; return x }

func (c *core) release(now time.Time) {
	if !c.stable(now) {
		return
	}
	for len(c.holdSlots) > 0 && c.holdSlots[0] <= c.prefix {
		s := c.holdSlots[0]
		if !c.releaseSlot(s, now) {
			return
		}
		heap.Pop(&c.holdSlots)
	}
	for len(c.fastSlots) > 0 && c.fastSlots[0] <= c.readPrefix {
		s := c.fastSlots[0]
		if !c.releaseSlot(s, now) {
			return
		}
		heap.Pop(&c.fastSlots)
		delete(c.fastQueued, s)
	}
}
