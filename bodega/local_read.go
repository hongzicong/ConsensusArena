package bodega

import (
	"container/heap"
	"slices"
	"time"

	"github.com/hongzicong/ConsensusArena/state"
)

func (e *engine) localReadAuthority(now time.Time) bool {
	return (e.current.Leader != e.id || e.prepared) && e.stable(now)
}

func (e *engine) finishLocalRead(r request, value state.Value) {
	e.stats.LocalReads++
	if e.current.Leader == e.id {
		e.stats.StableLeaderReads++
	}
	e.finish(r, value)
}

// A majority-accepted prefix fixes earlier values, preventing an old minority
// duplicate from later changing the first occurrence of a fresh request ID.
// This is separate from all-responder commitment and from state-machine execution.
func (e *engine) updateReadPrefix() {
	var prefixes [63]uint64
	copy(prefixes[:], e.acceptedProgress)
	slices.Sort(prefixes[:e.n])
	if p := prefixes[e.n-e.majority]; p > e.readPrefix {
		e.readPrefix = p
	}
}

// Read from a chosen slot only. Majority acceptance alone is not enough to
// expose a write while another leased responder can still serve an older value.
func (e *engine) readValue(s uint64, r request) (state.Value, bool) {
	if s <= e.prefix {
		return e.execute(r.Proposal.Command), true
	}
	v, ok := e.log[s]
	if !ok || !e.committed[s] || !v.ReadFresh || v.Ballot != e.current.Ballot || s > e.readPrefix {
		return nil, false
	}
	e.stats.OutOfOrderReads++
	return v.lastWrite(r.Proposal.Command.K)
}

func (e *engine) queueFastSlot(s uint64) {
	if len(e.holdWaiters[s]) > 0 && !e.fastQueued[s] {
		e.fastQueued[s] = true
		heap.Push(&e.fastSlots, s)
	}
}

// False means lease authority expired mid-release; retain the queue for renewal.
// A not-yet-committed slot keeps its waiters and is requeued on commit arrival.
func (e *engine) releaseSlot(s uint64, now time.Time) bool {
	for _, id := range e.holdWaiters[s] {
		h, ok := e.held[id]
		if !ok || h.Slot != s {
			continue
		}
		if !e.localReadAuthority(now) || e.current.respondersFor(h.Request.Proposal.Command.K)&bit(e.id) == 0 {
			return false
		}
		value, ready := e.readValue(s, h.Request)
		if !ready {
			return true
		}
		e.finishLocalRead(h.Request, value)
	}
	delete(e.holdWaiters, s)
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

func (e *engine) release(now time.Time) {
	if !e.stable(now) {
		return
	}
	for len(e.holdSlots) > 0 && e.holdSlots[0] <= e.prefix {
		s := e.holdSlots[0]
		if !e.releaseSlot(s, now) {
			return
		}
		heap.Pop(&e.holdSlots)
	}
	for len(e.fastSlots) > 0 && e.fastSlots[0] <= e.readPrefix {
		s := e.fastSlots[0]
		if !e.releaseSlot(s, now) {
			return
		}
		heap.Pop(&e.fastSlots)
		delete(e.fastQueued, s)
	}
}
