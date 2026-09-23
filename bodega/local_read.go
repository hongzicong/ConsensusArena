package bodega

import (
	"container/heap"
	"slices"
	"time"

	"github.com/hongzicong/ConsensusArena/state"
)

func (e *engine) markCommitted(s uint64) {
	e.committed[s] = true
	for _, r := range e.log[s].requests() {
		cmd := r.Proposal.Command
		if cmd.Op == state.PUT && s > e.latestCommitted[cmd.K] {
			e.latestCommitted[cmd.K] = s
		}
	}
	e.queueFastSlot(s)
}

func (e *engine) localReadAuthority(now time.Time) bool {
	return (e.current.Leader != e.id || e.prepared) && e.stable(now)
}

func (e *engine) finishLocalRead(r request, value state.Value) {
	e.stats.LocalReads++
	if e.current.Leader == e.id {
		e.stats.StableLeaderReads++
		if s := e.latest[r.Proposal.Command.K]; s > 0 && !e.committed[s] {
			e.stats.LeaderPendingWriteBypasses++
		}
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

func (e *engine) readValue(s uint64, r request) (state.Value, bool) {
	if s <= e.prefix {
		return e.execute(r.Proposal.Command), true
	}
	v, ok := e.log[s]
	if !ok || !v.ReadFresh || v.Ballot != e.current.Ballot || !e.committed[s] || s > e.readPrefix {
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
