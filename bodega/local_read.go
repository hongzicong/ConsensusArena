package bodega

import (
	"container/heap"
	"math/bits"
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
			e.stats.LeaderEarlyReads++
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
	if !ok || !v.ReadFresh || v.Ballot != e.current.Ballot || s > e.readPrefix {
		return nil, false
	}
	if !e.committed[s] {
		mask := e.noteVotes[s]
		responders := e.current.respondersFor(r.Proposal.Command.K)
		// Every location that could answer a later read must already know this
		// write. A bare majority permits read-new/read-old across responders.
		if bits.OnesCount64(mask) < e.majority || mask&responders != responders {
			return nil, false
		}
		e.stats.EarlyReads++
	}
	e.stats.OutOfOrderReads++
	return v.lastWrite(r.Proposal.Command.K)
}

// AcceptNote is read evidence only. It never commits or executes a write.
func (e *engine) notifyAccepted(v entry) {
	if v.Slot <= e.compacted || v.Ballot != e.current.Ballot {
		return
	}
	mask := uint64(0)
	for _, r := range v.requests() {
		if r.Proposal.Command.Op == state.PUT {
			mask |= e.current.respondersFor(r.Proposal.Command.K)
		}
	}
	for p := 0; p < e.n; p++ {
		if p != e.id && mask&bit(p) != 0 {
			e.emit(p, message{Kind: acceptNote, Entry: entry{Slot: v.Slot, Ballot: v.Ballot}, Prefix: e.acceptedPrefix})
		}
	}
	e.updateReadPrefix()
	e.queueFastSlot(v.Slot)
}

func (e *engine) receiveAcceptNote(m message) {
	if m.Entry.Ballot != e.current.Ballot || m.Entry.Slot <= e.compacted {
		return
	}
	e.stats.AcceptNotes++
	e.noteVotes[m.Entry.Slot] |= bit(m.From)
	e.acceptedProgress[m.From] = maxSlot(e.acceptedProgress[m.From], m.Prefix)
	e.updateReadPrefix()
	e.queueFastSlot(m.Entry.Slot)
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
