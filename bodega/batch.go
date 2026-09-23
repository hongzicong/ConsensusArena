package bodega

import (
	"time"

	"github.com/hongzicong/ConsensusArena/state"
)

const batchInterval = time.Millisecond
const maxBatchCommands = 5000
const maxBatchBytes = 8 << 20

func (v entry) requests() []request {
	if len(v.Batch) != 0 {
		return v.Batch
	}
	return []request{v.Request}
}

func (v entry) lastWrite(key state.Key) (state.Value, bool) {
	rs := v.requests()
	for i := len(rs) - 1; i >= 0; i-- {
		cmd := rs[i].Proposal.Command
		if cmd.Op == state.PUT && cmd.K == key {
			return cmd.V, true
		}
	}
	return nil, false
}

func requestBytes(r request) int { return 30 + len(r.Proposal.Command.V) }
func entryBytes(v entry) int {
	n := 21
	for _, r := range v.requests() {
		n += requestBytes(r)
	}
	return n
}

func (e *engine) admit(r request, now time.Time) {
	if !e.batching {
		e.proposeBatch([]request{r}, now)
		return
	}
	if e.batchIDs[r.id()] {
		return
	}
	if len(e.batch) > 0 && e.batchBytes+requestBytes(r) > maxBatchBytes {
		e.flushBatch(now)
	}
	e.batch = append(e.batch, r)
	e.batchIDs[r.id()] = true
	e.batchBytes += requestBytes(r)
	if len(e.batch) >= maxBatchCommands || e.batchBytes >= maxBatchBytes {
		e.flushBatch(now)
	}
}

func (e *engine) flushBatch(now time.Time) {
	if len(e.batch) == 0 {
		return
	}
	rs := e.batch
	e.batch, e.batchBytes = nil, 0
	e.batchIDs = map[requestID]bool{}
	if !e.active() || e.current.Leader != e.id || !e.prepared {
		for _, r := range rs {
			e.enqueue(r)
		}
		return
	}
	e.proposeBatch(rs, now)
}

func (e *engine) proposeBatch(rs []request, now time.Time) {
	e.next++
	v := entry{Slot: e.next, Ballot: e.current.Ballot, ReadFresh: true}
	if len(rs) == 1 {
		v.Request = rs[0]
	} else {
		v.Batch = rs
	}
	for _, r := range rs {
		e.inflight[r.id()] = v.Slot
		if r.Proposal.Command.Op == state.PUT {
			e.stats.Writes++
		} else {
			e.stats.FallbackReads++
		}
	}
	e.stats.Batches++
	e.stats.BatchCommands += uint64(len(rs))
	if uint64(len(rs)) > e.stats.MaxBatchCommands {
		e.stats.MaxBatchCommands = uint64(len(rs))
	}
	e.acceptEntry(v)
	e.votes[v.Slot] = bit(e.id)
	e.broadcast(message{Kind: accept, Entry: v})
	e.tryCommit(v.Slot, now)
}

// A cancellation only retires read delivery state, never an accepted log value.
func (e *engine) cancelRead(id requestID) {
	e.dropHeld(id)
	if r, ok := e.queued[id]; ok && r.Proposal.Command.Op == state.GET {
		delete(e.queued, id)
	}
	if e.batchIDs[id] {
		for i, r := range e.batch {
			if r.id() == id && r.Proposal.Command.Op == state.GET {
				e.batch = append(e.batch[:i], e.batch[i+1:]...)
				e.batchBytes -= requestBytes(r)
				delete(e.batchIDs, id)
				break
			}
		}
	}
}

func (e *engine) dropHeld(id requestID) {
	if h, ok := e.held[id]; ok {
		delete(e.held, id)
		e.holdCounts[h.Slot]--
		if e.holdCounts[h.Slot] == 0 {
			delete(e.holdCounts, h.Slot)
			delete(e.holdWaiters, h.Slot)
		}
	}
}
