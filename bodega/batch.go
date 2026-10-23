package bodega

// Protocol-specific message or proposal batching.
import (
	"time"

	"github.com/hongzicong/ConsensusArena/replica/defs"
	"github.com/hongzicong/ConsensusArena/replicaset"
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

func (c *core) admit(r request, now time.Time) {
	if !c.batching {
		c.proposeBatch([]request{r}, now)
		return
	}
	if c.batchIDs[r.id()] {
		return
	}
	if len(c.batch) > 0 && c.batchBytes+requestBytes(r) > maxBatchBytes {
		c.flushBatch(now)
	}
	c.batch = append(c.batch, r)
	c.batchIDs[r.id()] = true
	c.batchBytes += requestBytes(r)
	if len(c.batch) >= maxBatchCommands || c.batchBytes >= maxBatchBytes {
		c.flushBatch(now)
	}
}

func (c *core) flushBatch(now time.Time) {
	if len(c.batch) == 0 {
		return
	}
	rs := c.batch
	c.batch, c.batchBytes = nil, 0
	c.batchIDs = map[defs.RequestID]bool{}
	if !c.active() || c.current.Leader != c.id || !c.prepared {
		for _, r := range rs {
			c.enqueue(r)
		}
		return
	}
	c.proposeBatch(rs, now)
}

func (c *core) proposeBatch(rs []request, now time.Time) {
	c.next++
	v := entry{Slot: c.next, Ballot: c.current.Ballot, ReadFresh: true}
	if len(rs) == 1 {
		v.Request = rs[0]
	} else {
		v.Batch = rs
	}
	for _, r := range rs {
		c.inflight[r.id()] = v.Slot
		if r.Proposal.Command.Op == state.PUT {
			c.stats.Writes++
		} else {
			c.stats.FallbackReads++
		}
	}
	c.stats.Batches++
	c.stats.BatchCommands += uint64(len(rs))
	if uint64(len(rs)) > c.stats.MaxBatchCommands {
		c.stats.MaxBatchCommands = uint64(len(rs))
	}
	c.acceptEntry(v)
	c.votes[v.Slot] = replicaset.Set(0).With(c.id)
	c.broadcast(message{Kind: accept, Entry: v})
	c.tryCommit(v.Slot, now)
}

// A cancellation only retires read delivery state, never an accepted log value.
func (c *core) cancelRead(id defs.RequestID) {
	c.dropHeld(id)
	if r, ok := c.queued[id]; ok && r.Proposal.Command.Op == state.GET {
		delete(c.queued, id)
	}
	if c.batchIDs[id] {
		for i, r := range c.batch {
			if r.id() == id && r.Proposal.Command.Op == state.GET {
				c.batch = append(c.batch[:i], c.batch[i+1:]...)
				c.batchBytes -= requestBytes(r)
				delete(c.batchIDs, id)
				break
			}
		}
	}
}

func (c *core) dropHeld(id defs.RequestID) {
	if h, ok := c.held[id]; ok {
		delete(c.held, id)
		c.holdCounts[h.slot]--
		if c.holdCounts[h.slot] == 0 {
			delete(c.holdCounts, h.slot)
			delete(c.holdWaiters, h.slot)
		}
	}
}
