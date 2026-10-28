package kcensus

// Ordered execution, result deduplication, and completion accounting.
import (
	"time"

	"github.com/hongzicong/ConsensusArena/replica/defs"
	"github.com/hongzicong/ConsensusArena/state"
)

func (c *core) apply(k state.Key) {
	s := c.shard(k)
	for {
		i := s.executed + 1
		x := s.slots[i]
		if x == nil || x.decided == nil {
			break
		}
		for _, r := range x.decided.Records {
			if !c.requestCompleted(r.ID) {
				var v state.Value
				if c.execute != nil {
					v = c.execute(r.Command)
				}
				v = append(state.Value(nil), v...)
				c.stats.Executed++
				c.checkpointWrite(k, r)
				c.replyExecutedWrite(r, k, i, v)
				c.finishResultWaiter(r.ID, k, i)
				c.complete(r.ID, v)
				c.recordWriteCompletion(r.ID, s.executed)
				c.markCompleted(r)
			} else {
				c.stats.Deduplicated++
			}
			c.forgetRequest(k, r)
		}
		s.executed++
		s.lastUID, s.hasLast = x.decided.UID, true
		delete(s.slots, i)
		delete(c.active, slotID{k, i})
		c.retiredSlots = append(c.retiredSlots, x)
		c.purgeCommitted(k, s)
	}
	if s.executed >= s.high {
		delete(c.gaps, k)
	}
	if a := c.snapshotInputs[k]; a != nil && a.cut <= s.executed {
		delete(c.snapshotInputs, k)
	}
	c.checkpointAdvanced(k)
	for id, q := range c.readsByShard[k] {
		c.finishRead(id, q)
	}
	c.releaseFuture(k)
	c.propose(k)
}

func (c *core) finishRead(id defs.RequestID, q *pendingRead) {
	c.readyRead(q)
	if !q.quorum {
		return
	}
	var v state.Value
	if c.execute != nil {
		v = c.execute(q.record.Command)
	}
	v = append(state.Value(nil), v...)
	delete(c.reads, id)
	delete(c.readsByShard[c.shardFor(q.record.Command.K)], id)
	if len(c.readsByShard[c.shardFor(q.record.Command.K)]) == 0 {
		delete(c.readsByShard, c.shardFor(q.record.Command.K))
	}
	c.stats.ReadWaitNanos += uint64(c.now - q.started)
	if q.delegated {
		c.send(int(id.Client), message{Kind: executedResult, Shard: c.shardFor(q.record.Command.K), ID: id, Result: v})
	} else {
		c.complete(id, v)
	}
	c.markCompleted(q.record)
	c.forgetRequest(c.shardFor(q.record.Command.K), q.record)
}

// Only the fixed voting delegate returns an external PUT result. This also
// applies to learned fallback decisions and to each original member of a batch.
func (c *core) replyExecutedWrite(r Record, k state.Key, slot uint64, v state.Value) {
	owner := int(r.ID.Client)
	if r.Command.Op == state.PUT && owner >= c.n && owner < c.m && c.plan.Leaders[owner] == c.id {
		c.send(owner, message{Kind: executedResult, Shard: k, Slot: slot, ID: r.ID, Result: v})
	}
}

// The standard replica interface keeps local proposal callbacks. External
// adapters publish completion only after a validated voting-executor result.
func (c *core) complete(id defs.RequestID, v state.Value) {
	if c.reply != nil {
		c.reply(id, v)
	}
}

func (c *core) executed(k state.Key) uint64 {
	if s := c.shards[k]; s != nil {
		return s.executed
	}
	return c.pool.sleepingState(k).executed
}

func (c *core) recordWriteCompletion(id defs.RequestID, slot uint64) {
	a, ok := c.localWrites[id]
	if !ok {
		return
	}
	delete(c.localWrites, id)
	d := uint64(c.now - a.at)
	c.stats.LocalWrites++
	c.stats.LocalWriteNanos += d
	if d > c.stats.LocalWriteMaxNanos {
		c.stats.LocalWriteMaxNanos = d
	}
	if slot > a.slot {
		c.stats.LocalSlotWaits += slot - a.slot
	}
	limits := [...]time.Duration{100 * time.Millisecond, 250 * time.Millisecond, 500 * time.Millisecond, time.Second, 2 * time.Second, 5 * time.Second, 10 * time.Second, 30 * time.Second}
	i := 0
	for i < len(limits) && d > uint64(limits[i]) {
		i++
	}
	c.stats.LocalWriteLatencyBuckets[i]++
}
