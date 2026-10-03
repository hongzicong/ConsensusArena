package kcensus

// Ordered execution, result deduplication, and completion accounting.
import (
	"time"

	"github.com/hongzicong/ConsensusArena/state"
)

func (c *core) apply(k state.Key) {
	s := c.shard(k)
	for {
		x := s.slots[s.executed+1]
		if x == nil || x.decided == nil {
			break
		}
		for _, r := range x.decided.Records {
			if _, ok := c.results[r.ID]; ok {
				c.stats.Deduplicated++
				continue
			}
			var v state.Value
			if c.id < c.n && c.execute != nil {
				v = c.execute(r)
			}
			v = append(state.Value(nil), v...)
			c.ordered[r.ID] = true
			delete(c.repairHandoffs, payloadObject{k, c.commandUIDs[r.ID]})
			if c.id < c.n {
				c.results[r.ID] = v
				c.stats.Executed++
				c.recordWriteCompletion(r.ID, s.executed)
			}
			delete(s.queued, r.ID)
			delete(s.shared, r.ID)
			if c.id < c.n && c.complete != nil {
				c.complete(r.ID, v)
			}
		}
		s.executed++
	}
	if s.executed >= s.high {
		delete(c.gaps, k)
	}
	for id, q := range c.readsByKey[k] {
		c.finishRead(id, q)
	}
	c.releaseFuture(k)
	c.propose(k)
}

func (c *core) finishRead(id CommandID, q *pendingRead) {
	c.readyRead(q)
	if !q.quorum {
		return
	}
	var v state.Value
	if c.execute != nil {
		v = c.execute(q.record)
	}
	v = append(state.Value(nil), v...)
	c.results[id] = v
	delete(c.reads, id)
	delete(c.readsByKey[q.record.Command.K], id)
	if len(c.readsByKey[q.record.Command.K]) == 0 {
		delete(c.readsByKey, q.record.Command.K)
	}
	c.stats.ReadWaitNanos += uint64(c.now - q.started)
	if c.complete != nil {
		c.complete(id, v)
	}
}

func (c *core) executed(k state.Key) uint64 {
	if s := c.shards[k]; s != nil {
		return s.executed
	}
	return 0
}

func (c *core) recordWriteCompletion(id CommandID, slot uint64) {
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
