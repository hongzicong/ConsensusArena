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
				v = c.execute(r.Command)
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
			if c.id < c.n {
				c.complete(r.ID, v)
			}
		}
		s.executed++
		// Keep the chosen value for prefix repair, but release obsolete census
		// and graph evidence. Losing inputs are retained by the pending queue.
		x.fast, x.local, x.classic, x.selected = nil, nil, nil, nil
		x.values, x.graphs, x.nodeReports = nil, nil, nil
		x.classicValues, x.payloadRelayed, x.reports = nil, nil, nil
		x.knowledge = nil
	}
	if s.executed >= s.high {
		delete(c.gaps, k)
	}
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
	c.results[id] = v
	delete(c.reads, id)
	delete(c.readsByShard[c.shardFor(q.record.Command.K)], id)
	if len(c.readsByShard[c.shardFor(q.record.Command.K)]) == 0 {
		delete(c.readsByShard, c.shardFor(q.record.Command.K))
	}
	c.stats.ReadWaitNanos += uint64(c.now - q.started)
	c.complete(id, v)
}

// Local proposals take precedence over delegated executed-result replies.
func (c *core) complete(id defs.RequestID, v state.Value) {
	if c.reply == nil || c.reply(id, v) {
		return
	}
	if int(id.Client) >= c.n && int(id.Client) < c.m {
		record, ok := c.known[id]
		delegate := c.plan.Leaders[int(id.Client)]
		recoveryReply := c.peerFailed(delegate) && c.coordinator() == c.id
		if ok && (record.Command.Op == state.GET || delegate == c.id || recoveryReply) {
			if recoveryReply && record.Command.Op == state.PUT {
				c.stats.RecoveryResultReplies++
			}
			c.send(int(id.Client), message{Kind: executedResult, Shard: c.shardFor(record.Command.K), ID: id, Result: v})
		}
	}
}

func (c *core) acceptsResult(m message, r Record) bool {
	return m.From >= 0 && m.From < c.n && c.shardFor(r.Command.K) == m.Shard
}

// Called after client completion is claimed, before notifying the workload.
func (c *core) recordResult(m message, r Record) {
	if r.Command.Op != state.PUT {
		return
	}
	c.recordWriteCompletion(m.ID, c.executed(m.Shard))
	// A voter has executed this exact input. Stop offering it while local
	// commit/payload catch-up continues; this does not advance a slot prefix.
	c.ordered[m.ID] = true
	delete(c.repairHandoffs, payloadObject{m.Shard, c.commandUIDs[m.ID]})
	c.results[m.ID] = append(state.Value(nil), m.Result...)
	s := c.shard(m.Shard)
	delete(s.queued, m.ID)
	delete(s.shared, m.ID)
}

func (c *core) executed(k state.Key) uint64 {
	if s := c.shards[k]; s != nil {
		return s.executed
	}
	return 0
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
