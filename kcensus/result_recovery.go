package kcensus

import (
	"time"

	"github.com/hongzicong/ConsensusArena/replica/defs"
	"github.com/hongzicong/ConsensusArena/state"
)

// This is execution evidence, not a proposal. An unknown request is never
// completed just because it was queried. A surviving voter must execute it or
// install an executed-prefix checkpoint containing its ID in this shard.
func (c *core) receiveResultQuery(m message) {
	if m.From < c.n || m.Request == nil || m.Slot != 0 ||
		m.Request.ID.Client != int32(m.From) || m.ID != m.Request.ID ||
		m.Request.Command.Op != state.PUT || c.shardFor(m.Request.Command.K) != m.Shard {
		return
	}
	if c.resultWaiters == nil {
		c.resultWaiters = make(map[defs.RequestID]state.Key)
	}
	if _, exists := c.resultWaiters[m.ID]; !exists {
		c.resultWaiters[m.ID] = m.Shard
		if c.resultByShard == nil {
			c.resultByShard = make(map[state.Key]map[defs.RequestID]bool)
		}
		if c.resultByShard[m.Shard] == nil {
			c.resultByShard[m.Shard] = make(map[defs.RequestID]bool)
		}
		c.resultByShard[m.Shard][m.ID] = true
		c.resultQueue = append(c.resultQueue, m.ID)
	}
	c.stats.ResultQueries++
	if !c.finishResultWaiter(m.ID, m.Shard, c.executed(m.Shard)) {
		c.probeResultPrefix(m.Shard)
	}
}

func (c *core) finishResultWaiter(id defs.RequestID, k state.Key, slot uint64) bool {
	shard, exists := c.resultWaiters[id]
	if !exists || shard != k || slot == 0 || !c.checkpointHas(k, id) {
		return false
	}
	// Arena PUT returns NIL independently of the overwritten value. Reconstruct
	// that exact result from execution evidence, without keeping payload/result
	// history or re-executing the PUT. GET never uses this recovery shortcut.
	c.send(int(id.Client), message{Kind: executedResult, Shard: k, Slot: slot, ID: id, Result: state.NIL()})
	c.removeResultWaiter(id, k)
	c.stats.RecoveryResultReplies++
	return true
}

func (c *core) removeResultWaiter(id defs.RequestID, k state.Key) {
	delete(c.resultWaiters, id)
	delete(c.resultByShard[k], id)
	if len(c.resultByShard[k]) == 0 {
		delete(c.resultByShard, k)
		delete(c.checkpointProbes, k)
	}
}

func (c *core) probeResultPrefix(k state.Key) {
	if c.checkpointProbes == nil {
		c.checkpointProbes = make(map[state.Key]time.Duration)
	}
	if at, exists := c.checkpointProbes[k]; exists && c.now-at < 4*c.retry {
		return
	}
	c.checkpointProbes[k] = c.now
	c.requestCheckpoint(k, c.executed(k)+1, true)
}

func (c *core) retryResultWaiters() {
	count := min(len(c.resultQueue)-c.resultHead, 256)
	for ; count > 0; count-- {
		id := c.resultQueue[c.resultHead]
		c.resultHead++
		k, ok := c.resultWaiters[id]
		if !ok {
			continue
		}
		if c.peerFailed(int(id.Client)) {
			c.removeResultWaiter(id, k)
			continue
		}
		if !c.finishResultWaiter(id, k, c.executed(k)) {
			c.resultQueue = append(c.resultQueue, id)
			c.probeResultPrefix(k)
		}
	}
	if c.resultHead > 4096 || len(c.resultQueue) > 4*len(c.resultWaiters)+1024 {
		live := make([]defs.RequestID, 0, len(c.resultWaiters))
		for _, id := range c.resultQueue[c.resultHead:] {
			if _, ok := c.resultWaiters[id]; ok {
				live = append(live, id)
			}
		}
		c.resultQueue, c.resultHead = live, 0
	}
}
