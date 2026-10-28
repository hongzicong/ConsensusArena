package kcensus

import (
	"sort"

	"github.com/hongzicong/ConsensusArena/replica/defs"
	"github.com/hongzicong/ConsensusArena/state"
)

// Completion metadata contains no commands or results. Sparse ranges are
// necessary because requests on independent keys can finish in any order.
type sequenceRange struct{ first, last int32 }
type completedSequences struct {
	floor       int32
	prefixCount uint64
	ranges      []sequenceRange
}

func (s *completedSequences) contains(seq int32) bool {
	if seq <= s.floor {
		return true
	}
	i := sort.Search(len(s.ranges), func(i int) bool { return s.ranges[i].last >= seq })
	return i < len(s.ranges) && s.ranges[i].first <= seq
}

func (s *completedSequences) add(seq int32) {
	s.addRange(seq, seq)
}

// Bulk union is needed when installing a compacted shard prefix; expanding a
// long contiguous range into one transition per request would stall the loop.
func (s *completedSequences) addRange(first, last int32) {
	if last <= s.floor {
		return
	}
	if first <= s.floor {
		first = s.floor + 1
	}
	i := sort.Search(len(s.ranges), func(i int) bool { return int64(s.ranges[i].last)+1 >= int64(first) })
	if i == len(s.ranges) || int64(s.ranges[i].first) > int64(last)+1 {
		s.ranges = append(s.ranges, sequenceRange{})
		copy(s.ranges[i+1:], s.ranges[i:])
		s.ranges[i] = sequenceRange{first, last}
	} else {
		if first < s.ranges[i].first {
			s.ranges[i].first = first
		}
		if last > s.ranges[i].last {
			s.ranges[i].last = last
		}
	}
	for i+1 < len(s.ranges) && int64(s.ranges[i+1].first) <= int64(s.ranges[i].last)+1 {
		if s.ranges[i+1].last > s.ranges[i].last {
			s.ranges[i].last = s.ranges[i+1].last
		}
		copy(s.ranges[i+1:], s.ranges[i+2:])
		s.ranges = s.ranges[:len(s.ranges)-1]
	}
	s.advance(s.floor)
}

func (s *completedSequences) advance(floor int32) {
	if floor < s.floor {
		floor = s.floor
	}
	count := s.countThrough(floor)
	for len(s.ranges) > 0 && int64(s.ranges[0].first) <= int64(floor)+1 {
		if s.ranges[0].last > floor {
			start := int64(s.ranges[0].first)
			if start <= int64(floor) {
				start = int64(floor) + 1
			}
			count += uint64(int64(s.ranges[0].last) - start + 1)
			floor = s.ranges[0].last
		}
		s.ranges = s.ranges[1:]
	}
	s.floor, s.prefixCount = floor, count
	if len(s.ranges) == 0 {
		s.ranges = nil
	}
}

func (s *completedSequences) countThrough(seq int32) uint64 {
	count := s.prefixCount
	for _, r := range s.ranges {
		if r.first > seq {
			break
		}
		last := r.last
		if last > seq {
			last = seq
		}
		count += uint64(int64(last) - int64(r.first) + 1)
	}
	return count
}

func (c *core) markCompleted(r Record) {
	c.completions(r.ID.Client).add(r.ID.Sequence)
}

func (c *core) completions(client int32) *completedSequences {
	s := c.completed[client]
	if s == nil {
		s = &completedSequences{floor: -1}
		c.completed[client] = s
	}
	return s
}
func (c *core) requestCompleted(id defs.RequestID) bool {
	s := c.completed[id.Client]
	return s != nil && s.contains(id.Sequence)
}
func (c *core) valueCompleted(v *Value) bool {
	if v == nil || v.Reference || len(v.Records) == 0 {
		return false
	}
	for _, r := range v.Records {
		if !c.requestCompleted(r.ID) {
			return false
		}
	}
	return true
}

type sleepingShard struct {
	executed, nextUID, lastUID uint64
	hasLast                    bool
}
type shardTask struct {
	key        state.Key
	generation uint64
}

func (c *core) shard(k state.Key) *shard {
	c.touched[k] = true
	if s := c.shards[k]; s != nil {
		return s
	}
	s, grew := c.pool.acquire()
	if grew {
		c.stats.ShardsAllocated++
	}
	if s.generation != 0 {
		c.stats.ShardsReused++
	}
	progress := c.pool.wake(k)
	s.executed, s.high, s.nextUID = progress.executed, progress.executed, progress.nextUID
	s.lastUID, s.hasLast = progress.lastUID, progress.hasLast
	c.shardGeneration++
	s.generation = c.shardGeneration
	c.shards[k] = s
	return s
}

func (c *core) beginTurn() { c.turnDepth++ }
func (c *core) endTurn() {
	c.turnDepth--
	if c.turnDepth != 0 {
		return
	}
	// These objects may still be on a graph caller's stack until this point.
	for _, x := range c.retiredSlots {
		*x = slotState{}
	}
	clear(c.retiredSlots)
	c.retiredSlots = c.retiredSlots[:0]
	for k := range c.touched {
		c.trySleep(k)
		delete(c.touched, k)
	}
	c.compactTimers()
}

// Timer arrays are rotations, not admission queues. Finished work must not
// accumulate if completion runs faster than a bounded timer sweep can visit it.
func (c *core) compactTimers() {
	needsCompaction := func(length, head, capacity, live int) bool {
		limit := 4*live + 1024
		return length-head > limit || capacity > 2*limit
	}
	if needsCompaction(len(c.retryQueue), c.retryHead, cap(c.retryQueue), len(c.active)) {
		live := make([]retryItem, 0, len(c.active))
		for _, item := range c.retryQueue[c.retryHead:] {
			if c.active[item.id] == item.instance && item.instance != nil && c.shards[item.id.shard] == item.owner && item.owner.generation == item.generation {
				live = append(live, item)
			}
		}
		c.retryQueue, c.retryHead = live, 0
	}
	if needsCompaction(len(c.readQueue), c.readHead, cap(c.readQueue), len(c.reads)) {
		live := make([]defs.RequestID, 0, len(c.reads))
		for _, id := range c.readQueue[c.readHead:] {
			if c.reads[id] != nil {
				live = append(live, id)
			}
		}
		c.readQueue, c.readHead = live, 0
	}
	if needsCompaction(len(c.pendingShards), c.pendingHead, cap(c.pendingShards), len(c.shards)) {
		live := make([]shardTask, 0, len(c.shards))
		for _, task := range c.pendingShards[c.pendingHead:] {
			s := c.shards[task.key]
			if s == nil || s.generation != task.generation {
				continue
			}
			if len(s.queued) == 0 {
				s.scheduled = false
				continue
			}
			live = append(live, task)
		}
		c.pendingShards, c.pendingHead = live, 0
	}
	if needsCompaction(len(c.gapQueue), c.gapHead, cap(c.gapQueue), len(c.gaps)) {
		live := make([]shardTask, 0, len(c.gaps))
		for _, task := range c.gapQueue[c.gapHead:] {
			s := c.shards[task.key]
			if s == nil || s.generation != task.generation {
				continue
			}
			if !c.gaps[task.key] {
				s.gapScheduled = false
				continue
			}
			live = append(live, task)
		}
		c.gapQueue, c.gapHead = live, 0
	}
}

func (c *core) trySleep(k state.Key) {
	s := c.shards[k]
	if s == nil || len(s.slots) != 0 || len(s.queued) != 0 || len(s.objects) != 0 || len(c.future[k]) != 0 || len(c.readsByShard[k]) != 0 || len(c.payloadByShard[k]) != 0 {
		return
	}
	c.pool.sleep(k, sleepingShard{s.executed, s.nextUID, s.lastUID, s.hasLast})
	c.checkpointAdvanced(k)
	delete(c.shards, k)
	delete(c.gaps, k)
	// Keep small bucket allocations for reuse, but shrink formerly hot keys.
	if s.peakObjects > 3 {
		s.objects = make(map[uint64]*Value)
		s.queued = make(map[defs.RequestID]bool)
		s.shared = make(map[defs.RequestID]bool)
	}
	if s.peakSlots > 3 {
		s.slots = make(map[uint64]*slotState)
	}
	s.peakObjects, s.peakSlots = 0, 0
	if cap(s.pending) > 3 {
		s.pending = nil
	} else {
		s.pending = s.pending[:0]
	}
	s.pendingValue = nil
	s.scheduled, s.gapScheduled = false, false
	s.nextOffer = 0
	c.pool.release(s)
	c.stats.ShardsSlept++
}

func (c *core) forgetRequest(k state.Key, r Record) {
	s := c.shards[k]
	if s != nil {
		delete(s.queued, r.ID)
		delete(s.shared, r.ID)
		if uid, ok := c.commandUIDs[r.ID]; ok {
			delete(s.objects, uid)
			delete(c.payloadSeen, payloadObject{k, uid})
			delete(c.repairHandoffs, payloadObject{k, uid})
		}
		c.touched[k] = true
	}
	delete(c.known, r.ID)
	delete(c.commandUIDs, r.ID)
	delete(c.localWrites, r.ID)
}

func (c *core) purgeCommitted(k state.Key, s *shard) {
	for uid, v := range s.objects {
		if v.Batch != nil && v.Batch.Slot <= s.executed {
			delete(s.objects, uid)
			delete(c.payloadSeen, payloadObject{k, uid})
		}
	}
	if s.pendingValue != nil && s.pendingValue.Batch != nil && s.pendingValue.Batch.Slot <= s.executed {
		s.pendingValue = nil
	}
	retained := s.pending[:0]
	for _, id := range s.pending {
		if s.queued[id] {
			retained = append(retained, id)
		}
	}
	clear(s.pending[len(retained):])
	s.pending = retained
	for _, w := range c.payloadByShard[k] {
		if w.message.Slot > 0 && w.message.Slot <= s.executed {
			c.removePayloadWait(w)
		}
	}
}

func (c *core) receiveLatePayload(m message) {
	if c.valueCompleted(m.Value) {
		return
	}
	// Only a Single payload from an old slot may represent losing input.
	// A batch descriptor is specific to the retired slot and is discarded.
	if m.Value.Batch != nil {
		return
	}
	if err := c.storeValue(m.Shard, m.Value); err != nil {
		panic(err)
	}
	c.enqueueValue(m.Value)
	c.relayPayload(m)
	c.propose(m.Shard)
}

func (c *core) lifecycleSnapshot() map[string]int {
	objects, slots, pending, ranges := 0, 0, 0, 0
	for _, s := range c.shards {
		objects += len(s.objects)
		slots += len(s.slots)
		pending += len(s.queued)
	}
	for _, s := range c.completed {
		ranges += len(s.ranges)
	}

	denseActive := 0
	for k := range c.shards {
		if k >= 0 && k < state.Key(len(c.pool.sleeping)) {
			denseActive++
		}
	}
	sleeping := len(c.pool.sleeping) - denseActive + len(c.pool.outside)
	return map[string]int{"active_shards": len(c.shards), "sleeping_shards": sleeping, "free_shards": len(c.pool.free), "logical_shards": sleeping + len(c.shards), "pool_size": len(c.pool.physical), "initial_pool_size": c.pool.initialSize, "slots": slots, "objects": objects, "known_commands": len(c.known), "uid_bindings": len(c.commandUIDs), "pending_commands": pending, "completion_ranges": ranges, "payload_waits": len(c.payloadWaiting), "relay_objects": len(c.payloadSeen)}
}
