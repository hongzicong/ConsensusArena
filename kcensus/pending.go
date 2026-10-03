package kcensus

// Pending input ordering, batching, offers, and bounded retries.
import (
	"sort"
	"time"

	"github.com/hongzicong/ConsensusArena/replica/defs"
	"github.com/hongzicong/ConsensusArena/state"
)

type writeArrival struct {
	at   time.Duration
	slot uint64
}

func recordBytes(r Record) int { return 21 + len(r.Command.V) }

func (c *core) enqueue(r Record, shared bool) {
	c.remember(r)
	if c.ordered[r.ID] {
		return
	}
	s := c.shard(r.Command.K)
	if !s.queued[r.ID] {
		s.queued[r.ID] = true
		s.pending = append(s.pending, r.ID)
	}
	if shared {
		s.shared[r.ID] = true
	}
	if !s.scheduled {
		s.scheduled = true
		s.nextOffer = c.now + 4*c.retry
		c.pendingKeys = append(c.pendingKeys, r.Command.K)
	}
}

func (c *core) enqueueValue(v *Value) {
	if v == nil {
		return
	}
	if v.Reference {
		return
	}

	for _, r := range v.Records {
		c.enqueue(r, true)
	}
}

func (c *core) pendingRecords(k state.Key, shared *bool) []Record {
	s := c.shard(k)
	ids := make([]defs.RequestID, 0, len(s.pending))
	for _, id := range s.pending {
		if s.queued[id] && (shared == nil || s.shared[id] == *shared) {
			ids = append(ids, id)
		}
	}
	sort.Slice(ids, func(i, j int) bool { return c.commandUIDs[ids[i]] < c.commandUIDs[ids[j]] })
	var records []Record
	size := 0
	for _, id := range ids {
		r := c.known[id]
		if len(records) == maxBatch || size+recordBytes(r) > maxBatchBytes {
			break
		}
		size += recordBytes(r)
		records = append(records, r)
	}
	return records
}

func (c *core) pendingBatch(k state.Key, shared bool) *Value {
	// Upstream batches queued Singles before falling back to a minimum-UID retry.
	return c.makePendingValue(k, c.pendingRecords(k, &shared))
}

func (c *core) pendingSingle(k state.Key) *Value {
	s := c.shard(k)
	var chosen defs.RequestID
	var lowest uint64
	found := false
	for _, id := range s.pending {
		if !s.queued[id] || !s.shared[id] {
			continue
		}
		// Non-voters repair their own original input. Voters still retain and
		// relay any received input, including that of a failed input owner.
		if c.id >= c.n && id.Client != int32(c.id) {
			continue
		}
		uid := c.commandUIDs[id]
		if !found || uid < lowest {
			chosen, lowest, found = id, uid, true
		}
	}
	if !found {
		return &Value{Proposer: c.id}
	}
	return c.makePendingValue(k, []Record{c.known[chosen]})
}

type payloadEdgeID struct {
	Proposer int
	Edge     graphEdge
	ID       defs.RequestID
}

func (c *core) offerValue(k state.Key, v *Value, repair bool) {
	if len(v.Records) == 0 {
		return
	}
	c.stats.OfferedCommands += uint64(len(v.Records))
	m := message{Kind: offer, Key: k, ID: v.Records[0].ID, Proposer: v.Proposer, Value: v}
	if repair {
		m.GraphTime = -1
		// Make retained input available at the live reproposer and coordinator.
		// Broadcasting it from every holder causes quadratic repair traffic.
		proposer, coordinator := c.reproposer(), c.coordinator()
		if proposer != c.id {
			c.offerRepair(proposer, m)
		}
		if coordinator != proposer && coordinator != c.id {
			c.offerRepair(coordinator, m)
		}
		return
	}
	for _, edge := range c.plan.Graphs[v.Proposer].States[c.id][0].Outgoing {
		if c.plan.Graphs[v.Proposer].Payload[edge] {
			m.GraphTime = int64(edge.Time)
			c.send(edge.To, m)
		}
	}
}

// Repair Singles are immutable input handoffs, not graph edges or votes. The
// receiving core retains pending input until ordered, including losing inputs.
// A healthy lossless stream therefore needs only one handoff per UID/target.
// A new target has a different bit; unestablished streams keep ordinary retries.
func (c *core) offerRepair(target int, m message) {
	v := m.Value
	if !c.reliableTarget(target) || v == nil || v.Reference || v.Batch != nil || len(v.Records) != 1 {
		c.send(target, m)
		return
	}
	if c.repairHandoffs == nil {
		c.repairHandoffs = make(map[payloadObject]uint64)
	}
	object := payloadObject{m.Key, v.UID}
	bit := uint64(1) << target
	if c.repairHandoffs[object]&bit != 0 {
		c.stats.SuppressedRepairOffers++
		return
	}
	c.repairHandoffs[object] |= bit
	c.send(target, m)
}

// Pending queues can outlive a slot (and its proposer). Revisit them fairly even
// when there is no active slot to drive the ordinary recovery timer.
func (c *core) retryPending() {
	count := len(c.pendingKeys) - c.pendingHead
	if count > 256 {
		count = 256
	}
	for ; count > 0; count-- {
		k := c.pendingKeys[c.pendingHead]
		c.pendingHead++
		s := c.shard(k)
		if len(s.queued) == 0 {
			s.scheduled = false
			s.pending = nil
			continue
		}
		c.pendingKeys = append(c.pendingKeys, k)
		c.propose(k)
		// Reserve three quarters of each tick for active-slot recovery and reads.
		// A large payload backlog must not prevent the census that drains it.
		if c.now < s.nextOffer || c.retryBudget <= 192 {
			continue
		}
		s.nextOffer = c.now + 4*c.retry
		// Retry a FIFO prefix. Any survivor may relay already received input.
		// This is payload repair only and cannot change a slot's acceptance.
		v := c.pendingSingle(k)
		if len(v.Records) > 0 {
			c.retryBudget--
			c.offerValue(k, v, true)
		}
	}
	if c.pendingHead > 4096 && c.pendingHead*2 >= len(c.pendingKeys) {
		copy(c.pendingKeys, c.pendingKeys[c.pendingHead:])
		c.pendingKeys = c.pendingKeys[:len(c.pendingKeys)-c.pendingHead]
		c.pendingHead = 0
	}
}
