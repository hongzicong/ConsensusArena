package kcensus

import (
	"fmt"
	"github.com/hongzicong/ConsensusArena/state"
	"math"
)

// The registry is per shard: equal scalar UIDs on different keys are unrelated.
func (c *core) nextUID(k state.Key) uint64 {
	s := c.shard(k)
	uid := s.nextUID
	if uid > math.MaxUint64-uint64(2*c.m)-1 {
		panic("KCensus UID space exhausted")
	}
	s.nextUID += uint64(2 * c.m)
	return uid
}
func (c *core) singleValue(r Record) *Value {
	if uid, ok := c.commandUIDs[r.ID]; ok {
		v := *c.shard(r.Command.K).objects[uid]
		v.Proposer = c.id
		return &v
	}
	v := &Value{UID: c.nextUID(r.Command.K), Proposer: c.id, Records: []Record{r}}
	if err := c.storeValue(r.Command.K, v); err != nil {
		panic(err)
	}
	return v
}
func (c *core) storeValue(k state.Key, v *Value) error {
	if v == nil || v.Reference {
		return nil
	}
	if v.Proposer < 0 || v.Proposer >= c.m {
		return fmt.Errorf("invalid value context")
	}
	s := c.shard(k)
	if v.UID&1 == 0 {
		if v.Batch != nil || len(v.Records) != 1 {
			return fmt.Errorf("invalid Single descriptor")
		}
		r := v.Records[0]
		if r.Command.Op != state.PUT || r.Command.K != k || len(r.Command.V) > 65535 {
			return fmt.Errorf("invalid Single command")
		}
		if old, ok := c.commandUIDs[r.ID]; ok && old != v.UID {
			return fmt.Errorf("request has two Single UIDs")
		}
		if old := s.objects[v.UID]; old != nil && !sameRecord(old.Records[0], r) {
			return fmt.Errorf("Single UID reused")
		}
		c.remember(r)
		c.commandUIDs[r.ID] = v.UID
		if s.objects[v.UID] == nil {
			x := *v
			x.Records = []Record{c.known[r.ID]}
			s.objects[v.UID] = &x
			c.payloadArrived(k, v.UID)
		}
	} else {
		if v.Batch == nil || v.Batch.Slot == 0 || len(v.Batch.Members) < 2 || len(v.Batch.Members) > maxBatch {
			return fmt.Errorf("invalid Batch descriptor")
		}
		seen := map[uint64]bool{}
		for _, uid := range v.Batch.Members {
			if uid&1 != 0 || seen[uid] {
				return fmt.Errorf("nested or duplicate Batch member")
			}
			seen[uid] = true
		}
		if old := s.objects[v.UID]; old != nil {
			if old.Batch.Slot != v.Batch.Slot || len(old.Batch.Members) != len(v.Batch.Members) {
				return fmt.Errorf("Batch UID reused")
			}
			for i, uid := range old.Batch.Members {
				if uid != v.Batch.Members[i] {
					return fmt.Errorf("Batch UID reordered")
				}
			}
		} else {
			x := *v
			x.Records = nil
			x.Batch = &Batch{v.Batch.Slot, append([]uint64(nil), v.Batch.Members...)}
			s.objects[v.UID] = &x
			c.payloadArrived(k, v.UID)
		}
	}
	return nil
}
func (c *core) materialize(k state.Key, v *Value) (*Value, bool) {
	if v == nil {
		return nil, true
	}
	obj := c.shard(k).objects[v.UID]
	if obj == nil {
		return v, false
	}
	result := *obj
	result.Proposer = v.Proposer
	result.Reference = false
	if obj.Batch == nil {
		return &result, true
	}
	result.Records = make([]Record, 0, len(obj.Batch.Members))
	size := 0
	for _, uid := range obj.Batch.Members {
		single := c.shard(k).objects[uid]
		if single == nil {
			return v, false
		}
		if single.Batch != nil || len(single.Records) != 1 {
			panic("Batch member is not a Single")
		}
		size += recordBytes(single.Records[0])
		if size > maxBatchBytes {
			panic("Batch exceeds byte limit")
		}
		result.Records = append(result.Records, single.Records[0])
	}
	return &result, true
}

// Batch bounded pending Singles, or preserve the UID of a single retry.
func (c *core) makePendingValue(k state.Key, records []Record) *Value {
	if len(records) == 0 {
		return &Value{Proposer: c.id}
	}
	if len(records) == 1 {
		uid, ok := c.commandUIDs[records[0].ID]
		if !ok {
			panic("pending input has no UID")
		}
		v, _ := c.materialize(k, &Value{UID: uid, Proposer: c.id, Reference: true})
		return v
	}
	s := c.shard(k)
	members := make([]uint64, len(records))
	for i, r := range records {
		members[i] = c.commandUIDs[r.ID]
	}
	// Do not allocate another descriptor on a repeated pending-queue scan.
	if s.pendingValue != nil && s.pendingValue.Batch != nil && s.pendingValue.Batch.Slot == s.executed+1 && len(members) == len(s.pendingValue.Batch.Members) {
		same := true
		for i, uid := range members {
			if uid != s.pendingValue.Batch.Members[i] {
				same = false
				break
			}
		}
		if same {
			return s.pendingValue
		}
	}
	v := &Value{UID: c.nextUID(k) + 1, Proposer: c.id, Batch: &Batch{s.executed + 1, members}, Records: records}
	if err := c.storeValue(k, v); err != nil {
		panic(err)
	}
	s.pendingValue = v
	return v
}
