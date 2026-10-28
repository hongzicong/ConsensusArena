package kcensus

import (
	"github.com/hongzicong/ConsensusArena/replica/defs"
	"github.com/hongzicong/ConsensusArena/state"
)

// A census may select input whose original payload tree was interrupted. Phase
// 2 must carry the complete immutable object, not depend on bounded cache repair.
func (c *core) valuePayload(m message) message {
	m.References = false
	for _, value := range []*Value{m.Value, m.Fast} {
		if value == nil || value.Batch == nil {
			continue
		}
		for _, uid := range value.Batch.Members {
			single := c.shard(m.Shard).objects[uid]
			if single == nil || single.Batch != nil || len(single.Records) != 1 {
				panic("selected batch has missing Single payload")
			}
			m.Values = append(m.Values, single)
		}
	}
	return m
}

type payloadObject struct {
	Shard state.Key
	UID   uint64
}
type payloadWaitKey struct {
	Kind                         uint8
	From, Proposer               int
	Shard                        state.Key
	Slot, Ballot, AcceptedBallot uint64
	ID                           defs.RequestID
	GraphTime                    int64
	Value, Fast                  uint64
	HasValue, HasFast            bool
}
type payloadWait struct {
	key     payloadWaitKey
	message message
	missing map[payloadObject]bool
}

func payloadKey(m message) payloadWaitKey {
	k := payloadWaitKey{Kind: m.Kind, From: m.From, Proposer: m.Proposer, Shard: m.Shard, Slot: m.Slot, Ballot: m.Ballot, AcceptedBallot: m.AcceptedBallot, ID: m.ID, GraphTime: m.GraphTime}
	if m.Value != nil {
		k.HasValue = true
		k.Value = m.Value.UID
	}
	if m.Fast != nil {
		k.HasFast = true
		k.Fast = m.Fast.UID
	}
	return k
}

func (c *core) payloadArrived(k state.Key, uid uint64) {
	obj := payloadObject{k, uid}
	if len(c.payloadDependencies[obj]) > 0 {
		c.payloadReady = append(c.payloadReady, obj)
	}
}

// A UID-only message is replayed only after its descriptor and all Singles exist.
func (c *core) resolveValue(k state.Key, v *Value) (*Value, bool) {
	if v == nil {
		return nil, true
	}
	if err := c.storeValue(k, v); err != nil {
		panic(err)
	}
	return c.materialize(k, v)
}
func (c *core) resolveMessage(m *message) bool {
	ready := true
	var ok bool
	m.Value, ok = c.resolveValue(m.Shard, m.Value)
	ready = ready && ok
	m.Fast, ok = c.resolveValue(m.Shard, m.Fast)
	ready = ready && ok
	for i := range m.Reports {
		m.Reports[i].Value, _ = c.resolveValue(m.Shard, m.Reports[i].Value)
	}
	return ready
}
func (c *core) waitPayload(m message) {
	c.stats.PayloadWaits++
	if m.Kind == commit && m.Slot > c.executed(m.Shard) {
		// A UID-only decision may arrive after all donors retired its payload.
		// Keep a timed gap even though reference resolution cannot finish yet.
		c.slot(m.Shard, m.Slot)
	}
	c.queuePayload(m)
}
func (c *core) queuePayload(m message) {
	key := payloadKey(m)
	if old := c.payloadWaiting[key]; old != nil {
		old.message = m
		return
	}
	if c.payloadWaiting == nil {
		c.payloadWaiting = make(map[payloadWaitKey]*payloadWait)
		c.payloadDependencies = make(map[payloadObject]map[payloadWaitKey]bool)
	}
	w := &payloadWait{key: key, message: m, missing: make(map[payloadObject]bool)}
	for _, v := range []*Value{m.Value, m.Fast} {
		if v == nil {
			continue
		}
		obj := c.shard(m.Shard).objects[v.UID]
		if obj == nil {
			w.missing[payloadObject{m.Shard, v.UID}] = true
		} else if obj.Batch != nil {
			for _, uid := range obj.Batch.Members {
				if c.shard(m.Shard).objects[uid] == nil {
					w.missing[payloadObject{m.Shard, uid}] = true
				}
			}
		}
	}
	for obj := range w.missing {
		if c.payloadDependencies[obj] == nil {
			c.payloadDependencies[obj] = make(map[payloadWaitKey]bool)
		}
		c.payloadDependencies[obj][key] = true
	}
	c.payloadWaiting[key] = w
	if c.payloadByShard == nil {
		c.payloadByShard = make(map[state.Key]map[payloadWaitKey]*payloadWait)
	}
	if c.payloadByShard[m.Shard] == nil {
		c.payloadByShard[m.Shard] = make(map[payloadWaitKey]*payloadWait)
	}
	c.payloadByShard[m.Shard][key] = w
}

func (c *core) drainPayload() {
	if c.payloadDraining || len(c.payloadReady) == 0 {
		return
	}
	c.payloadDraining = true
	defer func() { c.payloadDraining = false }()
	for len(c.payloadReady) > 0 {
		ready := c.payloadReady
		c.payloadReady = nil
		for _, obj := range ready {
			keys := c.payloadDependencies[obj]
			delete(c.payloadDependencies, obj)
			for key := range keys {
				w := c.payloadWaiting[key]
				if w == nil {
					continue
				}
				m := w.message
				c.removePayloadWait(w)

				if c.resolveMessage(&m) {
					c.receiveMessage(m, true)
				} else {
					c.queuePayload(m)
				}
			}
		}
	}
}

func (c *core) removePayloadWait(w *payloadWait) {
	if c.payloadWaiting[w.key] != w {
		return
	}
	delete(c.payloadWaiting, w.key)
	delete(c.payloadByShard[w.key.Shard], w.key)
	if len(c.payloadByShard[w.key.Shard]) == 0 {
		delete(c.payloadByShard, w.key.Shard)
	}
	c.touched[w.key.Shard] = true
	for dep := range w.missing {
		delete(c.payloadDependencies[dep], w.key)
		if len(c.payloadDependencies[dep]) == 0 {
			delete(c.payloadDependencies, dep)
		}
	}
	w.message = message{}
	w.missing = nil
}
