package kcensus

import "github.com/hongzicong/ConsensusArena/state"

// A census may select input whose original payload tree was interrupted. Phase
// 2 must carry the complete immutable object, not depend on bounded cache repair.
func (c *core) valuePayload(m message) message {
	m.References = false
	for _, value := range []*Value{m.Value, m.Fast} {
		if value == nil || value.Batch == nil {
			continue
		}
		for _, uid := range value.Batch.Members {
			single := c.shard(m.Key).objects[uid]
			if single == nil || single.Batch != nil || len(single.Records) != 1 {
				panic("selected batch has missing Single payload")
			}
			m.Values = append(m.Values, single)
		}
	}
	return m
}

type payloadObject struct {
	Key state.Key
	UID uint64
}
type payloadWaitKey struct {
	Kind                         uint8
	From, Proposer               int
	Key                          state.Key
	Slot, Ballot, AcceptedBallot uint64
	ID                           CommandID
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
	k := payloadWaitKey{Kind: m.Kind, From: m.From, Proposer: m.Proposer, Key: m.Key, Slot: m.Slot, Ballot: m.Ballot, AcceptedBallot: m.AcceptedBallot, ID: m.ID, GraphTime: m.GraphTime}
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
	m.Value, ok = c.resolveValue(m.Key, m.Value)
	ready = ready && ok
	m.Fast, ok = c.resolveValue(m.Key, m.Fast)
	ready = ready && ok
	for i := range m.Reports {
		m.Reports[i].Value, _ = c.resolveValue(m.Key, m.Reports[i].Value)
	}
	return ready
}
func (c *core) waitPayload(m message) {
	c.queuePayload(m, true)
}
func (c *core) queuePayload(m message, query bool) {
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
		obj := c.shard(m.Key).objects[v.UID]
		if obj == nil {
			w.missing[payloadObject{m.Key, v.UID}] = true
		} else if obj.Batch != nil {
			for _, uid := range obj.Batch.Members {
				if c.shard(m.Key).objects[uid] == nil {
					w.missing[payloadObject{m.Key, uid}] = true
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
	c.payloadRotation = append(c.payloadRotation, w)
	if query {
		c.stats.PayloadWaits++
	}
	if query && len(c.payloadWaiting) == 1 {
		c.queryPayload(m.Key, []*Value{m.Value, m.Fast}, m.Reports)
		c.payloadRetry = c.now + c.retry
	}
}
func (c *core) queryPayload(k state.Key, values []*Value, reports []nodeReport) {
	for _, r := range reports {
		values = append(values, r.Value)
	}
	seen := map[uint64]bool{}
	var ids []uint64
	missing := func(uid uint64) {
		if c.shard(k).objects[uid] == nil && !seen[uid] {
			seen[uid] = true
			ids = append(ids, uid)
		}
	}
	for _, v := range values {
		if v == nil {
			continue
		}
		obj := c.shard(k).objects[v.UID]
		if obj == nil {
			missing(v.UID)
		} else if obj.Batch != nil {
			for _, uid := range obj.Batch.Members {
				missing(uid)
			}
		}
	}
	for len(ids) > 0 {
		count := len(ids)
		if count > maxBatch {
			count = maxBatch
		}
		c.broadcastAll(message{Kind: payloadQuery, Key: k, UIDs: ids[:count]})
		c.stats.PayloadQueries++
		ids = ids[count:]
	}
}
func (c *core) answerPayload(m message) {
	var values []*Value
	size := 0
	flush := func() {
		if len(values) > 0 {
			c.send(m.From, message{Kind: payloadReply, Key: m.Key, Values: values})
			values = nil
			size = 0
		}
	}
	seen := map[uint64]bool{}
	for _, uid := range m.UIDs {
		v := c.shard(m.Key).objects[uid]
		if v == nil || seen[uid] {
			continue
		}
		seen[uid] = true
		bytes := 32
		if v.Batch != nil {
			bytes += 8 * len(v.Batch.Members)
		} else {
			bytes += recordBytes(v.Records[0])
		}
		if size+bytes > maxBatchBytes || len(values) == maxBatch {
			flush()
		}
		values = append(values, v)
		size += bytes
	}
	flush()
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
				delete(c.payloadWaiting, key)
				for dep := range w.missing {
					delete(c.payloadDependencies[dep], key)
					if len(c.payloadDependencies[dep]) == 0 {
						delete(c.payloadDependencies, dep)
					}
				}
				m := w.message
				if c.resolveMessage(&m) {
					c.stepMessage(m, true)
				} else {
					c.queuePayload(m, false)
				}
			}
		}
	}
	c.compactPayloadRotation()
}

func (c *core) compactPayloadRotation() {
	if c.payloadHead < 4096 && len(c.payloadRotation) < 4*len(c.payloadWaiting)+4096 {
		return
	}
	var live []*payloadWait
	for _, w := range c.payloadRotation[c.payloadHead:] {
		if c.payloadWaiting[w.key] == w {
			live = append(live, w)
		}
	}
	c.payloadRotation = live
	c.payloadHead = 0
}

func (c *core) retryPayload() {
	if len(c.payloadWaiting) == 0 || c.now < c.payloadRetry {
		return
	}
	c.payloadRetry = c.now + c.retry
	// At most 32 waiting messages per sweep; rotate for fairness and reserve
	// the remaining budget for slot recovery and reads.
	count := len(c.payloadRotation) - c.payloadHead
	if count > 32 {
		count = 32
	}
	for i := 0; i < count && c.retryBudget > 0; i++ {
		w := c.payloadRotation[c.payloadHead]
		c.payloadHead++
		if c.payloadWaiting[w.key] != w {
			continue
		}
		c.payloadRotation = append(c.payloadRotation, w)
		m := w.message
		c.retryBudget--
		c.queryPayload(m.Key, []*Value{m.Value, m.Fast}, m.Reports)
	}
	c.compactPayloadRotation()
}
