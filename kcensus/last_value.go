package kcensus

import (
	"fmt"

	"github.com/hongzicong/ConsensusArena/state"
)

func (c *core) lastValue(k state.Key) (uint64, bool) {
	if s := c.shards[k]; s != nil {
		return s.lastUID, s.hasLast
	}
	s := c.pool.sleepingState(k)
	return s.lastUID, s.hasLast
}

// Match ShardMultiSink's predecessor snapshot. ReadResponse explicitly used
// None upstream; Arena-only control/result RPCs have no log-context assertion.
func (c *core) attachLastValue(m *message) {
	m.LastV = nil
	switch m.Kind {
	case spread, abandon, prepare, promise, accept, accepted, commit, nack, fetch,
		readQuery, offer:
		if uid, ok := c.lastValue(m.Shard); ok {
			m.LastV = &uid // Detached from the mutable, reusable physical shard.
		}
	}
}

// Upstream ready_to_process compares last_v only for the current slot. A
// future message reaches this check again when its existing buffer is replayed.
func (c *core) checkLastValue(m message) {
	if m.LastV == nil || m.Slot == 0 || m.Slot != c.executed(m.Shard)+1 {
		return
	}
	uid, ok := c.lastValue(m.Shard)
	if !ok || uid != *m.LastV {
		panic(fmt.Sprintf("KCensus last_v mismatch shard=%d slot=%d from=%d local_has_last=%t local_last=%d message_last=%d", m.Shard, m.Slot, m.From, ok, uid, *m.LastV))
	}
}
