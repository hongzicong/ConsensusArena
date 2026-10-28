package kcensus

import "github.com/hongzicong/ConsensusArena/state"

// shardFor is the only application-key to protocol-namespace conversion.
// Zero preserves the previous per-key logs, including negative application keys.
// The mapping is immutable and included in the configured peer digest. Every
// Record keeps its original Command.K for validation, execution and deduplication.
func (c *core) shardFor(key state.Key) state.Key { return shardFor(key, c.shardCount) }

func shardFor(key, count state.Key) state.Key {
	if count == 0 {
		return key
	}
	shard := key % count
	if shard < 0 {
		shard += count
	}
	return shard
}

func (c *core) validShard(shard state.Key) bool {
	return c.shardCount == 0 || shard >= 0 && shard < c.shardCount
}
