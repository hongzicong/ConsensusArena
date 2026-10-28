package kcensus

import (
	"github.com/hongzicong/ConsensusArena/replica/defs"
	"github.com/hongzicong/ConsensusArena/state"
)

// Match upstream shard_pool.rs: a dense sleeping-state table, stable physical
// objects, a stack of free indices, and one-at-a-time growth after exhaustion.
const defaultShardPoolSize = 1000

type shardPool struct {
	sleeping    []sleepingShard
	physical    []*shard
	free        []int
	initialSize int
	initial     sleepingShard
	// Identity mapping also supports arbitrary signed Arena keys. Keep their
	// sleeping metadata separately without remapping or restricting those keys.
	outside map[state.Key]sleepingShard
}

func newShardPool(logicalShards, pid int) *shardPool {
	if logicalShards <= 0 {
		panic("KCensus logical shard count must be positive")
	}
	size := min(logicalShards, defaultShardPoolSize)
	p := &shardPool{sleeping: make([]sleepingShard, logicalShards), physical: make([]*shard, size), free: make([]int, size), initialSize: size, initial: sleepingShard{nextUID: uint64(2 * pid)}}
	for i := range p.sleeping {
		p.sleeping[i] = p.initial
	}
	for i := range p.physical {
		p.physical[i] = newPhysicalShard(i)
		p.free[size-1-i] = i
	}
	return p
}

func newPhysicalShard(index int) *shard {
	return &shard{physicalIndex: index, objects: make(map[uint64]*Value), slots: make(map[uint64]*slotState), queued: make(map[defs.RequestID]bool), shared: make(map[defs.RequestID]bool)}
}

func (p *shardPool) acquire() (*shard, bool) {
	var index int
	grew := len(p.free) == 0
	if grew {
		index = len(p.physical)
		p.physical = append(p.physical, newPhysicalShard(index))
	} else {
		last := len(p.free) - 1
		index = p.free[last]
		p.free = p.free[:last]
	}
	s := p.physical[index]
	if s.inUse {
		panic("KCensus physical shard claimed twice")
	}
	s.inUse = true
	return s, grew
}

func (p *shardPool) release(s *shard) {
	if !s.inUse || s.physicalIndex < 0 || s.physicalIndex >= len(p.physical) || p.physical[s.physicalIndex] != s {
		panic("KCensus invalid physical shard release")
	}
	s.inUse = false
	p.free = append(p.free, s.physicalIndex)
}

func (p *shardPool) sleepingState(k state.Key) sleepingShard {
	if k >= 0 && k < state.Key(len(p.sleeping)) {
		return p.sleeping[k]
	}
	if s, ok := p.outside[k]; ok {
		return s
	}
	return p.initial
}

func (p *shardPool) wake(k state.Key) sleepingShard {
	s := p.sleepingState(k)
	delete(p.outside, k)
	return s
}

func (p *shardPool) sleep(k state.Key, s sleepingShard) {
	if k >= 0 && k < state.Key(len(p.sleeping)) {
		p.sleeping[k] = s
		return
	}
	if p.outside == nil {
		p.outside = make(map[state.Key]sleepingShard)
	}
	p.outside[k] = s
}
