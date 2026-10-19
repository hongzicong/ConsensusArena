package xpaxos

import (
	"github.com/hongzicong/ConsensusArena/config"
	"github.com/hongzicong/ConsensusArena/state"
	"github.com/hongzicong/ConsensusArena/xpaxos/heatpolicy"
)

// Only the frozen sample defines cohorts; raw keys/hashes remain unchanged.
type keyPolicy struct {
	count     int
	hotCutoff int
	inverse   []int32
}

func newKeyPolicy(cfg *config.Config) *keyPolicy {
	if cfg.HeatProfile == nil {
		panic("XPaxos profile must be validated before construction")
	}
	return &keyPolicy{count: cfg.KeyCount, hotCutoff: heatpolicy.HotCutoff(cfg), inverse: cfg.HeatProfile.Ranks}
}
func (p *keyPolicy) rank(key state.Key) int64 {
	if p == nil || key < 0 || int64(key) >= int64(p.count) {
		return -1
	}
	return int64(p.inverse[int(key)])
}
func (p *keyPolicy) cold(key, cutoff state.Key) bool {
	rank := p.rank(key)
	return rank >= 0 && rank >= int64(cutoff)
}
func (p *keyPolicy) hot(key state.Key) bool {
	rank := p.rank(key)
	return rank >= 0 && rank < int64(p.hotCutoff)
}
