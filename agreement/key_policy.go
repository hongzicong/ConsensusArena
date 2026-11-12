package agreement

import (
	"github.com/hongzicong/ConsensusArena/agreement/heatpolicy"
	"github.com/hongzicong/ConsensusArena/config"
	"github.com/hongzicong/ConsensusArena/state"
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

// Keep generator settings private: planning and optional client certificates use
// the observed operation mix, at full precision, from the same frozen sample.
// No observed writes selects conservative ordinary write completion; it never
// disables PUT processing or proves that future requests will be read-only.
func sampledWriteFraction(c *config.Config) float64 {
	p := c.HeatProfile
	if p == nil || p.File.Requests == 0 {
		panic("validated heat profile required for sampled operation mix")
	}
	return float64(p.WriteRequests) / float64(p.File.Requests)
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
