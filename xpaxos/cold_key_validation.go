package xpaxos

import "github.com/hongzicong/ConsensusArena/state"

// Membership only: all other payload/evidence checks remain at the call site.
func (c *coldReplica) isRecoveryKey(key state.Key) bool {
	c.stats.RecoveryRankChecks++
	if !c.r.engine.opt.KeyPolicy.cold(key, c.cutoff) {
		c.stats.RecoveryRankRejected++
		return false
	}
	if key < c.cutoff {
		c.stats.RecoveryRankAcceptedBelowNumericCutoff++
	}
	return true
}
