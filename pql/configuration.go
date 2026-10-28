package pql

import (
	"sort"
	"time"

	"github.com/hongzicong/ConsensusArena/replica/defs"
	"github.com/hongzicong/ConsensusArena/state"
)

const (
	failureGrace          = 5 * time.Second
	configurationInterval = 10 * time.Second
	configurationBatch    = 5000
)

// Lease-Paxos uses an independent instance of the same majority log, with its
// own ballots, accepted values, recovery, and execution prefix. Both logs are
// driven by the replica's one event-loop goroutine.
func (c *core) proposeConfiguration(change *LeaseChange, now time.Time) {
	c.configSequence++
	r := Request{ID: defs.RequestID{Client: -2 - c.id, Sequence: c.configSequence},
		Origin: c.id, Change: change}
	c.configLog.submit(r, now)
	c.lastConfigProposal = now
}

func (c *core) commitConfiguration(slot int64, change *LeaseChange) {
	epoch := genesisLeaseEpoch + slot + 1
	previous := c.policies[c.latestEpoch]
	if change == nil {
		c.policies[epoch] = previous
	} else {
		policy := &leasePolicy{n: c.n, leader: int(change.Leader),
			parent: previous, excluded: previous.excluded | change.Excluded,
			holders: make(map[state.Key]uint64, len(change.Updates)), defaultMask: previous.defaultMask}
		for _, update := range change.Updates {
			policy.holders[update.Key] = update.Holders
		}
		c.policies[epoch] = policy
	}
	c.latestEpoch = epoch
}

func (c *core) hasUnexcludedSuspect() bool {
	return c.suspected&^c.policies[c.latestEpoch].excluded != 0
}

func (c *core) maintainConfigurations(now time.Time, alive []bool) {
	// A replica cannot use a remote accusation to suspect itself.
	c.suspected &^= uint64(1) << uint(c.id)
	if c.nextConfiguration.IsZero() {
		c.nextConfiguration = now.Add(configurationInterval)
		for peer := range c.lease.lastResponse {
			c.lease.lastResponse[peer] = now
		}
	}
	for peer, up := range alive {
		if int32(peer) == c.id {
			continue
		}
		if up {
			c.unresponsive[peer] = time.Time{}
		} else if c.unresponsive[peer].IsZero() {
			c.unresponsive[peer] = now
		}
		lastReply := c.lease.lastResponse[peer]
		if c.policies[c.latestEpoch].excluded&(uint64(1)<<uint(peer)) != 0 {
			continue
		}
		if (!c.unresponsive[peer].IsZero() && now.Sub(c.unresponsive[peer]) >= failureGrace) ||
			(!lastReply.IsZero() && c.lease.lastSent[peer].Sub(lastReply) >= failureGrace) {
			c.suspected |= uint64(1) << uint(peer)
		}
	}
	latest := c.policies[c.latestEpoch]
	if c.id == c.leader && c.configLog.active && !c.configLog.preparing &&
		(c.hasUnexcludedSuspect() || latest.leader != int(c.leader)) &&
		now.Sub(c.lastConfigProposal) >= time.Second {
		c.proposeConfiguration(&LeaseChange{Leader: c.leader, Excluded: c.suspected &^ (uint64(1) << uint(c.id))}, now)
	}
	if c.id == c.leader && c.configLog.active && !c.configLog.preparing && !now.Before(c.nextConfiguration) {
		c.proposeObservedHolders(now)
		c.nextConfiguration = now.Add(configurationInterval)
	}
	// §3.5 and the authors' implementation take the non-overlapping option:
	// stop renewing, wait out every outgoing promise, then change eras.
	if c.latestEpoch > c.lease.promisedBy {
		if !c.lease.canWriteOutside(now) {
			return
		}
		c.policy = c.policies[c.latestEpoch]
		c.lease.switchEpoch(c.latestEpoch, now)
	}
	if c.hasUnexcludedSuspect() || c.policy.excluded&(uint64(1)<<uint(c.id)) != 0 {
		return
	}
	if c.lease.started && now.Before(c.lease.renewAt) {
		return
	}
	eligible := append([]bool(nil), alive...)
	for peer := range eligible {
		eligible[peer] = eligible[peer] && c.policy.excluded&(uint64(1)<<uint(peer)) == 0
	}
	c.lease.tick(now, eligible, c.high, c.send)
}

// The authors count reads forwarded by non-holders. As in §3.4, an observed
// challenger displaces an existing holder only when its count is higher.
func (c *core) proposeObservedHolders(now time.Time) {
	policy := c.policies[c.latestEpoch]
	updates := make([]HolderUpdate, 0)
	keys := make([]state.Key, 0, len(c.readStats))
	for key := range c.readStats {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
	for _, key := range keys {
		counts := c.readStats[key]
		old := policy.holderMask(key)
		ids := make([]int, 0, c.n-1)
		for id := 0; id < c.n; id++ {
			if id != int(c.leader) && policy.excluded&(uint64(1)<<uint(id)) == 0 {
				ids = append(ids, id)
			}
		}
		sort.Slice(ids, func(i, j int) bool {
			a, b := ids[i], ids[j]
			if counts[a] != counts[b] {
				return counts[a] > counts[b]
			}
			ah, bh := old&(uint64(1)<<uint(a)) != 0, old&(uint64(1)<<uint(b)) != 0
			if ah != bh {
				return ah
			}
			return a < b
		})
		if len(ids) < c.n/2 {
			continue
		}
		mask := uint64(1) << uint(c.leader)
		for _, id := range ids[:c.n/2] {
			mask |= uint64(1) << uint(id)
		}
		if mask != old {
			updates = append(updates, HolderUpdate{Key: key, Holders: mask})
		}
	}
	c.readStats = map[state.Key][]uint64{}
	for start := 0; start < len(updates); start += configurationBatch {
		end := start + configurationBatch
		if end > len(updates) {
			end = len(updates)
		}
		c.proposeConfiguration(&LeaseChange{Leader: c.leader, Excluded: policy.excluded, Updates: updates[start:end]}, now)
	}
}
