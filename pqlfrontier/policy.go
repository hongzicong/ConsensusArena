package pqlfrontier

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"

	"github.com/hongzicong/ConsensusArena/config"
	"github.com/hongzicong/ConsensusArena/heatprofile"
	"github.com/hongzicong/ConsensusArena/state"
)

// A policy is an immutable lease configuration. Incremental Lease-Paxos values
// overlay their parent, so live promises can still refer to older versions.
type leasePolicy struct {
	n, leader   int
	holders     map[state.Key]uint64
	defaultMask uint64
	excluded    uint64
	parent      *leasePolicy
}

func loadLeasePolicy(c *config.Config) (*leasePolicy, *heatprofile.Profile, error) {
	n := len(c.ReplicaAliases)
	if n < 3 || n > 63 || n%2 == 0 {
		return nil, nil, fmt.Errorf("PQL requires odd membership of 3..63 replicas")
	}
	leader := int(c.Plan.LeaderID())
	if leader < 0 || leader >= n {
		return nil, nil, fmt.Errorf("PQL deployment leader is outside membership")
	}
	profile, err := heatprofile.LoadRegional(c.HeatProfilePath, c.HeatProfileSHA256, c.KeyCount, sortedClients(c.ClientAddrs))
	if err != nil {
		return nil, nil, fmt.Errorf("PQL regional empirical profile: %w", err)
	}
	indices := make(map[string]int, n)
	for id, alias := range c.ReplicaAliases {
		indices[alias] = id
	}
	reads := make(map[state.Key][]uint64)
	for _, client := range profile.File.Clients {
		alias, ok := c.Plan.ClientRoutes[client.Client]
		if !ok {
			return nil, nil, fmt.Errorf("PQL plan has no route for sampled client %q", client.Client)
		}
		region, ok := indices[alias]
		if !ok {
			return nil, nil, fmt.Errorf("PQL route %q is not a replica", alias)
		}
		for _, row := range client.Keys {
			if row.Reads == 0 {
				continue
			}
			key := state.Key(row.Key)
			if reads[key] == nil {
				reads[key] = make([]uint64, n)
			}
			reads[key][region] += row.Reads
		}
	}
	holders := make(map[state.Key]uint64, len(reads))
	for key, counts := range reads {
		ids := make([]int, 0, n-1)
		for id := 0; id < n; id++ {
			if id != leader {
				ids = append(ids, id)
			}
		}
		sort.Slice(ids, func(i, j int) bool {
			if counts[ids[i]] != counts[ids[j]] {
				return counts[ids[i]] > counts[ids[j]]
			}
			return ids[i] < ids[j]
		})
		mask := uint64(1) << uint(leader)
		for _, id := range ids[:n/2] {
			mask |= uint64(1) << uint(id)
		}
		holders[key] = mask
	}
	defaultMask := uint64(0)
	for _, replica := range c.Plan.FastQuorum {
		if replica.Rank < 0 || replica.Rank >= n {
			return nil, nil, fmt.Errorf("PQL default write quorum contains invalid rank")
		}
		defaultMask |= uint64(1) << uint(replica.Rank)
	}
	if bitCount(defaultMask) != n/2+1 || defaultMask&(uint64(1)<<uint(leader)) == 0 {
		return nil, nil, fmt.Errorf("PQL default write quorum must be a majority containing the deployment leader")
	}
	return &leasePolicy{n: n, leader: leader, holders: holders, defaultMask: defaultMask}, profile, nil
}

func sortedClients(clients map[string]string) []string {
	names := make([]string, 0, len(clients))
	for name := range clients {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func (p *leasePolicy) holderMask(key state.Key) uint64 {
	for version := p; version != nil; version = version.parent {
		if mask, ok := version.holders[key]; ok {
			return mask &^ p.excluded
		}
	}
	return uint64(1) << uint(p.leader) &^ p.excluded
}

func (p *leasePolicy) writeMask(command state.Command) uint64 {
	mask := uint64(1) << uint(p.leader)
	if command.Op == state.PUT {
		mask |= p.holderMask(command.K)
	}
	preferred := p.defaultMask &^ p.excluded
	for id := 0; id < p.n && bitCount(mask) < p.n/2+1; id++ {
		if preferred&(uint64(1)<<uint(id)) != 0 {
			mask |= uint64(1) << uint(id)
		}
	}
	for id := 0; id < p.n && bitCount(mask) < p.n/2+1; id++ {
		if p.excluded&(uint64(1)<<uint(id)) == 0 {
			mask |= uint64(1) << uint(id)
		}
	}
	return mask
}

func (p *leasePolicy) holds(id int32, key state.Key) bool {
	return id >= 0 && int(id) < p.n && p.holderMask(key)&(uint64(1)<<uint(id)) != 0
}

func emitPolicyRecord(c *config.Config, profile *heatprofile.Profile, p *leasePolicy) error {
	counts := make([]int, p.n)
	for _, mask := range p.holders {
		for id := range counts {
			if mask&(uint64(1)<<uint(id)) != 0 {
				counts[id]++
			}
		}
	}
	record := map[string]interface{}{
		"policy": "sampled-majority-holder-genesis", "profile_sha256": profile.SHA256,
		"sample_source": profile.File.Source, "sample_seed": profile.File.SampleSeed,
		"sample_requests": profile.File.Requests, "configured_read_keys": len(p.holders),
		"leader": c.ReplicaAliases[p.leader], "holder_count_per_configured_key": p.n/2 + 1,
		"holder_selection":           "deployment leader plus highest sampled regional read counts; replica rank breaks ties",
		"default_unobserved_policy":  "leader-only read lease; topology-selected majority write quorum",
		"configured_keys_per_holder": counts, "frozen": false,
		"updates": "independent Lease-Paxos log; forwarded-read counts every 10 seconds; failed-holder exclusions after 5 second grace",
	}
	data, err := json.Marshal(record)
	if err != nil {
		return err
	}
	fmt.Fprintln(os.Stderr, "PQL_FRONTIER_SAMPLE_POLICY", string(data))
	return nil
}

func bitCount(mask uint64) int {
	n := 0
	for mask != 0 {
		mask &= mask - 1
		n++
	}
	return n
}
