package bodega

import (
	"encoding/json"
	"fmt"
	"math"
	"os"
	"sort"
	"strings"

	"github.com/hongzicong/ConsensusArena/config"
	"github.com/hongzicong/ConsensusArena/heatprofile"
	"github.com/hongzicong/ConsensusArena/paxos"
	"github.com/hongzicong/ConsensusArena/placement"
)

// Fix the selection fraction before looking at the sample: the top fifth of
// observed read keys in each region, rounded up. Unobserved and write-only keys
// never qualify. Read-count ties use key ID.
const hotKeyDivisor = 5

// Under IID uniform sampled keys, Cantelli's inequality bounds false rejection.
// Non-rejection only means uniform-compatible, not proof of true uniformity.
const uniformFalseRejection = .001

type uniformityEvidence struct {
	EqualKeyPairs float64 `json:"equal_key_pairs"`
	ExpectedPairs float64 `json:"uniform_expected_pairs"`
	Variance      float64 `json:"uniform_variance"`
	Threshold     float64 `json:"non_uniform_threshold"`
	NonUniform    bool    `json:"non_uniform"`
}

func sampledUniformity(sample heatprofile.File) uniformityEvidence {
	n, keys := float64(sample.Requests), float64(sample.KeyCount)
	expected := n * (n - 1) / (2 * keys)
	variance := expected * (1 - 1/keys)
	evidence := uniformityEvidence{
		ExpectedPairs: expected, Variance: variance,
		Threshold: expected + math.Sqrt(variance*(1-uniformFalseRejection)/uniformFalseRejection),
	}
	// Count collisions using observed operations only. Under the uniform null,
	// pair indicators are pairwise independent, even when they share a request.
	for _, row := range sample.Keys {
		count := float64(row.Reads + row.Writes)
		evidence.EqualKeyPairs += count * (count - 1) / 2
	}
	evidence.NonUniform = evidence.EqualKeyPairs > evidence.Threshold
	return evidence
}

type sampledRegion struct {
	Responder        string   `json:"responder"`
	Clients          []string `json:"clients"`
	Requests         uint64   `json:"sample_requests"`
	Reads            uint64   `json:"sample_reads"`
	ObservedReadKeys int      `json:"observed_read_keys"`
	HotKeyBudget     int      `json:"hot_key_budget"`
	SelectedKeys     int      `json:"selected_keys"`
	SelectedReads    uint64   `json:"selected_sample_reads"`
	reads            map[int]uint64
}

// PlanDeployment learns per-region hot keys exclusively from a frozen sample.
// Regions are the nearest-replica groups in the public deployment topology.
// Samples with reads and no observed writes, or uniform-compatible samples, use
// all replicas as responders. Otherwise original key IDs are preserved, and only
// adjacent keys with identical responder sets are coalesced into roster ranges.
// The leader follows Paxos's topology-only placement rule. No path reads
// generator parameters.
func PlanDeployment(conf *config.Config, t *placement.Topology) (config.DeploymentPlan, error) {
	profile, err := heatprofile.LoadRegional(conf.HeatProfilePath, conf.HeatProfileSHA256, conf.KeyCount, t.Clients)
	if err != nil {
		return config.DeploymentPlan{}, fmt.Errorf("Bodega regional empirical profile: %w", err)
	}
	uniformity := sampledUniformity(profile.File)
	var sampleReads, sampleWrites uint64
	for _, row := range profile.File.Keys {
		sampleReads += row.Reads
		sampleWrites += row.Writes
	}
	sampleReadOnly := sampleReads > 0 && sampleWrites == 0
	allResponders := sampleReadOnly || !uniformity.NonUniform
	leaderPlan, err := paxos.PlanDeployment(conf, t)
	if err != nil {
		return config.DeploymentPlan{}, fmt.Errorf("Bodega Paxos leader placement: %w", err)
	}
	leader := *leaderPlan.Leader
	leaderMask := uint64(1) << uint(leader.Rank)
	p := config.DeploymentPlan{
		Model: "sampled-regional-hot-key-ranges", Leader: &leader,
		ClientRoutes: t.NearestRoutes(), Responders: []string{leader.Alias},
	}
	regions := make([]sampledRegion, len(t.Replicas))
	indices := make(map[string]int, len(t.Replicas))
	for i, replica := range t.Replicas {
		indices[replica.Alias] = i
		regions[i] = sampledRegion{Responder: replica.Alias, reads: make(map[int]uint64)}
	}
	for _, client := range profile.File.Clients {
		region := &regions[indices[p.ClientRoutes[client.Client]]]
		region.Clients = append(region.Clients, client.Client)
		region.Requests += client.Requests
		for _, row := range client.Keys {
			if row.Reads > 0 {
				region.reads[row.Key] += row.Reads
				region.Reads += row.Reads
			}
		}
	}
	// Masks use topology indices only while planning. Ranges are emitted using
	// aliases, which the existing runtime resolves to configured voter IDs.
	selected := make(map[int]uint64)
	for i := range regions {
		region := &regions[i]
		region.ObservedReadKeys = len(region.reads)
		if allResponders {
			continue
		}
		keys := make([]int, 0, len(region.reads))
		for key := range region.reads {
			keys = append(keys, key)
		}
		sort.Slice(keys, func(a, b int) bool {
			if region.reads[keys[a]] != region.reads[keys[b]] {
				return region.reads[keys[a]] > region.reads[keys[b]]
			}
			return keys[a] < keys[b]
		})
		budget := (len(keys) + hotKeyDivisor - 1) / hotKeyDivisor
		region.HotKeyBudget = budget
		if len(keys) > budget {
			keys = keys[:budget]
		}
		region.SelectedKeys = len(keys)
		for _, key := range keys {
			region.SelectedReads += region.reads[key]
			selected[key] |= uint64(1)<<uint(i) | leaderMask
		}
	}
	ranges, rangeCount := sampledRanges(selected, t.Replicas, leaderMask)
	p.ResponderRanges = ranges
	branch := "non-uniform"
	if allResponders {
		branch = "uniform-compatible"
		p.Model = "sampled-uniform-all-responders"
		if sampleReadOnly {
			branch = "read-only"
			p.Model = "sampled-read-only-all-responders"
		}
		p.Responders = nil
		for _, replica := range t.Replicas {
			p.Responders = append(p.Responders, replica.Alias)
		}
	}
	if conf.BodegaResponderRanges != "" && conf.BodegaResponderRanges != p.ResponderRanges {
		return p, fmt.Errorf("explicit Bodega responder ranges conflict with sampled responder policy")
	}
	record := map[string]interface{}{
		"policy": p.Model, "profile_sha256": profile.SHA256,
		"sample_source": profile.File.Source, "sample_seed": profile.File.SampleSeed,
		"sample_requests": profile.File.Requests, "observed_keys": len(profile.File.Keys),
		"sample_reads": sampleReads, "sample_writes": sampleWrites, "sample_read_only": sampleReadOnly,
		"leader": leader.Alias, "leader_selection": leaderPlan.Model,
		"leader_selection_mean_ms": leaderPlan.FastMean,
		"branch":                   branch, "uniformity_check": uniformity,
		"uniform_false_rejection_bound": uniformFalseRejection,
		"uniformity_assumption":         "independent identically distributed sampled keys",
		"known_key_count":               conf.KeyCount,
		"hot_key_fraction_basis":        "observed positive-read keys per region, rounded up",
		"hot_key_fraction":              1.0 / hotKeyDivisor,
		"selection":                     "positive sampled reads descending, key ID ascending on ties",
		"region_definition":             "clients grouped by nearest replica in deployment topology",
		"regions":                       regions, "selected_keys_union": len(selected), "range_count": rangeCount,
		"default_responders": p.Responders, "frozen": true,
	}
	if allResponders {
		delete(record, "hot_key_fraction_basis")
		delete(record, "hot_key_fraction")
		record["selection"] = "all replicas for all keys; sample does not reject uniformity"
		if sampleReadOnly {
			record["selection"] = "all replicas for all keys; sample contains reads and no observed writes"
		}
	}
	data, err := json.Marshal(record)
	if err != nil {
		return p, err
	}
	fmt.Fprintln(os.Stderr, "BODEGA_SAMPLE_POLICY", string(data))
	conf.BodegaResponders = strings.Join(p.Responders, ",")
	conf.BodegaResponderRanges = p.ResponderRanges
	return p, nil
}

// sampledRanges never spans a gap or combines different responder sets.
// Leader-only selections need no override of the leader-only default.
func sampledRanges(selected map[int]uint64, replicas []config.PlannedReplica, leaderMask uint64) (string, int) {
	keys := make([]int, 0, len(selected))
	for key, mask := range selected {
		if mask != leaderMask {
			keys = append(keys, key)
		}
	}
	sort.Ints(keys)
	var rules []string
	for i := 0; i < len(keys); {
		start, end, mask := keys[i], keys[i], selected[keys[i]]
		i++
		for i < len(keys) && keys[i] == end+1 && selected[keys[i]] == mask {
			end = keys[i]
			i++
		}
		var names []string
		for j, replica := range replicas {
			if mask&(uint64(1)<<uint(j)) != 0 {
				names = append(names, replica.Alias)
			}
		}
		rules = append(rules, fmt.Sprintf("%d..%d=%s", start, end, strings.Join(names, ",")))
	}
	return strings.Join(rules, ";"), len(rules)
}
