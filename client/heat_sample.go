package client

import (
	"fmt"
	"math/rand"
	"sort"

	"github.com/hongzicong/ConsensusArena/config"
	"github.com/hongzicong/ConsensusArena/heatprofile"
)

// SampleHeat generates a separate offered-request stream without networking,
// protocol adapters or consumption of the formal clients' RNG state.
func SampleHeat(cfg *config.Config, requests int, seed int64) (heatprofile.File, error) {
	if cfg.UniqueKeys {
		return heatprofile.File{}, fmt.Errorf("offline heat sampling currently requires reusable keys")
	}
	if requests <= 0 || seed < 0 || seed == int64(cfg.WorkloadSeed) || cfg.Clones < 0 {
		return heatprofile.File{}, fmt.Errorf("positive request count and an independent non-negative sample seed are required")
	}
	aliases := make([]string, 0, len(cfg.ClientAddrs))
	for alias := range cfg.ClientAddrs {
		aliases = append(aliases, alias)
	}
	sort.Strings(aliases)
	if len(aliases) == 0 {
		return heatprofile.File{}, fmt.Errorf("no configured clients for offline sampling")
	}
	cdf := sharedZipfCDF(cfg.KeyCount, cfg.ZipfSkew)
	var mapping []int
	if cfg.ShuffleKeys {
		mapping = sharedKeyMapping(cfg.KeyCount, int64(cfg.KeyMappingSeed))
	}
	counts := make([]heatprofile.KeyCount, cfg.KeyCount)
	participants := len(aliases) * (cfg.Clones + 1)
	for i, alias := range aliases {
		for clone := 0; clone <= cfg.Clones; clone++ {
			ordinal := i*(cfg.Clones+1) + clone
			n := requests / participants
			if ordinal < requests%participants {
				n++
			}
			rng := rand.New(rand.NewSource(DeriveWorkloadSeed(seed, alias, clone)))
			for j := 0; j < n; j++ {
				// Mirror the common offered-request generator's random draw order.
				rng.ExpFloat64()
				rank := sort.SearchFloat64s(cdf, rng.Float64())
				key := rank
				if mapping != nil {
					key = mapping[rank]
				}
				counts[key].Key = key
				write := cfg.Writes >= 100
				if cfg.Writes > 0 && cfg.Writes < 100 {
					write = rng.Intn(100) < cfg.Writes
				}
				if write {
					counts[key].Writes++
				} else {
					counts[key].Reads++
				}
			}
		}
	}
	file := heatprofile.File{Schema: 1, Source: "independent-offline-offered-requests", KeyCount: cfg.KeyCount, Requests: uint64(requests), SampleSeed: seed}
	for _, row := range counts {
		if row.Reads+row.Writes > 0 {
			file.Keys = append(file.Keys, row)
		}
	}
	heatprofile.CanonicalKeys(file.Keys)
	return file, nil
}
