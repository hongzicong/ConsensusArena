package client

import (
	"fmt"
	"math/rand"
	"sort"

	"github.com/hongzicong/ConsensusArena/config"
	"github.com/hongzicong/ConsensusArena/heatprofile"
)

// HeatSampleOptions controls which observations the sampler retains.
// The zero value produces a global-only sample.
type HeatSampleOptions struct {
	PreserveClientOrigins bool
}

// SampleHeat generates a separate offered-request stream without networking,
// protocol adapters or consumption of the formal clients' RNG state.
func SampleHeat(conf *config.Config, requests int, seed int64, options HeatSampleOptions) (heatprofile.File, error) {
	if conf.UniqueKeys {
		return heatprofile.File{}, fmt.Errorf("offline heat sampling currently requires reusable keys")
	}
	if requests <= 0 || seed < 0 || seed == int64(conf.WorkloadSeed) || conf.Clones < 0 {
		return heatprofile.File{}, fmt.Errorf("positive request count and an independent non-negative sample seed are required")
	}
	aliases := make([]string, 0, len(conf.ClientAddrs))
	for alias := range conf.ClientAddrs {
		aliases = append(aliases, alias)
	}
	sort.Strings(aliases)
	if len(aliases) == 0 {
		return heatprofile.File{}, fmt.Errorf("no configured clients for offline sampling")
	}
	cdf := sharedZipfCDF(conf.KeyCount, conf.ZipfSkew)
	var mapping []int
	if conf.ShuffleKeys {
		mapping = sharedKeyMapping(conf.KeyCount, int64(conf.KeyMappingSeed))
	}
	counts := make([]heatprofile.KeyCount, conf.KeyCount)
	// Retaining client origins leaves the aggregate sample unchanged.
	regional := options.PreserveClientOrigins
	var clients []heatprofile.ClientCounts
	participants := len(aliases) * (conf.Clones + 1)
	for i, alias := range aliases {
		observed := heatprofile.ClientCounts{Client: alias}
		var local map[int]heatprofile.KeyCount
		if regional {
			local = make(map[int]heatprofile.KeyCount)
		}
		for clone := 0; clone <= conf.Clones; clone++ {
			ordinal := i*(conf.Clones+1) + clone
			n := requests / participants
			if ordinal < requests%participants {
				n++
			}
			observed.Requests += uint64(n)
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
				write := conf.Writes >= 100
				if conf.Writes > 0 && conf.Writes < 100 {
					write = rng.Intn(100) < conf.Writes
				}
				if write {
					counts[key].Writes++
				} else {
					counts[key].Reads++
				}
				if regional {
					row := local[key]
					row.Key = key
					if write {
						row.Writes++
					} else {
						row.Reads++
					}
					local[key] = row
				}
			}
		}
		if regional {
			for _, row := range local {
				observed.Keys = append(observed.Keys, row)
			}
			heatprofile.CanonicalKeys(observed.Keys)
			clients = append(clients, observed)
		}
	}
	file := heatprofile.File{Schema: 1, Source: "independent-offline-offered-requests", KeyCount: conf.KeyCount, Requests: uint64(requests), SampleSeed: seed}
	for _, row := range counts {
		if row.Reads+row.Writes > 0 {
			file.Keys = append(file.Keys, row)
		}
	}
	heatprofile.CanonicalKeys(file.Keys)
	if regional {
		file.Schema = 2
		file.Clients = clients
	}
	return file, nil
}
