// Package heatpolicy defines immutable, sample-derived rank boundaries.
package heatpolicy

import (
	"sort"

	"github.com/hongzicong/ConsensusArena/config"
)

// HotCutoff is the number of observed keys with more requests than the mean
// observed key. Integer division makes the strict comparison exact, including
// ties, without multiplication overflow. No raw key value defines hotness.
// A uniform sample has no hot keys. Unobserved keys retain the ordinary path.
func HotCutoff(c *config.Config) int {
	p := c.HeatProfile
	if p == nil || len(p.File.Keys) == 0 {
		panic("validated heat profile required")
	}
	mean := p.File.Requests / uint64(len(p.File.Keys))
	return sort.Search(len(p.File.Keys), func(i int) bool {
		k := p.File.Keys[i]
		return k.Reads+k.Writes <= mean
	})
}

// ColdCuts preserves the planner's bounded seven-candidate search, but derives
// its boundaries from cumulative measured cold traffic instead of key numbers.
// Equal-frequency keys are never split across a candidate boundary.
func ColdCuts(c *config.Config) []int {
	keys := c.HeatProfile.File.Keys
	start := HotCutoff(c)
	cuts := []int{start}
	var total uint64
	for _, k := range keys[start:] {
		total += k.Reads + k.Writes
	}
	var seen uint64
	next := 1
	for i := start; i < len(keys); i++ {
		seen += keys[i].Reads + keys[i].Writes
		if i+1 == len(keys) || keys[i].Reads+keys[i].Writes == keys[i+1].Reads+keys[i+1].Writes {
			continue
		}
		// Seven evenly spaced mass targets, including the hot boundary.
		if next < 7 && float64(seen)/float64(total) >= float64(next)/7 {
			cuts = append(cuts, i+1)
			for next < 7 && float64(seen)/float64(total) >= float64(next)/7 {
				next++
			}
		}
	}
	return cuts
}
