package bodega

import (
	"fmt"
	"slices"
	"sort"
	"strconv"
	"strings"

	"github.com/hongzicong/ConsensusArena/config"
	"github.com/hongzicong/ConsensusArena/replica/defs"
)

func responderMask(text string, c *config.Config, n int) (uint64, error) {
	text = strings.ToLower(strings.TrimSpace(text))
	if text == "all" {
		return (uint64(1) << uint(n)) - 1, nil
	}
	if text == "leader" {
		return 0, nil
	}
	var mask uint64
	for _, name := range strings.Split(text, ",") {
		id := slices.Index(c.ReplicaAliases, strings.TrimSpace(name))
		if id < 0 || id >= n {
			return 0, fmt.Errorf("Bodega unknown responder alias %q", name)
		}
		mask |= bit(id)
	}
	return mask, nil
}

func responderRanges(text string, c *config.Config, n int) ([]defs.BodegaResponderRange, error) {
	if strings.TrimSpace(text) == "" {
		return nil, nil
	}
	var ranges []defs.BodegaResponderRange
	for _, rule := range strings.Split(text, ";") {
		span, names, ok := strings.Cut(rule, "=")
		if !ok {
			return nil, fmt.Errorf("Bodega range must be start..end=responders or key=responders: %q", rule)
		}
		lo, hi, interval := strings.Cut(span, "..")
		if !interval {
			hi = lo
		}
		start, err := strconv.ParseInt(strings.TrimSpace(lo), 10, 64)
		if err != nil {
			return nil, fmt.Errorf("Bodega range start: %w", err)
		}
		end, err := strconv.ParseInt(strings.TrimSpace(hi), 10, 64)
		if err != nil {
			return nil, fmt.Errorf("Bodega range end: %w", err)
		}
		mask, err := responderMask(names, c, n)
		if err != nil {
			return nil, err
		}
		ranges = append(ranges, defs.BodegaResponderRange{Start: start, End: end, Responders: mask})
	}
	sort.Slice(ranges, func(i, j int) bool { return ranges[i].Start < ranges[j].Start })
	if !defs.ValidBodegaRanges(ranges, n) {
		return nil, fmt.Errorf("Bodega ranges must be nonoverlapping inclusive intervals with start <= end")
	}
	return ranges, nil
}
