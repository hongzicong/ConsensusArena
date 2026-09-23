package defs

import "sort"

// BodegaCancelRead is a client transport control, never a state-machine command.
const BodegaCancelRead = 254

// BodegaResponderRange overrides the default mask on inclusive [Start, End].
// The leader is implicit; masks contain replica IDs from configuration order.
type BodegaResponderRange struct {
	Start, End int64
	Responders uint64
}

func BodegaRespondersFor(defaultMask uint64, ranges []BodegaResponderRange, leader int, key int64) uint64 {
	i := sort.Search(len(ranges), func(i int) bool { return ranges[i].End >= key })
	if i < len(ranges) && ranges[i].Start <= key {
		defaultMask = ranges[i].Responders
	}
	return defaultMask | uint64(1)<<uint(leader)
}

func ValidBodegaRanges(ranges []BodegaResponderRange, n int) bool {
	for i, r := range ranges {
		if r.Start > r.End || r.Responders>>uint(n) != 0 || (i > 0 && ranges[i-1].End >= r.Start) {
			return false
		}
	}
	return true
}

// BodegaRosterReply is a routing hint, not a lease or a commit certificate.
type BodegaRosterReply struct {
	Ballot     uint64
	Leader     int
	Ready      bool
	Responders uint64
	Ranges     []BodegaResponderRange
}
