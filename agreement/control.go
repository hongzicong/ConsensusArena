package agreement

import "sort"

// These routing hints and range rules come from the supplied XPaxos checkout.
// They belong to Agreement; baseline Bodega keeps its independent definitions.
const cancelRead = 254

func respondersFor(defaultMask uint64, ranges []ResponderRange, leader int, key int64) uint64 {
	i := sort.Search(len(ranges), func(i int) bool { return ranges[i].End >= key })
	if i < len(ranges) && ranges[i].Start <= key {
		defaultMask = ranges[i].Responders
	}
	return defaultMask | uint64(1)<<uint(leader)
}

func validRanges(ranges []ResponderRange, n int) bool {
	for i, r := range ranges {
		if r.Start > r.End || r.Responders>>uint(n) != 0 || (i > 0 && ranges[i-1].End >= r.Start) {
			return false
		}
	}
	return true
}

// RosterReply is a routing hint, never a lease or a commit certificate.
type RosterReply struct {
	ColdMigrated                      bool
	ColdBallot, ColdEpoch, ColdQuorum uint64
	ColdReady                         bool
	Ballot                            uint64
	Leader                            int
	Ready                             bool
	Responders                        uint64
	Ranges                            []ResponderRange
}
