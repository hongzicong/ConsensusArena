// Package bodega implements a crash-stop Bodega prototype with roster leases.
package bodega

// Protocol messages, identifiers, constants, and value helpers.
import (
	"slices"
	"sort"

	"github.com/hongzicong/ConsensusArena/replica/defs"
	"github.com/hongzicong/ConsensusArena/state"
)

// BodegaCancelRead is a client transport control, never a state-machine command.
const BodegaCancelRead = 254

// BodegaResponderRange overrides the default mask on inclusive [Start, End].
// The leader is implicit; masks contain replica IDs from configuration order.
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

type kind uint8

const (
	heartbeat kind = iota
	leaseRequest
	leaseReply
	prepare
	promise
	accept
	accepted
	commit
	forward
	result
	repairRequest
	leaseRevoke
	leaseRevoked
	acceptNote
	committedEntry
	lightHeartbeat
	rosterRequest
	kindCount
)

func sameRoster(a, b roster) bool {
	return a.Ballot == b.Ballot && a.Leader == b.Leader && a.Responders == b.Responders && slices.Equal(a.Ranges, b.Ranges)
}

func (r roster) respondersFor(key state.Key) uint64 {
	return BodegaRespondersFor(r.Responders, r.Ranges, r.Leader, int64(key))
}

func (r roster) allResponders() uint64 {
	mask := r.Responders | bit(r.Leader)
	for _, span := range r.Ranges {
		mask |= span.Responders
	}
	return mask
}

func (r request) id() defs.RequestID {
	return r.Proposal.RequestID()
}

func maxSlot(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}

func minSlot(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}
