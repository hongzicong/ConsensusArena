// Package agreement implements Agreement, formerly named XPaxos.
package agreement

// Protocol messages, identifiers, constants, and value helpers.
import (
	"slices"

	"github.com/hongzicong/ConsensusArena/replica/defs"
	"github.com/hongzicong/ConsensusArena/state"
)

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
	coldControl
	kindCount
)

func sameRoster(a, b roster) bool {
	return a.Ballot == b.Ballot && a.Leader == b.Leader && a.Responders == b.Responders && slices.Equal(a.Ranges, b.Ranges)
}

func (r roster) respondersFor(key state.Key) uint64 {
	return respondersFor(r.Responders, r.Ranges, r.Leader, int64(key))
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

// Protocol-local client envelope, normalized before consensus.
const directPUT = state.Operation(252)
