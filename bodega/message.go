// Package bodega implements a crash-stop Bodega prototype with roster leases.
package bodega

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
)

type roster struct {
	Ballot     uint64
	Leader     int
	Responders uint64
	Ranges     []defs.BodegaResponderRange
}

func sameRoster(a, b roster) bool {
	return a.Ballot == b.Ballot && a.Leader == b.Leader && a.Responders == b.Responders && slices.Equal(a.Ranges, b.Ranges)
}

func (r roster) respondersFor(key state.Key) uint64 {
	return defs.BodegaRespondersFor(r.Responders, r.Ranges, r.Leader, int64(key))
}

func (r roster) allResponders() uint64 {
	mask := r.Responders | bit(r.Leader)
	for _, span := range r.Ranges {
		mask |= span.Responders
	}
	return mask
}

type requestID struct{ Client, Command int32 }
type request struct {
	Proposal defs.Propose
	Origin   int
}

func (r request) id() requestID { return requestID{r.Proposal.ClientId, r.Proposal.CommandId} }

type entry struct {
	Slot, Ballot uint64
	Request      request
	Batch        []request // nonempty only for multi-command entries
	ReadFresh    bool      // first assignment in this ballot, never a recovered entry
}
type message struct {
	Kind                        kind
	From                        int
	Roster                      roster
	Sequence, Threshold, Prefix uint64
	ReadPrefix                  uint64 // leader-certified majority accepted prefix
	CommitPrefix, CommitSlot    uint64 // inclusive prefix and optional out-of-order slot
	RepairStart                 uint64
	Part, Parts                 int
	Entry                       entry
	Entries                     []entry
	Request                     request
	Value                       state.Value
}
