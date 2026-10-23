package kcensus

// Protocol messages, identifiers, constants, and value helpers.
import (
	"bytes"

	"github.com/hongzicong/ConsensusArena/state"
)

const (
	spread uint8 = iota + 1
	abandon
	prepare
	promise
	accept
	accepted
	commit
	nack
	fetch
	readQuery
	readReply
	heartbeat
	resumeCensus // local replay only: wake selection after payload resolution
	reservedGraphWitness
	offer // payload only: never a fast acceptance or a slot proposal
	payloadQuery
	payloadReply
	delegateRead
	executedResult
	resultQuery
)

const maxBatch = 4096

const maxBatchBytes = 4 << 20

func sameValue(a, b *Value) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return a.UID == b.UID
}

func sameRecord(x, y Record) bool {
	return x.ID == y.ID && x.Command.Op == y.Command.Op && x.Command.K == y.Command.K && bytes.Equal(x.Command.V, y.Command.V)
}

// Original voter identity survives relaying. Ballot zero denotes an unfrozen
// state. Classic acceptance replaces the node's fast proposer and knowledge.
type envelope struct {
	To      int
	Count   int // positive: broadcast to process IDs [0, Count), including self
	Message message
	// Destinations of one broadcast share its immutable wire image. This is
	// transient transport metadata, never protocol state or received evidence.
	Encoded *[]byte
}

var commandPolicy = state.CommandPolicy{Operations: []state.Operation{state.PUT, state.GET}}
