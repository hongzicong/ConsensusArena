package kcensus

// Protocol messages, identifiers, constants, and value helpers.
import (
	"bytes"
	"net"

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

type CommandID struct{ Client, Seq int32 }

type Record struct {
	ID      CommandID
	Command state.Command
}

type Value struct {
	UID       uint64
	Proposer  int      // round graph context, independent of immutable UID origin
	Records   []Record // Single body or locally resolved Batch execution cache
	Batch     *Batch
	Reference bool // UID only; resolve before accepting, deciding or executing
}

type Batch struct {
	Slot    uint64
	Members []uint64
}

func sameValue(a, b *Value) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return a.UID == b.UID
}

func sameRecord(x, y Record) bool {
	return x.ID == y.ID && x.Command.Op == y.Command.Op && x.Command.K == y.Command.K && bytes.Equal(x.Command.V, y.Command.V)
}

type message struct {
	Kind                               uint8
	From                               int
	Proposer                           int // fast value identity within this key/slot
	Key                                state.Key
	Slot, Ballot, AcceptedBallot, High uint64
	ID                                 CommandID
	Value, Fast                        *Value
	Knowledge                          []uint64
	Mask                               uint64 // Abandon: proposer set; Promise: frozen first-order evidence
	Digest                             [32]byte
	GraphTime                          int64
	Explicit                           bool
	NewValue, References               bool
	StateTime                          int64
	Reports                            []nodeReport
	UIDs                               []uint64
	Values                             []*Value
	Request                            *Record
	Result                             state.Value
	// Local transport context, never serialized.
	ClientConn     net.Conn
	ClientIdentity string
}

// Original voter identity survives relaying. Ballot zero denotes an unfrozen
// state. Classic acceptance replaces the node's fast proposer and knowledge.
type nodeReport struct {
	From, Proposer               int // Proposer=-1 means no fast acceptance
	Ballot, AcceptedBallot, Mask uint64
	Time                         int64
	Value                        *Value
}

type envelope struct {
	To      int
	Message message
}
