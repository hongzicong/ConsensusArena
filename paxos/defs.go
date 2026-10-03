package paxos

// Protocol messages, identifiers, constants, and value helpers.
import (
	"github.com/hongzicong/ConsensusArena/state"
)

const PageSize = 128

const AdmissionWindow = 8192

type Key struct{ Client, Sequence int32 }

type Request struct {
	ID      Key
	Command state.Command
}

type Record struct {
	Slot, Ballot int64
	Request      Request
	Committed    bool
}

const (
	packetPrepare uint8 = iota
	packetPromise
	packetAccept
	packetAck
	packetCommit
	packetReady
	packetFetch
	packetForward
)

type Packet struct {
	Kind                uint8
	From                int32
	Ballot, Floor, High int64
	Page, Pages         int32
	Records             []Record
	Requests            []Request
}
