package curp

// Protocol messages, identifiers, constants, and value helpers.
import (
	"github.com/hongzicong/ConsensusArena/replica/defs"
	"github.com/hongzicong/ConsensusArena/state"
)

const (
	TRUE  = uint8(1)
	FALSE = uint8(0)
)

type MReply struct {
	Replica int32
	Ballot  int32
	CmdId   defs.RequestID
	Rep     []byte
	Ok      uint8
}

type MAccept struct {
	Replica int32
	Ballot  int32
	Cmd     state.Command
	CmdId   defs.RequestID
	CmdSlot int
}

type MAcceptAck struct {
	Replica int32
	Ballot  int32
	CmdSlot int
}

type MAAcks struct {
	Acks    []MAcceptAck
	Accepts []MAccept
}

type MRecordAck struct {
	Replica int32
	Ballot  int32
	CmdId   defs.RequestID
	Ok      uint8
}

type MCommit struct {
	Replica int32
	Ballot  int32
	CmdSlot int
}

type MSync struct {
	CmdId defs.RequestID
}

type MSyncReply struct {
	Replica int32
	Ballot  int32
	CmdId   defs.RequestID
	Rep     []byte
}

const PageSize = 128

const AdmissionWindow = 8192

type Request struct {
	ID      defs.RequestID
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
