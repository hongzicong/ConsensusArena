package curp

// Protocol messages, identifiers, constants, and value helpers.
import (
	"fmt"

	"github.com/hongzicong/ConsensusArena/state"
)

const (
	TRUE  = uint8(1)
	FALSE = uint8(0)
)

type CommandId struct {
	ClientId int32
	SeqNum   int32
}

func (cmdId CommandId) String() string {
	return fmt.Sprintf("%v,%v", cmdId.ClientId, cmdId.SeqNum)
}

type MReply struct {
	Replica int32
	Ballot  int32
	CmdId   CommandId
	Rep     []byte
	Ok      uint8
}

type MAccept struct {
	Replica int32
	Ballot  int32
	Cmd     state.Command
	CmdId   CommandId
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
	CmdId   CommandId
	Ok      uint8
}

type MCommit struct {
	Replica int32
	Ballot  int32
	CmdSlot int
}

type MSync struct {
	CmdId CommandId
}

type MSyncReply struct {
	Replica int32
	Ballot  int32
	CmdId   CommandId
	Rep     []byte
}

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
