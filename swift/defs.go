package swift

// Protocol messages, identifiers, constants, and value helpers.
import (
	"fmt"

	"github.com/hongzicong/ConsensusArena/state"
)

// status
const (
	NORMAL = iota
	RECOVERING
)

// phase
const (
	START = iota
	PRE_ACCEPT
	ACCEPT
	COMMIT
)

const HISTORY_SIZE = 10010001

type CommandId struct {
	ClientId int32
	SeqNum   int32
}

type Dep []CommandId

//////////////////////////////////////////////////////////////
//                                                          //
//  gobin-codegen doesn't support declarations of the form  //
//                                                          //
//      type A B                                            //
//                                                          //
//  that's why we use `[]CommandId` instead of `Dep`        //
//                                                          //
//////////////////////////////////////////////////////////////

type MFastAck struct {
	Replica  int32
	Ballot   int32
	CmdId    CommandId
	Dep      []CommandId
	Checksum []SHash
	Seqnum   int
}

type MFastAckClient struct {
	Replica  int32
	Ballot   int32
	CmdId    CommandId
	Checksum []SHash
}

type MSlowAck struct {
	Replica  int32
	Ballot   int32
	CmdId    CommandId
	Dep      []CommandId
	Checksum []SHash
}

type MLightSlowAck struct {
	Replica int32
	Ballot  int32
	CmdId   CommandId
}

type MAcks struct {
	FastAcks      []MFastAck
	LightSlowAcks []MLightSlowAck
}

type Ack struct {
	CmdId    CommandId
	Dep      []CommandId
	Checksum []SHash
	Seqnum   int
}

type MOptAcks struct {
	Replica int32
	Ballot  int32
	Acks    []Ack
}

type MReply struct {
	Replica  int32
	Ballot   int32
	CmdId    CommandId
	Checksum []SHash
	Rep      []byte
}

type MAccept struct {
	Replica int32
	Ballot  int32
	CmdId   CommandId
	Rep     []byte
}

type MNewLeader struct {
	Replica int32
	Ballot  int32
}

type MNewLeaderAck struct {
	Replica int32
	Ballot  int32
	Cballot int32
}

type SDep struct {
	Dep []CommandId
}

type MNewLeaderAckN struct {
	Replica int32
	Ballot  int32
	Cballot int32
	CmdIds  []CommandId
	Phases  []int
	Cmds    []state.Command
	Deps    []SDep
}

type MShareState struct {
	Replica int32
	Ballot  int32
}

type MSync struct {
	Replica int32
	Ballot  int32
	Phases  map[CommandId]int
	Cmds    map[CommandId]state.Command
	Deps    map[CommandId]Dep
}

type MLightSync struct {
	Replica int32
	Ballot  int32
}

type MCollect struct {
	Replica int32
	Ballot  int32
	Ids     []CommandId
}

type MPing struct {
	Replica int32
	Ballot  int32
}

type MPingRep struct {
	Replica int32
	Ballot  int32
}

func (cmdId CommandId) String() string {
	return fmt.Sprintf("%v,%v", cmdId.ClientId, cmdId.SeqNum)
}

func (d Dep) Contains(cmdId CommandId) bool {
	for _, c := range d {
		if c == cmdId {
			return true
		}
	}
	return false
}

func NilDepOfCmdId(cmdId CommandId) Dep {
	return []CommandId{cmdId}
}

func IsNilDepOfCmdId(cmdId CommandId, dep Dep) bool {
	return len(dep) == 1 && dep[0] == cmdId
}

func (dep1 Dep) Equals(dep2 Dep) bool {
	if len(dep1) != len(dep2) {
		return false
	}

	seen1 := make(map[CommandId]struct{})
	seen2 := make(map[CommandId]struct{})
	for i := 0; i < len(dep1); i++ {
		if dep1[i] == dep2[i] {
			continue
		}

		_, exists := seen2[dep1[i]]
		if exists {
			delete(seen2, dep1[i])
		} else {
			seen1[dep1[i]] = struct{}{}
		}

		_, exists = seen1[dep2[i]]
		if exists {
			delete(seen1, dep2[i])
		} else {
			seen2[dep2[i]] = struct{}{}
		}
	}

	return len(seen1) == len(seen2) && len(seen1) == 0
}

type SHash struct {
	H [32]byte
}
