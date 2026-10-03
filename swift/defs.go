package swift

// Protocol messages, identifiers, constants, and value helpers.
import (
	"github.com/hongzicong/ConsensusArena/replica/defs"
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

type Dep []defs.RequestID

type MFastAck struct {
	Replica  int32
	Ballot   int32
	CmdId    defs.RequestID
	Dep      []defs.RequestID
	Checksum []SHash
	Seqnum   int
}

type MFastAckClient struct {
	Replica  int32
	Ballot   int32
	CmdId    defs.RequestID
	Checksum []SHash
}

type MSlowAck struct {
	Replica  int32
	Ballot   int32
	CmdId    defs.RequestID
	Dep      []defs.RequestID
	Checksum []SHash
}

type MLightSlowAck struct {
	Replica int32
	Ballot  int32
	CmdId   defs.RequestID
}

type MAcks struct {
	FastAcks      []MFastAck
	LightSlowAcks []MLightSlowAck
}

type Ack struct {
	CmdId    defs.RequestID
	Dep      []defs.RequestID
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
	CmdId    defs.RequestID
	Checksum []SHash
	Rep      []byte
}

type MAccept struct {
	Replica int32
	Ballot  int32
	CmdId   defs.RequestID
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
	Dep []defs.RequestID
}

type MNewLeaderAckN struct {
	Replica int32
	Ballot  int32
	Cballot int32
	CmdIds  []defs.RequestID
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
	Phases  map[defs.RequestID]int
	Cmds    map[defs.RequestID]state.Command
	Deps    map[defs.RequestID]Dep
}

type MLightSync struct {
	Replica int32
	Ballot  int32
}

type MCollect struct {
	Replica int32
	Ballot  int32
	Ids     []defs.RequestID
}

type MPing struct {
	Replica int32
	Ballot  int32
}

type MPingRep struct {
	Replica int32
	Ballot  int32
}

func (d Dep) Contains(cmdId defs.RequestID) bool {
	for _, c := range d {
		if c == cmdId {
			return true
		}
	}
	return false
}

func NilDepOfCmdId(cmdId defs.RequestID) Dep {
	return []defs.RequestID{cmdId}
}

func IsNilDepOfCmdId(cmdId defs.RequestID, dep Dep) bool {
	return len(dep) == 1 && dep[0] == cmdId
}

func (dep1 Dep) Equals(dep2 Dep) bool {
	if len(dep1) != len(dep2) {
		return false
	}

	seen1 := make(map[defs.RequestID]struct{})
	seen2 := make(map[defs.RequestID]struct{})
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
