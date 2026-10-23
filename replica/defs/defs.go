package defs

import (
	"bufio"
	"sync"
)

// master RPC definitions

type RegisterArgs struct {
	Alias string
	Addr  string
	Port  int
}

type RegisterReply struct {
	ReplicaId int
	NodeList  []string
	Ready     bool
	IsLeader  bool
}

type GetLeaderArgs struct{}

type GetLeaderReply struct {
	LeaderId int
}

type GetReplicaListArgs struct{}

type GetReplicaListReply struct {
	ReplicaList []string
	AliveList   []bool
	Ready       bool
}

// replica and client definitions

const (
	PROPOSE uint8 = iota
	PROPOSE_REPLY
	READ
	READ_REPLY
	PROPOSE_AND_READ
	PROPOSE_AND_READ_REPLY
	GENERIC_SMR_BEACON
	GENERIC_SMR_BEACON_REPLY
	STATS
	RPC_TABLE
)

type GPropose struct {
	*Propose
	Reply *bufio.Writer
	Mutex *sync.Mutex
	Proxy bool
	Addr  string
}

type GBeacon struct {
	Rid       int32
	Timestamp int64
}

const (
	CHAN_BUFFER_SIZE = 2000000
	TRUE             = uint8(1)
	FALSE            = uint8(0)
)

var (
	Storage       = ""
	StoreFilename = "stable_store"
)

func NewBeTheLeaderReply() *BeTheLeaderReply {
	return &BeTheLeaderReply{
		Leader:     -1,
		NextLeader: -1,
	}
}

func UpdateBeTheLeaderReply(btlr *BeTheLeaderReply) {
	if btlr.Leader == -2 {
		btlr.Leader = 0
	}
	if btlr.NextLeader == -2 {
		btlr.NextLeader = 0
	}
}

func (m *BeTheLeaderReply) IsDefault() bool {
	return m.Leader == -1 && m.NextLeader == -1
}

type Stats struct {
	M map[string]int `json:"stats"`
}
