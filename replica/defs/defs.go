package defs

import (
	"bufio"
	"io"
	"sync"

	"github.com/hongzicong/ConsensusArena/state"
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

type Propose struct {
	CommandId int32
	ClientId  int32
	Command   state.Command
	Timestamp int64
}

type ProposeReply struct {
	OK        uint8
	CommandId int32
}

type ProposeReplyTS struct {
	OK        uint8
	CommandId int32
	Value     state.Value
	Timestamp int64
}

type Read struct {
	CommandId int32
	Key       state.Key
}

type ReadReply struct {
	CommandId int32
	Value     state.Value
}

type ProposeAndRead struct {
	CommandId int32
	Command   state.Command
	Key       state.Key
}

type ProposeAndReadReply struct {
	OK        uint8
	CommandId int32
	Value     state.Value
}

type Beacon struct {
	Timestamp int64
}

type BeaconReply struct {
	Timestamp int64
}

type PingArgs struct {
	ActAsLeader uint8
}

type PingReply struct{}

type BeTheLeaderArgs struct{}

type BeTheLeaderReply struct {
	Leader     int32
	NextLeader int32
}

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
	Storage      = ""
	StoreFilname = "stable_store"
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

func (r *BeTheLeaderReply) IsDefault() bool {
	return r.Leader == -1 && r.NextLeader == -1
}

type Stats struct {
	M map[string]int `json:"stats"`
}

///////////////////////////////////////////////////////////////////////////////
//                                                                           //
//  Generated with gobin-codegen [https://code.google.com/p/gobin-codegen/]  //
//                                                                           //
///////////////////////////////////////////////////////////////////////////////

func (t *PingArgs) BinarySize() (nbytes int, sizeKnown bool) {
	return 1, true
}

func (t *PingArgs) Marshal(wire io.Writer) {
	var b [1]byte
	var bs []byte
	bs = b[:1]
	bs[0] = byte(t.ActAsLeader)
	wire.Write(bs)
}

func (t *PingArgs) Unmarshal(wire io.Reader) error {
	var b [1]byte
	var bs []byte
	bs = b[:1]
	if _, err := io.ReadAtLeast(wire, bs, 1); err != nil {
		return err
	}
	t.ActAsLeader = uint8(bs[0])
	return nil
}

func (t *PingReply) BinarySize() (nbytes int, sizeKnown bool) {
	return 0, true
}

func (t *PingReply) Marshal(wire io.Writer) {
}

func (t *PingReply) Unmarshal(wire io.Reader) error {
	return nil
}

func (t *BeTheLeaderReply) BinarySize() (nbytes int, sizeKnown bool) {
	return 8, true
}

func (t *BeTheLeaderReply) Marshal(wire io.Writer) {
	var b [8]byte
	var bs []byte
	bs = b[:8]
	tmp32 := t.Leader
	bs[0] = byte(tmp32)
	bs[1] = byte(tmp32 >> 8)
	bs[2] = byte(tmp32 >> 16)
	bs[3] = byte(tmp32 >> 24)
	tmp32 = t.NextLeader
	bs[4] = byte(tmp32)
	bs[5] = byte(tmp32 >> 8)
	bs[6] = byte(tmp32 >> 16)
	bs[7] = byte(tmp32 >> 24)
	wire.Write(bs)
}

func (t *BeTheLeaderReply) Unmarshal(wire io.Reader) error {
	var b [8]byte
	var bs []byte
	bs = b[:8]
	if _, err := io.ReadAtLeast(wire, bs, 8); err != nil {
		return err
	}
	t.Leader = int32((uint32(bs[0]) | (uint32(bs[1]) << 8) | (uint32(bs[2]) << 16) | (uint32(bs[3]) << 24)))
	t.NextLeader = int32((uint32(bs[4]) | (uint32(bs[5]) << 8) | (uint32(bs[6]) << 16) | (uint32(bs[7]) << 24)))
	return nil
}

func (t *Propose) BinarySize() (nbytes int, sizeKnown bool) {
	return 0, false
}

func (t *Propose) Marshal(wire io.Writer) {
	var b [8]byte
	var bs []byte
	bs = b[:8]
	tmp32 := t.CommandId
	bs[0] = byte(tmp32)
	bs[1] = byte(tmp32 >> 8)
	bs[2] = byte(tmp32 >> 16)
	bs[3] = byte(tmp32 >> 24)
	tmp32 = t.ClientId
	bs[4] = byte(tmp32)
	bs[5] = byte(tmp32 >> 8)
	bs[6] = byte(tmp32 >> 16)
	bs[7] = byte(tmp32 >> 24)
	wire.Write(bs)
	t.Command.Marshal(wire)
	tmp64 := t.Timestamp
	bs[0] = byte(tmp64)
	bs[1] = byte(tmp64 >> 8)
	bs[2] = byte(tmp64 >> 16)
	bs[3] = byte(tmp64 >> 24)
	bs[4] = byte(tmp64 >> 32)
	bs[5] = byte(tmp64 >> 40)
	bs[6] = byte(tmp64 >> 48)
	bs[7] = byte(tmp64 >> 56)
	wire.Write(bs)
}

func (t *Propose) Unmarshal(wire io.Reader) error {
	var b [8]byte
	var bs []byte
	bs = b[:8]
	if _, err := io.ReadAtLeast(wire, bs, 8); err != nil {
		return err
	}
	t.CommandId = int32((uint32(bs[0]) | (uint32(bs[1]) << 8) | (uint32(bs[2]) << 16) | (uint32(bs[3]) << 24)))
	t.ClientId = int32((uint32(bs[4]) | (uint32(bs[5]) << 8) | (uint32(bs[6]) << 16) | (uint32(bs[7]) << 24)))
	t.Command.Unmarshal(wire)
	if _, err := io.ReadAtLeast(wire, bs, 8); err != nil {
		return err
	}
	t.Timestamp = int64((uint64(bs[0]) | (uint64(bs[1]) << 8) | (uint64(bs[2]) << 16) | (uint64(bs[3]) << 24) | (uint64(bs[4]) << 32) | (uint64(bs[5]) << 40) | (uint64(bs[6]) << 48) | (uint64(bs[7]) << 56)))
	return nil
}

func (t *ProposeReplyTS) BinarySize() (nbytes int, sizeKnown bool) {
	return 0, false
}

func (t *ProposeReplyTS) Marshal(wire io.Writer) {
	var b [8]byte
	var bs []byte
	bs = b[:5]
	bs[0] = byte(t.OK)
	tmp32 := t.CommandId
	bs[1] = byte(tmp32)
	bs[2] = byte(tmp32 >> 8)
	bs[3] = byte(tmp32 >> 16)
	bs[4] = byte(tmp32 >> 24)
	wire.Write(bs)
	t.Value.Marshal(wire)
	bs = b[:8]
	tmp64 := t.Timestamp
	bs[0] = byte(tmp64)
	bs[1] = byte(tmp64 >> 8)
	bs[2] = byte(tmp64 >> 16)
	bs[3] = byte(tmp64 >> 24)
	bs[4] = byte(tmp64 >> 32)
	bs[5] = byte(tmp64 >> 40)
	bs[6] = byte(tmp64 >> 48)
	bs[7] = byte(tmp64 >> 56)
	wire.Write(bs)
}

func (t *ProposeReplyTS) Unmarshal(wire io.Reader) error {
	var b [8]byte
	var bs []byte
	bs = b[:5]
	if _, err := io.ReadAtLeast(wire, bs, 5); err != nil {
		return err
	}
	t.OK = uint8(bs[0])
	t.CommandId = int32((uint32(bs[1]) | (uint32(bs[2]) << 8) | (uint32(bs[3]) << 16) | (uint32(bs[4]) << 24)))
	t.Value.Unmarshal(wire)
	bs = b[:8]
	if _, err := io.ReadAtLeast(wire, bs, 8); err != nil {
		return err
	}
	t.Timestamp = int64((uint64(bs[0]) | (uint64(bs[1]) << 8) | (uint64(bs[2]) << 16) | (uint64(bs[3]) << 24) | (uint64(bs[4]) << 32) | (uint64(bs[5]) << 40) | (uint64(bs[6]) << 48) | (uint64(bs[7]) << 56)))
	return nil
}

func (t *Read) BinarySize() (nbytes int, sizeKnown bool) {
	return 0, false
}

func (t *Read) Marshal(wire io.Writer) {
	var b [4]byte
	var bs []byte
	bs = b[:4]
	tmp32 := t.CommandId
	bs[0] = byte(tmp32)
	bs[1] = byte(tmp32 >> 8)
	bs[2] = byte(tmp32 >> 16)
	bs[3] = byte(tmp32 >> 24)
	wire.Write(bs)
	t.Key.Marshal(wire)
}

func (t *Read) Unmarshal(wire io.Reader) error {
	var b [4]byte
	var bs []byte
	bs = b[:4]
	if _, err := io.ReadAtLeast(wire, bs, 4); err != nil {
		return err
	}
	t.CommandId = int32((uint32(bs[0]) | (uint32(bs[1]) << 8) | (uint32(bs[2]) << 16) | (uint32(bs[3]) << 24)))
	t.Key.Unmarshal(wire)
	return nil
}

func (t *ProposeAndRead) BinarySize() (nbytes int, sizeKnown bool) {
	return 0, false
}

func (t *ProposeAndRead) Marshal(wire io.Writer) {
	var b [4]byte
	var bs []byte
	bs = b[:4]
	tmp32 := t.CommandId
	bs[0] = byte(tmp32)
	bs[1] = byte(tmp32 >> 8)
	bs[2] = byte(tmp32 >> 16)
	bs[3] = byte(tmp32 >> 24)
	wire.Write(bs)
	t.Command.Marshal(wire)
	t.Key.Marshal(wire)
}

func (t *ProposeAndRead) Unmarshal(wire io.Reader) error {
	var b [4]byte
	var bs []byte
	bs = b[:4]
	if _, err := io.ReadAtLeast(wire, bs, 4); err != nil {
		return err
	}
	t.CommandId = int32((uint32(bs[0]) | (uint32(bs[1]) << 8) | (uint32(bs[2]) << 16) | (uint32(bs[3]) << 24)))
	t.Command.Unmarshal(wire)
	t.Key.Unmarshal(wire)
	return nil
}

func (t *BeaconReply) BinarySize() (nbytes int, sizeKnown bool) {
	return 8, true
}

func (t *BeaconReply) Marshal(wire io.Writer) {
	var b [8]byte
	var bs []byte
	bs = b[:8]
	tmp64 := t.Timestamp
	bs[0] = byte(tmp64)
	bs[1] = byte(tmp64 >> 8)
	bs[2] = byte(tmp64 >> 16)
	bs[3] = byte(tmp64 >> 24)
	bs[4] = byte(tmp64 >> 32)
	bs[5] = byte(tmp64 >> 40)
	bs[6] = byte(tmp64 >> 48)
	bs[7] = byte(tmp64 >> 56)
	wire.Write(bs)
}

func (t *BeaconReply) Unmarshal(wire io.Reader) error {
	var b [8]byte
	var bs []byte
	bs = b[:8]
	if _, err := io.ReadAtLeast(wire, bs, 8); err != nil {
		return err
	}
	t.Timestamp = int64((uint64(bs[0]) | (uint64(bs[1]) << 8) | (uint64(bs[2]) << 16) | (uint64(bs[3]) << 24) | (uint64(bs[4]) << 32) | (uint64(bs[5]) << 40) | (uint64(bs[6]) << 48) | (uint64(bs[7]) << 56)))
	return nil
}

func (t *BeTheLeaderArgs) BinarySize() (nbytes int, sizeKnown bool) {
	return 0, true
}

func (t *BeTheLeaderArgs) Marshal(wire io.Writer) {
}

func (t *BeTheLeaderArgs) Unmarshal(wire io.Reader) error {
	return nil
}

func (t *ProposeReply) BinarySize() (nbytes int, sizeKnown bool) {
	return 5, true
}

func (t *ProposeReply) Marshal(wire io.Writer) {
	var b [5]byte
	var bs []byte
	bs = b[:5]
	bs[0] = byte(t.OK)
	tmp32 := t.CommandId
	bs[1] = byte(tmp32)
	bs[2] = byte(tmp32 >> 8)
	bs[3] = byte(tmp32 >> 16)
	bs[4] = byte(tmp32 >> 24)
	wire.Write(bs)
}

func (t *ProposeReply) Unmarshal(wire io.Reader) error {
	var b [5]byte
	var bs []byte
	bs = b[:5]
	if _, err := io.ReadAtLeast(wire, bs, 5); err != nil {
		return err
	}
	t.OK = uint8(bs[0])
	t.CommandId = int32((uint32(bs[1]) | (uint32(bs[2]) << 8) | (uint32(bs[3]) << 16) | (uint32(bs[4]) << 24)))
	return nil
}

func (t *ReadReply) BinarySize() (nbytes int, sizeKnown bool) {
	return 0, false
}

func (t *ReadReply) Marshal(wire io.Writer) {
	var b [4]byte
	var bs []byte
	bs = b[:4]
	tmp32 := t.CommandId
	bs[0] = byte(tmp32)
	bs[1] = byte(tmp32 >> 8)
	bs[2] = byte(tmp32 >> 16)
	bs[3] = byte(tmp32 >> 24)
	wire.Write(bs)
	t.Value.Marshal(wire)
}

func (t *ReadReply) Unmarshal(wire io.Reader) error {
	var b [4]byte
	var bs []byte
	bs = b[:4]
	if _, err := io.ReadAtLeast(wire, bs, 4); err != nil {
		return err
	}
	t.CommandId = int32((uint32(bs[0]) | (uint32(bs[1]) << 8) | (uint32(bs[2]) << 16) | (uint32(bs[3]) << 24)))
	t.Value.Unmarshal(wire)
	return nil
}

func (t *ProposeAndReadReply) BinarySize() (nbytes int, sizeKnown bool) {
	return 0, false
}

func (t *ProposeAndReadReply) Marshal(wire io.Writer) {
	var b [5]byte
	var bs []byte
	bs = b[:5]
	bs[0] = byte(t.OK)
	tmp32 := t.CommandId
	bs[1] = byte(tmp32)
	bs[2] = byte(tmp32 >> 8)
	bs[3] = byte(tmp32 >> 16)
	bs[4] = byte(tmp32 >> 24)
	wire.Write(bs)
	t.Value.Marshal(wire)
}

func (t *ProposeAndReadReply) Unmarshal(wire io.Reader) error {
	var b [5]byte
	var bs []byte
	bs = b[:5]
	if _, err := io.ReadAtLeast(wire, bs, 5); err != nil {
		return err
	}
	t.OK = uint8(bs[0])
	t.CommandId = int32((uint32(bs[1]) | (uint32(bs[2]) << 8) | (uint32(bs[3]) << 16) | (uint32(bs[4]) << 24)))
	t.Value.Unmarshal(wire)
	return nil
}

func (t *Beacon) BinarySize() (nbytes int, sizeKnown bool) {
	return 8, true
}

func (t *Beacon) Marshal(wire io.Writer) {
	var b [8]byte
	var bs []byte
	bs = b[:8]
	tmp64 := t.Timestamp
	bs[0] = byte(tmp64)
	bs[1] = byte(tmp64 >> 8)
	bs[2] = byte(tmp64 >> 16)
	bs[3] = byte(tmp64 >> 24)
	bs[4] = byte(tmp64 >> 32)
	bs[5] = byte(tmp64 >> 40)
	bs[6] = byte(tmp64 >> 48)
	bs[7] = byte(tmp64 >> 56)
	wire.Write(bs)
}

func (t *Beacon) Unmarshal(wire io.Reader) error {
	var b [8]byte
	var bs []byte
	bs = b[:8]
	if _, err := io.ReadAtLeast(wire, bs, 8); err != nil {
		return err
	}
	t.Timestamp = int64((uint64(bs[0]) | (uint64(bs[1]) << 8) | (uint64(bs[2]) << 16) | (uint64(bs[3]) << 24) | (uint64(bs[4]) << 32) | (uint64(bs[5]) << 40) | (uint64(bs[6]) << 48) | (uint64(bs[7]) << 56)))
	return nil
}
