package curp

import (
	"time"

	"github.com/hongzicong/ConsensusArena/protocol"
	"github.com/hongzicong/ConsensusArena/replica"
	"github.com/hongzicong/ConsensusArena/replica/defs"
	fastrpc "github.com/hongzicong/ConsensusArena/rpc"
)

// Retain message registrations in their existing order for wire compatibility.
type transportChannels struct {
	replyChan     chan fastrpc.Serializable
	acceptChan    chan fastrpc.Serializable
	acceptAckChan chan fastrpc.Serializable
	aacksChan     chan fastrpc.Serializable
	recordAckChan chan fastrpc.Serializable
	commitChan    chan fastrpc.Serializable
	syncChan      chan fastrpc.Serializable
	syncReplyChan chan fastrpc.Serializable

	replyRPC     uint8
	acceptRPC    uint8
	acceptAckRPC uint8
	aacksRPC     uint8
	recordAckRPC uint8
	commitRPC    uint8
	syncRPC      uint8
	syncReplyRPC uint8
}

func registerRPCs(channels *transportChannels, t *fastrpc.Table) {
	channels.replyChan = make(chan fastrpc.Serializable, defs.CHAN_BUFFER_SIZE)
	channels.acceptChan = make(chan fastrpc.Serializable, defs.CHAN_BUFFER_SIZE)
	channels.acceptAckChan = make(chan fastrpc.Serializable, defs.CHAN_BUFFER_SIZE)
	channels.aacksChan = make(chan fastrpc.Serializable, defs.CHAN_BUFFER_SIZE)
	channels.recordAckChan = make(chan fastrpc.Serializable, defs.CHAN_BUFFER_SIZE)
	channels.commitChan = make(chan fastrpc.Serializable, defs.CHAN_BUFFER_SIZE)
	channels.syncChan = make(chan fastrpc.Serializable, defs.CHAN_BUFFER_SIZE)
	channels.syncReplyChan = make(chan fastrpc.Serializable, defs.CHAN_BUFFER_SIZE)

	channels.replyRPC = t.Register(new(MReply), channels.replyChan)
	channels.acceptRPC = t.Register(new(MAccept), channels.acceptChan)
	channels.acceptAckRPC = t.Register(new(MAcceptAck), channels.acceptAckChan)
	channels.aacksRPC = t.Register(new(MAAcks), channels.aacksChan)
	channels.recordAckRPC = t.Register(new(MRecordAck), channels.recordAckChan)
	channels.commitRPC = t.Register(new(MCommit), channels.commitChan)
	channels.syncRPC = t.Register(new(MSync), channels.syncChan)
	channels.syncReplyRPC = t.Register(new(MSyncReply), channels.syncReplyChan)
}

func newEventLoop(base *replica.Replica, machine replica.EventLoopMachine) *replica.EventLoop {
	return &replica.EventLoop{
		Transport: replica.Transport{Base: base, Local: func(msg fastrpc.Serializable) {
			protocol.Must(machine.Step(msg, time.Time{}))
		}},
		Machine: machine,
		Inbox:   make(chan fastrpc.Serializable, 65536),
		Control: make(chan chan int32),
	}
}
