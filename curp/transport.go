package curp

import (
	"time"

	"github.com/hongzicong/ConsensusArena/protocol"
	"github.com/hongzicong/ConsensusArena/replica"
	fastrpc "github.com/hongzicong/ConsensusArena/rpc"
)

// Retain message registrations in their existing order for wire compatibility.
type transportChannels struct {
	replyChan     chan fastrpc.Serializable
	recordAckChan chan fastrpc.Serializable
	syncReplyChan chan fastrpc.Serializable

	replyRPC     uint8
	recordAckRPC uint8
	syncReplyRPC uint8
}

func registerRPCs(channels *transportChannels, t *fastrpc.Table) {
	// Only clients supply reply queues. The other registrations retain their
	// wire codes and factories without allocating queues that nobody consumes.
	channels.replyRPC = t.Register(new(MReply), channels.replyChan)
	t.Register(new(MAccept), nil)
	t.Register(new(MAcceptAck), nil)
	t.Register(new(MAAcks), nil)
	channels.recordAckRPC = t.Register(new(MRecordAck), channels.recordAckChan)
	t.Register(new(MCommit), nil)
	t.Register(new(MSync), nil)
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
