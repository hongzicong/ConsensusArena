package curp

import (
	"github.com/hongzicong/ConsensusArena/protocol"
	"github.com/hongzicong/ConsensusArena/replica"
	fastrpc "github.com/hongzicong/ConsensusArena/rpc"
	"time"
)

func newEventLoop(base *replica.Replica, machine replica.EventLoopMachine) *replica.EventLoop {
	return &replica.EventLoop{
		Transport: replica.Transport{Base: base, Local: func(msg fastrpc.Serializable) {
			protocol.Must(machine.Handle(msg, time.Time{}))
		}},
		Machine: machine,
		Inbox:   make(chan fastrpc.Serializable, 65536),
		Control: make(chan chan int32),
	}
}
