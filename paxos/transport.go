package paxos

import (
	"github.com/hongzicong/ConsensusArena/protocol"
	"github.com/hongzicong/ConsensusArena/replica"
	fastrpc "github.com/hongzicong/ConsensusArena/rpc"
	"time"
)

func newTransportRuntime(base *replica.Replica, machine runtimeProtocol) *transportRuntime {
	return &transportRuntime{
		Transport: replica.Transport{Base: base, Local: func(msg fastrpc.Serializable) {
			protocol.Must(machine.Handle(msg, time.Time{}))
		}},
		protocol: machine,
		in:       make(chan fastrpc.Serializable, 65536),
		control:  make(chan chan int32),
	}
}
