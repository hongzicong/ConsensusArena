package swift

import (
	"time"

	"github.com/hongzicong/ConsensusArena/replica/defs"
	fastrpc "github.com/hongzicong/ConsensusArena/rpc"
	"github.com/hongzicong/ConsensusArena/state"
)

type transportChannels struct {
	maxLatency time.Duration

	fastAckChan       chan fastrpc.Serializable
	fastAckClientChan chan fastrpc.Serializable
	lightSlowAckChan  chan fastrpc.Serializable
	acksChan          chan fastrpc.Serializable
	optAcksChan       chan fastrpc.Serializable
	replyChan         chan fastrpc.Serializable
	newLeaderChan     chan fastrpc.Serializable
	newLeaderAckNChan chan fastrpc.Serializable
	syncChan          chan fastrpc.Serializable
	acceptChan        chan fastrpc.Serializable

	rpcCodes
}

type rpcCodes struct {
	fastAckRPC       uint8
	fastAckClientRPC uint8
	slowAckRPC       uint8
	lightSlowAckRPC  uint8
	acksRPC          uint8
	optAcksRPC       uint8
	replyRPC         uint8
	newLeaderRPC     uint8
	newLeaderAckNRPC uint8
	shareStateRPC    uint8
	syncRPC          uint8
	pingRPC          uint8
	pingRepRPC       uint8
	collectRPC       uint8
	acceptRPC        uint8
}

func registerRPCs(channels *transportChannels, t *fastrpc.Table) {
	// Each role allocates only its consumed queues before registration. Keep all
	// factories in their original order; nil destinations remain decode-only.
	channels.fastAckRPC = t.Register(new(MFastAck), channels.fastAckChan)
	channels.fastAckClientRPC = t.Register(new(MFastAckClient), channels.fastAckClientChan)
	channels.slowAckRPC = t.Register(new(MSlowAck), nil)
	channels.lightSlowAckRPC = t.Register(new(MLightSlowAck), channels.lightSlowAckChan)
	channels.acksRPC = t.Register(new(MAcks), channels.acksChan)
	channels.optAcksRPC = t.Register(new(MOptAcks), channels.optAcksChan)
	channels.replyRPC = t.Register(new(MReply), channels.replyChan)
	channels.newLeaderRPC = t.Register(new(MNewLeader), channels.newLeaderChan)
	channels.newLeaderAckNRPC = t.Register(new(MNewLeaderAckN), channels.newLeaderAckNChan)
	channels.syncRPC = t.Register(new(MSync), channels.syncChan)
	channels.pingRPC = t.Register(new(MPing), nil)
	channels.pingRepRPC = t.Register(new(MPingRep), nil)
	channels.collectRPC = t.Register(new(MCollect), nil)
	channels.acceptRPC = t.Register(new(MAccept), channels.acceptChan)
}

type replyArgs struct {
	hs      []SHash
	val     state.Value
	cmdID   defs.RequestID
	finish  chan interface{}
	propose *proposal
}

type replyWorker struct {
	ok   chan struct{}
	exit chan struct{}
	args chan *replyArgs
}

func newReplyWorker(r *Replica) *replyWorker {
	rc := &replyWorker{
		ok:   make(chan struct{}, 1),
		exit: make(chan struct{}, 2),
		args: make(chan *replyArgs, defs.CHAN_BUFFER_SIZE),
	}

	go func() {
		for !r.Shutdown {
			select {
			case <-rc.exit:
				rc.ok <- struct{}{}
				return
			case args := <-rc.args:
				r.core.completeReply(args)
			}
		}
	}()

	return rc
}

func (r *replyWorker) stop() {
	r.exit <- struct{}{}
	<-r.ok
}

func (r *replyWorker) reply(desc *commandDesc, cmdID defs.RequestID, val state.Value) {
	hs := make([]SHash, len(desc.hs))
	copy(hs, desc.hs)

	r.args <- &replyArgs{
		hs:      hs,
		val:     val,
		cmdID:   cmdID,
		finish:  desc.msgs,
		propose: desc.propose,
	}
}
