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
	slowAckChan       chan fastrpc.Serializable
	lightSlowAckChan  chan fastrpc.Serializable
	acksChan          chan fastrpc.Serializable
	optAcksChan       chan fastrpc.Serializable
	replyChan         chan fastrpc.Serializable
	newLeaderChan     chan fastrpc.Serializable
	newLeaderAckNChan chan fastrpc.Serializable
	shareStateChan    chan fastrpc.Serializable
	syncChan          chan fastrpc.Serializable
	pingChan          chan fastrpc.Serializable
	pingRepChan       chan fastrpc.Serializable
	collectChan       chan fastrpc.Serializable
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
	channels.maxLatency = 0

	channels.fastAckChan = make(chan fastrpc.Serializable, defs.CHAN_BUFFER_SIZE)
	channels.fastAckClientChan = make(chan fastrpc.Serializable, defs.CHAN_BUFFER_SIZE)
	channels.slowAckChan = make(chan fastrpc.Serializable, defs.CHAN_BUFFER_SIZE)
	channels.lightSlowAckChan = make(chan fastrpc.Serializable, defs.CHAN_BUFFER_SIZE)
	channels.acksChan = make(chan fastrpc.Serializable, defs.CHAN_BUFFER_SIZE)
	channels.optAcksChan = make(chan fastrpc.Serializable, defs.CHAN_BUFFER_SIZE)
	channels.replyChan = make(chan fastrpc.Serializable, defs.CHAN_BUFFER_SIZE)
	channels.newLeaderChan = make(chan fastrpc.Serializable, defs.CHAN_BUFFER_SIZE)
	channels.newLeaderAckNChan = make(chan fastrpc.Serializable, defs.CHAN_BUFFER_SIZE)
	channels.syncChan = make(chan fastrpc.Serializable, defs.CHAN_BUFFER_SIZE)
	channels.pingChan = make(chan fastrpc.Serializable, defs.CHAN_BUFFER_SIZE)
	channels.pingRepChan = make(chan fastrpc.Serializable, defs.CHAN_BUFFER_SIZE)
	channels.collectChan = make(chan fastrpc.Serializable, defs.CHAN_BUFFER_SIZE)
	channels.acceptChan = make(chan fastrpc.Serializable, defs.CHAN_BUFFER_SIZE)

	channels.fastAckRPC = t.Register(new(MFastAck), channels.fastAckChan)
	channels.fastAckClientRPC = t.Register(new(MFastAckClient), channels.fastAckClientChan)
	channels.slowAckRPC = t.Register(new(MSlowAck), channels.slowAckChan)
	channels.lightSlowAckRPC = t.Register(new(MLightSlowAck), channels.lightSlowAckChan)
	channels.acksRPC = t.Register(new(MAcks), channels.acksChan)
	channels.optAcksRPC = t.Register(new(MOptAcks), channels.optAcksChan)
	channels.replyRPC = t.Register(new(MReply), channels.replyChan)
	channels.newLeaderRPC = t.Register(new(MNewLeader), channels.newLeaderChan)
	channels.newLeaderAckNRPC = t.Register(new(MNewLeaderAckN), channels.newLeaderAckNChan)
	channels.syncRPC = t.Register(new(MSync), channels.syncChan)
	channels.pingRPC = t.Register(new(MPing), channels.pingChan)
	channels.pingRepRPC = t.Register(new(MPingRep), channels.pingRepChan)
	channels.collectRPC = t.Register(new(MCollect), channels.collectChan)
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
