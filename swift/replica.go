package swift

// Replica state, configuration, RPC registration, and startup.
import (
	"fmt"
	"github.com/hongzicong/ConsensusArena/config"
	"github.com/hongzicong/ConsensusArena/dlog"
	"github.com/hongzicong/ConsensusArena/hook"
	"github.com/hongzicong/ConsensusArena/replica"
	"github.com/hongzicong/ConsensusArena/replica/defs"
	"github.com/hongzicong/ConsensusArena/replicaset"
	fastrpc "github.com/hongzicong/ConsensusArena/rpc"
	"github.com/hongzicong/ConsensusArena/state"
	cmap "github.com/orcaman/concurrent-map"
	"sync"
	"time"
)

var MaxDescRoutines = 100

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

// Replica owns runtime resources and exposes the external protocol event boundary.
type Replica struct {
	*replica.Replica
	core                *core
	channels            transportChannels
	control             chan leaderCall
	recover             chan int32
	delivery            chan defs.RequestID
	replyWorker         *replyWorker
	descriptorPool      sync.Pool
	descriptorPoolLevel int
	descriptorCount     int
}

func New(alias string, replicaID int, addrs []string, executeEnabled, fastRead, optExec bool,
	descriptorPoolLevel, f int, conf *config.Config, logger *dlog.Logger, slowAddrs []string) *Replica {
	cmap.SHARD_COUNT = 32768

	r := &Replica{Replica: replica.New(alias, replicaID, f, addrs, executeEnabled, conf, logger), control: make(chan leaderCall, 8), recover: make(chan int32, 8), delivery: make(chan defs.RequestID, defs.CHAN_BUFFER_SIZE), descriptorPoolLevel: descriptorPoolLevel, descriptorPool: sync.Pool{New: func() interface{} { return &commandDesc{} }}}
	initial := replicaset.New()
	for _, member := range conf.Plan.FastQuorum {
		initial.Add(member.Rank)
	}

	r.core = newCore(corePorts{
		N: r.N, ID: r.ID, Logger: r.Logger, codes: &r.channels.rpcCodes,
		send:                func(id int, code uint8, msg fastrpc.Serializable) error { return r.Messages().Send(id, code, msg) },
		broadcast:           func(msg fastrpc.Serializable, code uint8) int { return r.SendToAll(msg, code) },
		sendClient:          r.SendClientMsg,
		peerAlive:           func(id int) bool { return r.Alive[id] },
		executeEnabled:      func() bool { return r.ExecuteEnabled },
		replyAfterExecution: func() bool { return r.ReplyAfterExecution },
		requeueProposal:     r.requeueProposal, scheduleDelivery: r.scheduleDelivery,
		startDescriptor: r.startDescriptor, stopDescriptors: r.stopDescriptors, resetDescriptors: func() { r.descriptorCount = 0 },
		sequentialDescriptor: func() bool { return r.descriptorCount >= MaxDescRoutines }, allocDesc: r.allocDesc, freeDesc: r.freeDesc,
		stopReplies: func() { r.replyWorker.stop() }, resetReplies: func() { r.replyWorker = newReplyWorker(r) },
		queueReply: func(d *commandDesc, id defs.RequestID, v state.Value) { r.replyWorker.reply(d, id, v) },
		awaitReply: r.awaitReply, awaitRecovery: r.awaitRecovery,
	}, fastRead, optExec, slowAddrs, initial, conf.Plan.LeaderID())
	r.core.execute = func(cmd state.Command) state.Value { return cmd.Execute(r.State) }

	r.core.batcher = newMessageBatcher(r, 16)
	r.replyWorker = newReplyWorker(r)

	registerRPCs(&r.channels, r.RPC)

	r.Println("the leader is:", r.core.leader(), "ballot is:", r.core.ballot)

	hook.HookUser1(func() {
		totalNum := 0
		slowPaths := 0
		for i := 0; i < HISTORY_SIZE; i++ {
			if r.core.history[i].dep == nil {
				continue
			}
			totalNum++
			if r.core.history[i].slowPath {
				slowPaths++
			}
		}

		fmt.Printf("Total number of commands: %d\n", totalNum)
		fmt.Printf("Number of slow paths: %d\n", slowPaths)
	})

	r.Println("SQ:", r.core.SQ)
	r.Println("FQ:", r.core.FQ)

	go r.run()

	return r
}
