package epaxos

import (
	"github.com/hongzicong/ConsensusArena/config"
	"github.com/hongzicong/ConsensusArena/dlog"
	"github.com/hongzicong/ConsensusArena/replica"
	"github.com/hongzicong/ConsensusArena/replica/defs"
	fastrpc "github.com/hongzicong/ConsensusArena/rpc"
	"github.com/hongzicong/ConsensusArena/state"
	"os"
)

// Replica owns network ingress, RPC registration, RTT observation and storage.
// Replica owns runtime resources and exposes the external protocol event boundary.
type Replica struct {
	*replica.Replica
	core *core
	rpcCodes
	prepareChan           chan fastrpc.Serializable
	preAcceptChan         chan fastrpc.Serializable
	acceptChan            chan fastrpc.Serializable
	commitChan            chan fastrpc.Serializable
	prepareReplyChan      chan fastrpc.Serializable
	preAcceptReplyChan    chan fastrpc.Serializable
	preAcceptOKChan       chan fastrpc.Serializable
	acceptReplyChan       chan fastrpc.Serializable
	tryPreAcceptChan      chan fastrpc.Serializable
	tryPreAcceptReplyChan chan fastrpc.Serializable
	requestChan           chan fastrpc.Serializable
	Durable               bool
	StableStore           *os.File
	Beacon                bool
	EWMA                  []float64
	BeaconChan            chan *defs.GBeacon
	fastClockChan         chan bool
	slowClockChan         chan bool
	control               chan leaderCall
}

type rpcCodes struct {
	prepareRPC           uint8
	prepareReplyRPC      uint8
	preAcceptRPC         uint8
	preAcceptReplyRPC    uint8
	acceptRPC            uint8
	acceptReplyRPC       uint8
	commitRPC            uint8
	tryPreAcceptRPC      uint8
	tryPreAcceptReplyRPC uint8
	requestRPC           uint8
}

func New(alias string, replicaID int, addrs []string, executeEnabled, beacon, durable bool, batchWait int, transconf bool, f int, conf *config.Config, logger *dlog.Logger) *Replica {
	r := &Replica{
		Replica:               replica.New(alias, replicaID, f, addrs, executeEnabled, conf, logger),
		prepareChan:           make(chan fastrpc.Serializable, defs.CHAN_BUFFER_SIZE),
		preAcceptChan:         make(chan fastrpc.Serializable, defs.CHAN_BUFFER_SIZE),
		acceptChan:            make(chan fastrpc.Serializable, defs.CHAN_BUFFER_SIZE),
		commitChan:            make(chan fastrpc.Serializable, defs.CHAN_BUFFER_SIZE),
		prepareReplyChan:      make(chan fastrpc.Serializable, defs.CHAN_BUFFER_SIZE),
		preAcceptReplyChan:    make(chan fastrpc.Serializable, defs.CHAN_BUFFER_SIZE*3),
		preAcceptOKChan:       make(chan fastrpc.Serializable, defs.CHAN_BUFFER_SIZE*3),
		acceptReplyChan:       make(chan fastrpc.Serializable, defs.CHAN_BUFFER_SIZE*2),
		tryPreAcceptChan:      make(chan fastrpc.Serializable, defs.CHAN_BUFFER_SIZE),
		tryPreAcceptReplyChan: make(chan fastrpc.Serializable, defs.CHAN_BUFFER_SIZE),
		requestChan:           make(chan fastrpc.Serializable, 4096),
		Durable:               durable,
		EWMA:                  make([]float64, len(addrs)),
		BeaconChan:            make(chan *defs.GBeacon, defs.CHAN_BUFFER_SIZE),
		control:               make(chan leaderCall, 8),
	}
	r.core = newCore(corePorts{
		N: r.N, F: r.F, ID: r.ID, Logger: r.Logger, M: &r.M, Stats: r.Stats,
		rpcCodes: &r.rpcCodes,
		send:     r.Send, broadcast: r.broadcast, replyResult: r.replyResult, requeueProposal: r.requeueProposal,
		peerAlive:              r.peerAlive,
		preferredPeerOrder:     func() []int32 { return r.PreferredPeerOrder },
		executeEnabled:         func() bool { return r.ExecuteEnabled },
		replyAfterExecution:    func() bool { return r.ReplyAfterExecution },
		recordInstanceMetadata: r.recordInstanceMetadata, recordCommands: r.recordCommands, sync: r.sync,
		replyBeacon: r.ReplyBeacon, beaconTick: r.beaconTick,
	}, batchWait, transconf)
	r.core.execute = func(cmd state.Command) state.Value { return cmd.Execute(r.State) }
	r.Beacon = false // peer ordering is static; avoid concurrent writes from legacy beacon code
	r.Replica.ObserveRTT = r.observeRTT
	//register RPCs
	r.prepareRPC = r.RPC.Register(new(Prepare), r.prepareChan)
	r.prepareReplyRPC = r.RPC.Register(new(PrepareReply), r.prepareReplyChan)
	r.preAcceptRPC = r.RPC.Register(new(PreAccept), r.preAcceptChan)
	r.preAcceptReplyRPC = r.RPC.Register(new(PreAcceptReply), r.preAcceptReplyChan)
	r.acceptRPC = r.RPC.Register(new(Accept), r.acceptChan)
	r.acceptReplyRPC = r.RPC.Register(new(AcceptReply), r.acceptReplyChan)
	r.commitRPC = r.RPC.Register(new(Commit), r.commitChan)
	r.tryPreAcceptRPC = r.RPC.Register(new(TryPreAccept), r.tryPreAcceptChan)
	r.tryPreAcceptReplyRPC = r.RPC.Register(new(TryPreAcceptReply), r.tryPreAcceptReplyChan)

	r.Stats.M["weird"], r.Stats.M["conflicted"], r.Stats.M["slow"], r.Stats.M["fast"], r.Stats.M["totalCommitTime"], r.Stats.M["totalBatching"], r.Stats.M["totalBatchingSize"] = 0, 0, 0, 0, 0, 0, 0
	r.Stats.M["proposedCommands"], r.Stats.M["maxBatchSize"], r.Stats.M["maxProposalQueue"] = 0, 0, 0
	r.Stats.M["recoveryScheduled"], r.Stats.M["recoverySuppressed"], r.Stats.M["recoveryQueueFull"] = 0, 0, 0
	r.Stats.M["executedCommands"], r.Stats.M["clientReplies"] = 0, 0

	r.requestRPC = r.RPC.Register(new(repairRequest), r.requestChan)
	go r.run()

	return r
}
