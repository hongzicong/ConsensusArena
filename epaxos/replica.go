package epaxos

// Replica state, configuration, RPC registration, and startup.
import (
	"sync"

	"github.com/hongzicong/ConsensusArena/config"
	"github.com/hongzicong/ConsensusArena/dlog"
	"github.com/hongzicong/ConsensusArena/replica"
	"github.com/hongzicong/ConsensusArena/replica/defs"
	fastrpc "github.com/hongzicong/ConsensusArena/rpc"
	"github.com/hongzicong/ConsensusArena/state"
)

type Replica struct {
	*replica.Replica
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
	prepareRPC            uint8
	prepareReplyRPC       uint8
	preAcceptRPC          uint8
	preAcceptReplyRPC     uint8
	acceptRPC             uint8
	acceptReplyRPC        uint8
	commitRPC             uint8
	tryPreAcceptRPC       uint8
	tryPreAcceptReplyRPC  uint8
	// the space of all instances (used and not yet used)
	InstanceSpace [][]*Instance
	// highest active instance numbers that this replica knows about
	crtInstance []int32
	// highest committed instance per replica that this replica knows about
	CommittedUpTo []int32
	// instance up to which all commands have been executed (including iteslf)
	ExecedUpTo       []int32
	exec             *Exec
	conflicts        []map[state.Key]*InstPair
	maxSeqPerKey     map[state.Key]int32
	maxSeq           int32
	latestCPReplica  int32
	latestCPInstance int32
	// for synchronizing when sending replies to clients
	// from multiple go-routines
	clientMutex        *sync.Mutex
	instancesToRecover chan *instanceId
	// does this replica think it is the leader
	IsLeader         bool
	maxRecvBallot    int32
	batchWait        int
	transconf        bool
	recoveryMu       sync.Mutex
	recoveryAttempts map[uint64]recoveryAttempt
	*repairState
	transportState
}

func New(alias string, id int, peerAddrList []string, exec, beacon, durable bool, batchWait int, transconf bool, failures int, conf *config.Config, logger *dlog.Logger) *Replica {
	r := &Replica{
		replica.New(alias, id, failures, peerAddrList, true, exec, false, conf, logger),
		make(chan fastrpc.Serializable, defs.CHAN_BUFFER_SIZE),
		make(chan fastrpc.Serializable, defs.CHAN_BUFFER_SIZE),
		make(chan fastrpc.Serializable, defs.CHAN_BUFFER_SIZE),
		make(chan fastrpc.Serializable, defs.CHAN_BUFFER_SIZE),
		make(chan fastrpc.Serializable, defs.CHAN_BUFFER_SIZE),
		make(chan fastrpc.Serializable, defs.CHAN_BUFFER_SIZE*3),
		make(chan fastrpc.Serializable, defs.CHAN_BUFFER_SIZE*3),
		make(chan fastrpc.Serializable, defs.CHAN_BUFFER_SIZE*2),
		make(chan fastrpc.Serializable, defs.CHAN_BUFFER_SIZE),
		make(chan fastrpc.Serializable, defs.CHAN_BUFFER_SIZE),
		0, 0, 0, 0, 0, 0, 0, 0, 0,
		make([][]*Instance, len(peerAddrList)),
		make([]int32, len(peerAddrList)),
		make([]int32, len(peerAddrList)),
		make([]int32, len(peerAddrList)),
		nil,
		make([]map[state.Key]*InstPair, len(peerAddrList)),
		make(map[state.Key]int32),
		0,
		0,
		-1,
		new(sync.Mutex),
		make(chan *instanceId, defs.CHAN_BUFFER_SIZE),
		false,
		-1,
		batchWait,
		transconf,
		sync.Mutex{},
		make(map[uint64]recoveryAttempt),
		newRepairState(),
		newTransportState(id, len(peerAddrList)),
	}

	r.Beacon = false // peer ordering is static; avoid concurrent writes from legacy beacon code
	r.Durable = durable

	for i := 0; i < r.N; i++ {
		r.InstanceSpace[i] = make([]*Instance, MAX_INSTANCE) // FIXME
		r.crtInstance[i] = -1
		r.ExecedUpTo[i] = -1
		r.CommittedUpTo[i] = -1
		r.conflicts[i] = make(map[state.Key]*InstPair, HT_INIT_SIZE)
	}

	r.exec = &Exec{r}

	cpMarker = make([]state.Command, 0)

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
