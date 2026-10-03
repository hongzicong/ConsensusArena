package swift

// Replica state, configuration, RPC registration, and startup.
import (
	"fmt"
	"sync"
	"time"

	"github.com/hongzicong/ConsensusArena/config"
	"github.com/hongzicong/ConsensusArena/dlog"
	"github.com/hongzicong/ConsensusArena/hook"
	"github.com/hongzicong/ConsensusArena/replica"
	"github.com/hongzicong/ConsensusArena/replica/defs"
	fastrpc "github.com/hongzicong/ConsensusArena/rpc"
	"github.com/hongzicong/ConsensusArena/state"
	cmap "github.com/orcaman/concurrent-map"
)

var MaxDescRoutines = 100

type CommunicationSupply struct {
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

func initCs(cs *CommunicationSupply, t *fastrpc.Table) {
	cs.maxLatency = 0

	cs.fastAckChan = make(chan fastrpc.Serializable, defs.CHAN_BUFFER_SIZE)
	cs.fastAckClientChan = make(chan fastrpc.Serializable, defs.CHAN_BUFFER_SIZE)
	cs.slowAckChan = make(chan fastrpc.Serializable, defs.CHAN_BUFFER_SIZE)
	cs.lightSlowAckChan = make(chan fastrpc.Serializable, defs.CHAN_BUFFER_SIZE)
	cs.acksChan = make(chan fastrpc.Serializable, defs.CHAN_BUFFER_SIZE)
	cs.optAcksChan = make(chan fastrpc.Serializable, defs.CHAN_BUFFER_SIZE)
	cs.replyChan = make(chan fastrpc.Serializable, defs.CHAN_BUFFER_SIZE)
	cs.newLeaderChan = make(chan fastrpc.Serializable, defs.CHAN_BUFFER_SIZE)
	cs.newLeaderAckNChan = make(chan fastrpc.Serializable, defs.CHAN_BUFFER_SIZE)
	cs.syncChan = make(chan fastrpc.Serializable, defs.CHAN_BUFFER_SIZE)
	cs.pingChan = make(chan fastrpc.Serializable, defs.CHAN_BUFFER_SIZE)
	cs.pingRepChan = make(chan fastrpc.Serializable, defs.CHAN_BUFFER_SIZE)
	cs.collectChan = make(chan fastrpc.Serializable, defs.CHAN_BUFFER_SIZE)
	cs.acceptChan = make(chan fastrpc.Serializable, defs.CHAN_BUFFER_SIZE)

	cs.fastAckRPC = t.Register(new(MFastAck), cs.fastAckChan)
	cs.fastAckClientRPC = t.Register(new(MFastAckClient), cs.fastAckClientChan)
	cs.slowAckRPC = t.Register(new(MSlowAck), cs.slowAckChan)
	cs.lightSlowAckRPC = t.Register(new(MLightSlowAck), cs.lightSlowAckChan)
	cs.acksRPC = t.Register(new(MAcks), cs.acksChan)
	cs.optAcksRPC = t.Register(new(MOptAcks), cs.optAcksChan)
	cs.replyRPC = t.Register(new(MReply), cs.replyChan)
	cs.newLeaderRPC = t.Register(new(MNewLeader), cs.newLeaderChan)
	cs.newLeaderAckNRPC = t.Register(new(MNewLeaderAckN), cs.newLeaderAckNChan)
	cs.syncRPC = t.Register(new(MSync), cs.syncChan)
	cs.pingRPC = t.Register(new(MPing), cs.pingChan)
	cs.pingRepRPC = t.Register(new(MPingRep), cs.pingRepChan)
	cs.collectRPC = t.Register(new(MCollect), cs.collectChan)
	cs.acceptRPC = t.Register(new(MAccept), cs.acceptChan)
}

type Replica struct {
	*replica.Replica

	ballot  int32
	cballot int32
	status  int

	cmdDescs  cmap.ConcurrentMap
	delivered cmap.ConcurrentMap

	batcher *Batcher
	repchan *replyChan

	keys map[state.Key]keyInfo
	hlog map[state.Key]*HashLog

	proposalBatches       uint64
	deferredHashElisions  uint64
	hashBacklogDeferrals  uint64
	proposalBatchCommands uint64
	proposalBatchMax      int
	proposedInBallot      map[CommandId]struct{}
	seqnum                int
	pendingHashUpds       map[CommandId]*UpdateEntry
	recoveryCmds          map[CommandId]int // commands installed by the current Sync

	history      []commandStaticDesc
	historySize  int
	historyStart int

	qs *replica.QuorumSystem
	SQ replica.QuorumI
	FQ replica.QuorumI
	cs CommunicationSupply

	fixedMajority bool

	optExec     bool
	fastRead    bool
	deliverChan chan CommandId

	descPool     sync.Pool
	poolLevel    int
	routineCount int

	recover        chan int32
	recStart       time.Time
	newLeaderAckNs *replica.MsgSet

	proposes map[CommandId]*defs.GPropose

	// take only slow paths for these addresses
	slowAddrs map[string]struct{}
}

func New(alias string, rid int, addrs []string, exec, fastRead, optExec bool,
	pl, f int, conf *config.Config, l *dlog.Logger, slowAddrs []string) *Replica {
	cmap.SHARD_COUNT = 32768

	r := &Replica{
		Replica: replica.New(alias, rid, f, addrs, false, exec, false, conf, l),

		ballot:  0,
		cballot: 0,
		status:  NORMAL,

		cmdDescs:  cmap.New(),
		delivered: cmap.New(),

		keys:             make(map[state.Key]keyInfo),
		hlog:             make(map[state.Key]*HashLog),
		seqnum:           0,
		proposedInBallot: make(map[CommandId]struct{}),
		pendingHashUpds:  make(map[CommandId]*UpdateEntry),

		history:      make([]commandStaticDesc, HISTORY_SIZE),
		historySize:  0,
		historyStart: 0,

		fixedMajority: false,

		optExec:     optExec,
		fastRead:    fastRead,
		deliverChan: make(chan CommandId, defs.CHAN_BUFFER_SIZE),

		poolLevel:    pl,
		routineCount: 0,

		recover: make(chan int32, 8),

		descPool: sync.Pool{
			New: func() interface{} {
				return &commandDesc{}
			},
		},

		proposes: make(map[CommandId]*defs.GPropose),

		slowAddrs: make(map[string]struct{}),
	}

	for _, addr := range slowAddrs {
		r.slowAddrs[addr] = struct{}{}
	}

	r.SQ = replica.NewMajorityOf(r.N)
	r.FQ = replica.NewThreeQuartersOf(r.N)

	r.batcher = NewBatcher(r, 16)
	r.repchan = NewReplyChan(r)

	initial := replica.NewQuorum(len(conf.Plan.FastQuorum))
	for _, member := range conf.Plan.FastQuorum {
		initial[int32(member.Rank)] = struct{}{}
	}
	qs, err := replica.NewQuorumSystem(r.N/2+1, r.Replica, initial, conf.Plan.LeaderID())
	if err != nil {
		r.Fatal(err)
	}
	r.qs = qs
	r.ballot = r.qs.BallotAt(0)
	r.cballot = r.ballot
	r.fixedMajority = true
	r.FQ = r.qs.AQ(r.ballot)

	initCs(&r.cs, r.RPC)

	r.Println("the leader is:", r.leader(), "ballot is:", r.ballot)

	hook.HookUser1(func() {
		totalNum := 0
		slowPaths := 0
		for i := 0; i < HISTORY_SIZE; i++ {
			if r.history[i].dep == nil {
				continue
			}
			totalNum++
			if r.history[i].slowPath {
				slowPaths++
			}
		}

		fmt.Printf("Total number of commands: %d\n", totalNum)
		fmt.Printf("Number of slow paths: %d\n", slowPaths)
	})

	r.Println("SQ:", r.SQ)
	r.Println("FQ:", r.FQ)

	go r.run()

	return r
}
