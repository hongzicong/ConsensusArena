package epaxos

import (
	"encoding/binary"
	"io"
	"sync"
	"time"

	"github.com/hongzicong/ConsensusArena/config"
	"github.com/hongzicong/ConsensusArena/dlog"
	"github.com/hongzicong/ConsensusArena/replica"
	"github.com/hongzicong/ConsensusArena/replica/defs"
	fastrpc "github.com/hongzicong/ConsensusArena/rpc"
	"github.com/hongzicong/ConsensusArena/state"
)

const MAX_INSTANCE = 10 * 1024 * 1024

const MAX_DEPTH_DEP = 10
const TRUE = uint8(1)
const FALSE = uint8(0)
const ADAPT_TIME_SEC = 10

const COMMIT_GRACE_PERIOD = 3 * time.Second

const MAX_BATCH = 1000

// Bound the unexecuted local suffix. With compressed dependencies, an
// unlimited proposal stream can keep extending an open SCC faster than its
// last dependencies commit. Backpressure lets that component close.
const MAX_INFLIGHT_INSTANCES = 32

const INITIAL_RECOVERY_BACKOFF = 3 * time.Second
const MAX_RECOVERY_BACKOFF = 6 * time.Second

const BF_K = 4
const BF_M_N = 32.0

const HT_INIT_SIZE = 200000

// main differences with the original code base
// - fix N=3 case
// - add vbal variable (TLA spec. is wrong)
// - remove checkpoints (need to fix them first)
// - remove short commits (with N>7 propagating committed dependencies is necessary)
// - must run with thriftiness on (recovery is incorrect otherwise)
// - when conflicts are transitive skip waiting prior commuting commands

var cpMarker []state.Command
var cpcounter = 0

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
}

type recoveryAttempt struct {
	nextAttempt time.Time
	backoff     time.Duration
}

type InstPair struct {
	last      int32
	lastWrite int32
}

type Instance struct {
	Cmds           []state.Command
	bal, vbal      int32
	Status         int8
	Seq            int32
	Deps           []int32
	lb             *LeaderBookkeeping
	Index, Lowlink int
	onStack        bool
	bfilter        any
	proposeTime    int64
	id             *instanceId
}

type instanceId struct {
	replica  int32
	instance int32
}

type LeaderBookkeeping struct {
	clientProposals                        []*defs.GPropose
	ballot                                 int32
	allEqual                               bool
	preAcceptOKs                           int
	acceptOKs                              int
	nacks                                  int
	originalDeps                           []int32
	committedDeps                          []int32
	prepareReplies                         []*PrepareReply
	preparing                              bool
	tryingToPreAccept                      bool
	possibleQuorum                         []bool
	tpaReps                                int
	tpaAccepted                            bool
	lastTriedBallot                        int32
	cmds                                   []state.Command
	status                                 int8
	seq                                    int32
	deps                                   []int32
	leaderResponded                        bool
	preVoters, acceptVoters, prepareVoters map[int32]bool
	phaseStarted                           time.Time
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

func (r *Replica) recordInstanceMetadata(inst *Instance) {
	if !r.Durable {
		return
	}

	b := make([]byte, 9+r.N*4)
	binary.LittleEndian.PutUint32(b[0:4], uint32(inst.bal))
	binary.LittleEndian.PutUint32(b[0:4], uint32(inst.vbal))
	b[4] = byte(inst.Status)
	binary.LittleEndian.PutUint32(b[5:9], uint32(inst.Seq))
	l := 9
	for _, dep := range inst.Deps {
		binary.LittleEndian.PutUint32(b[l:l+4], uint32(dep))
		l += 4
	}
	r.StableStore.Write(b[:])
}

func (r *Replica) recordCommands(cmds []state.Command) {
	if !r.Durable {
		return
	}

	if cmds == nil {
		return
	}
	for i := 0; i < len(cmds); i++ {
		cmds[i].Marshal(io.Writer(r.StableStore))
	}
}

func (r *Replica) sync() {
	if !r.Durable {
		return
	}

	r.StableStore.Sync()
}

var fastClockChan chan bool
var slowClockChan chan bool

func (r *Replica) fastClock() {
	for !r.Shutdown {
		time.Sleep(time.Duration(r.batchWait) * time.Millisecond)
		fastClockChan <- true
	}
}
func (r *Replica) slowClock() {
	for !r.Shutdown {
		time.Sleep(150 * time.Millisecond)
		slowClockChan <- true
	}
}

func (r *Replica) stopAdapting() {
	time.Sleep(1000 * 1000 * 1000 * ADAPT_TIME_SEC)
	r.Beacon = false
	time.Sleep(1000 * 1000 * 1000)

	for i := 0; i < r.N-1; i++ {
		min := i
		for j := i + 1; j < r.N-1; j++ {
			if r.Ewma[r.PreferredPeerOrder[j]] < r.Ewma[r.PreferredPeerOrder[min]] {
				min = j
			}
		}
		aux := r.PreferredPeerOrder[i]
		r.PreferredPeerOrder[i] = r.PreferredPeerOrder[min]
		r.PreferredPeerOrder[min] = aux
	}

	r.Println(r.PreferredPeerOrder)
}

func (r *Replica) BatchingEnabled() bool {
	return r.batchWait > 0
}

func (r *Replica) maxBatchCommands() int {
	if !r.BatchingEnabled() {
		return 1
	}
	return MAX_BATCH
}

func recoveryKey(replica, instance int32) uint64 {
	return uint64(uint32(replica))<<32 | uint64(uint32(instance))
}

func (r *Replica) scheduleRecovery(replica, instance int32, now time.Time) bool {
	key := recoveryKey(replica, instance)
	r.recoveryMu.Lock()
	attempt, exists := r.recoveryAttempts[key]
	if exists && now.Before(attempt.nextAttempt) {
		r.recoveryMu.Unlock()
		r.M.Lock()
		r.Stats.M["recoverySuppressed"]++
		r.M.Unlock()
		return false
	}
	backoff := INITIAL_RECOVERY_BACKOFF
	if exists {
		backoff = attempt.backoff * 2
		if backoff > MAX_RECOVERY_BACKOFF {
			backoff = MAX_RECOVERY_BACKOFF
		}
	}
	r.recoveryAttempts[key] = recoveryAttempt{nextAttempt: now.Add(backoff), backoff: backoff}
	r.recoveryMu.Unlock()

	select {
	case r.instancesToRecover <- &instanceId{replica: replica, instance: instance}:
		r.M.Lock()
		r.Stats.M["recoveryScheduled"]++
		r.M.Unlock()
		if r.Logger != nil {
			r.Printf("EPAXOS_RECOVERY_SCHEDULED replica=%d instance=%d next_backoff=%s", replica, instance, backoff)
		}
		return true
	default:
		r.M.Lock()
		r.Stats.M["recoveryQueueFull"]++
		r.M.Unlock()
		return false
	}
}

func (r *Replica) clearRecovery(replica, instance int32) {
	r.recoveryMu.Lock()
	delete(r.recoveryAttempts, recoveryKey(replica, instance))
	r.recoveryMu.Unlock()
}

func (r *Replica) logProgress() {
	r.M.Lock()
	proposed := r.Stats.M["proposedCommands"]
	batches := r.Stats.M["totalBatching"]
	maxBatch := r.Stats.M["maxBatchSize"]
	maxProposalQueue := r.Stats.M["maxProposalQueue"]
	fast := r.Stats.M["fast"]
	slow := r.Stats.M["slow"]
	executed := r.Stats.M["executedCommands"]
	replies := r.Stats.M["clientReplies"]
	recoveryScheduled := r.Stats.M["recoveryScheduled"]
	recoverySuppressed := r.Stats.M["recoverySuppressed"]
	r.M.Unlock()
	r.Printf("EPAXOS_PROGRESS proposed=%d batches=%d max_batch=%d max_proposal_queue=%d fast=%d slow=%d executed=%d replies=%d recovery_scheduled=%d recovery_suppressed=%d recovery_queue=%d",
		proposed, batches, maxBatch, maxProposalQueue, fast, slow, executed, replies, recoveryScheduled, recoverySuppressed, len(r.instancesToRecover))
	r.Printf("EPAXOS_REPAIR active=%d blocked=%d send_drops=%d executed_prefix=%v known_prefix=%v", len(r.active), len(r.blocked), r.Stats.M["sendQueueDrops"], r.ExecedUpTo, r.crtInstance)
	for q := int32(0); q < int32(r.N); q++ {
		slot := r.ExecedUpTo[q] + 1
		if slot > r.crtInstance[q] {
			continue
		}
		inst := r.InstanceSpace[q][slot]
		if inst != nil {
			r.Printf("EPAXOS_HEAD owner=%d slot=%d status=%d promise=%d value_ballot=%d deps=%v", q, slot, inst.Status, inst.bal, inst.vbal, inst.Deps)
		}
	}
}

func (r *Replica) run() {
	r.ConnectToPeers()
	r.startSenders()

	r.ComputeClosestPeers()

	execTicker := time.NewTicker(2 * time.Millisecond)
	defer execTicker.Stop()

	slowClockChan = make(chan bool, 1)
	fastClockChan = make(chan bool, 1)
	go r.slowClock()

	if r.BatchingEnabled() {
		go r.fastClock()
	}

	if r.Beacon {
		go r.stopAdapting()
	}

	onOffProposeChan := r.ProposeChan
	progressTicker := time.NewTicker(5 * time.Second)
	defer progressTicker.Stop()

	go r.WaitForClientConnections()

	for !r.Shutdown {

		select {

		case propose := <-onOffProposeChan:
			r.handlePropose(propose)
			if r.BatchingEnabled() || r.crtInstance[r.Id]-r.ExecedUpTo[r.Id] >= MAX_INFLIGHT_INSTANCES {
				onOffProposeChan = nil
			}
			break

		case <-fastClockChan:
			if r.crtInstance[r.Id]-r.ExecedUpTo[r.Id] < MAX_INFLIGHT_INSTANCES {
				onOffProposeChan = r.ProposeChan
			}
			break

		case prepareS := <-r.prepareChan:
			prepare := prepareS.(*Prepare)
			r.handlePrepare(prepare)
			break

		case preAcceptS := <-r.preAcceptChan:
			preAccept := preAcceptS.(*PreAccept)
			r.handlePreAccept(preAccept)
			break

		case acceptS := <-r.acceptChan:
			accept := acceptS.(*Accept)
			r.handleAccept(accept)
			break

		case commitS := <-r.commitChan:
			commit := commitS.(*Commit)
			r.handleCommit(commit)
			break

		case prepareReplyS := <-r.prepareReplyChan:
			prepareReply := prepareReplyS.(*PrepareReply)
			r.handlePrepareReply(prepareReply)
			break

		case preAcceptReplyS := <-r.preAcceptReplyChan:
			preAcceptReply := preAcceptReplyS.(*PreAcceptReply)
			r.handlePreAcceptReply(preAcceptReply)
			break

		case acceptReplyS := <-r.acceptReplyChan:
			acceptReply := acceptReplyS.(*AcceptReply)
			r.handleAcceptReply(acceptReply)
			break

		case tryPreAcceptS := <-r.tryPreAcceptChan:
			tryPreAccept := tryPreAcceptS.(*TryPreAccept)
			r.handleTryPreAccept(tryPreAccept)
			break

		case tryPreAcceptReplyS := <-r.tryPreAcceptReplyChan:
			tryPreAcceptReply := tryPreAcceptReplyS.(*TryPreAcceptReply)
			r.handleTryPreAcceptReply(tryPreAcceptReply)
			break

		case beacon := <-r.BeaconChan:
			r.ReplyBeacon(beacon)
			break

		case <-slowClockChan:
			if r.Beacon {
				r.Printf("weird %d; conflicted %d; slow %d; fast %d\n", r.Stats.M["weird"], r.Stats.M["conflicted"], r.Stats.M["slow"], r.Stats.M["fast"])
				for q := int32(0); q < int32(r.N); q++ {
					if q == r.Id {
						continue
					}
					r.SendBeacon(q)
				}
			}
			break

		case now := <-execTicker.C:
			r.repairTick(now)
			if r.Exec {
				r.executeReady(now)
			}
			if r.crtInstance[r.Id]-r.ExecedUpTo[r.Id] >= MAX_INFLIGHT_INSTANCES {
				onOffProposeChan = nil
			} else if !r.BatchingEnabled() {
				onOffProposeChan = r.ProposeChan
			}

		case <-progressTicker.C:
			r.logProgress()
			break

		case req := <-r.requestChan:
			r.handleRepairRequest(req.(*repairRequest))

		case iid := <-r.instancesToRecover:
			r.startRecoveryForInstance(iid.replica, iid.instance)
		}
	}
}

func isInitialBallot(ballot int32, replica int32, instance int32) bool {
	return ballot == replica
}

func (r *Replica) makeBallot(replica int32, instance int32) {
	inst := r.InstanceSpace[replica][instance]
	high := inst.bal
	if r.maxRecvBallot > high {
		high = r.maxRecvBallot
	}
	inst.lb.lastTriedBallot = (high/int32(r.N)+1)*int32(r.N) + r.Id
}

func (r *Replica) replyPrepare(replicaId int32, reply *PrepareReply) {
	r.SendMsg(replicaId, r.prepareReplyRPC, reply)
}

func (r *Replica) replyPreAccept(replicaId int32, reply *PreAcceptReply) {
	r.SendMsg(replicaId, r.preAcceptReplyRPC, reply)
}

func (r *Replica) replyAccept(replicaId int32, reply *AcceptReply) {
	r.SendMsg(replicaId, r.acceptReplyRPC, reply)
}

func (r *Replica) replyTryPreAccept(replicaId int32, reply *TryPreAcceptReply) {
	r.SendMsg(replicaId, r.tryPreAcceptReplyRPC, reply)
}

func (r *Replica) bcastPrepare(replica int32, instance int32) {
	defer func() {
		if err := recover(); err != nil {
			r.Println("Prepare bcast failed:", err)
		}
	}()
	lb := r.InstanceSpace[replica][instance].lb
	args := &Prepare{r.Id, replica, instance, lb.lastTriedBallot}

	n := r.N - 1
	q := r.Id
	for sent := 0; sent < n; {
		q = (q + 1) % int32(r.N)
		if q == r.Id {
			break
		}
		if !r.peerAlive(q) {
			continue
		}
		r.SendMsg(q, r.prepareRPC, args)
		sent++
	}

}

func (r *Replica) bcastPreAccept(replica int32, instance int32) {
	defer func() {
		if err := recover(); err != nil {
			r.Println("PreAccept bcast failed:", err)
		}
	}()
	lb := r.InstanceSpace[replica][instance].lb
	pa := new(PreAccept)
	pa.LeaderId = r.Id
	pa.Replica = replica
	pa.Instance = instance
	pa.Ballot = lb.lastTriedBallot
	pa.Command = lb.cmds
	pa.Seq = lb.seq
	pa.Deps = lb.deps

	n := r.N - 1
	if r.Thrifty {
		n = r.Replica.FastQuorumSize() - 1
	}

	sent := 0
	for q := 0; q < r.N-1; q++ {
		if !r.peerAlive(r.PreferredPeerOrder[q]) {
			continue
		}
		r.SendMsg(r.PreferredPeerOrder[q], r.preAcceptRPC, pa)
		sent++
		if sent >= n {
			break
		}
	}
}

func (r *Replica) bcastTryPreAccept(replica int32, instance int32) {
	defer func() {
		if err := recover(); err != nil {
			r.Println("PreAccept bcast failed:", err)
		}
	}()
	lb := r.InstanceSpace[replica][instance].lb
	tpa := new(TryPreAccept)
	tpa.LeaderId = r.Id
	tpa.Replica = replica
	tpa.Instance = instance
	tpa.Ballot = lb.lastTriedBallot
	tpa.Command = lb.cmds
	tpa.Seq = lb.seq
	tpa.Deps = lb.deps

	for q := int32(0); q < int32(r.N); q++ {
		if q == r.Id {
			continue
		}
		if !r.peerAlive(q) {
			continue
		}
		r.SendMsg(q, r.tryPreAcceptRPC, tpa)
	}
}

func (r *Replica) bcastAccept(replica int32, instance int32) {
	defer func() {
		if err := recover(); err != nil {
			r.Println("Accept bcast failed:", err)
		}
	}()

	lb := r.InstanceSpace[replica][instance].lb
	ea := new(Accept)
	ea.LeaderId = r.Id
	ea.Replica = replica
	ea.Instance = instance
	ea.Ballot = lb.lastTriedBallot
	ea.Seq = lb.seq
	ea.Deps = lb.deps
	ea.Command = lb.cmds

	n := r.N - 1
	if r.Thrifty {
		n = r.N / 2
	}

	sent := 0
	for q := 0; q < r.N-1; q++ {
		if !r.peerAlive(r.PreferredPeerOrder[q]) {
			continue
		}
		r.SendMsg(r.PreferredPeerOrder[q], r.acceptRPC, ea)
		sent++
		if sent >= n {
			break
		}
	}
}

func (r *Replica) bcastCommit(replica int32, instance int32) {
	defer func() {
		if err := recover(); err != nil {
			r.Println("Commit bcast failed:", err)
		}
	}()
	lb := r.InstanceSpace[replica][instance].lb
	ec := new(Commit)
	ec.LeaderId = r.Id
	ec.Replica = replica
	ec.Instance = instance
	ec.Command = lb.cmds
	ec.Seq = lb.seq
	ec.Deps = lb.deps
	ec.Ballot = lb.ballot

	for q := 0; q < r.N-1; q++ {
		if !r.peerAlive(r.PreferredPeerOrder[q]) {
			continue
		}
		r.SendMsg(r.PreferredPeerOrder[q], r.commitRPC, ec)
	}
}

func (r *Replica) clearHashtables() {
	for q := 0; q < r.N; q++ {
		r.conflicts[q] = make(map[state.Key]*InstPair, HT_INIT_SIZE)
	}
}

func (r *Replica) updateCommitted(replica int32) {
	r.M.Lock()
	for r.InstanceSpace[replica][r.CommittedUpTo[replica]+1] != nil &&
		(r.InstanceSpace[replica][r.CommittedUpTo[replica]+1].Status == COMMITTED ||
			r.InstanceSpace[replica][r.CommittedUpTo[replica]+1].Status == EXECUTED) {
		r.CommittedUpTo[replica] = r.CommittedUpTo[replica] + 1
	}
	r.M.Unlock()
}

func (r *Replica) updateConflicts(cmds []state.Command, replica int32, instance int32, seq int32) {
	for i := 0; i < len(cmds); i++ {
		if dpair, present := r.conflicts[replica][cmds[i].K]; present {
			if dpair.last < instance {
				r.conflicts[replica][cmds[i].K].last = instance
			}
			if dpair.lastWrite < instance && cmds[i].Op != state.GET {
				r.conflicts[replica][cmds[i].K].lastWrite = instance
			}
		} else {
			r.conflicts[replica][cmds[i].K] = &InstPair{
				last:      instance,
				lastWrite: -1,
			}
			if cmds[i].Op != state.GET {
				r.conflicts[replica][cmds[i].K].lastWrite = instance
			}
		}
		if s, present := r.maxSeqPerKey[cmds[i].K]; present {
			if s < seq {
				r.maxSeqPerKey[cmds[i].K] = seq
			}
		} else {
			r.maxSeqPerKey[cmds[i].K] = seq
		}
	}
}

func (r *Replica) updateAttributes(cmds []state.Command, seq int32, deps []int32, replica int32, instance int32) (int32, []int32, bool) {
	changed := false
	for q := 0; q < r.N; q++ {
		if r.Id != replica && int32(q) == replica {
			continue
		}
		for i := 0; i < len(cmds); i++ {
			if dpair, present := (r.conflicts[q])[cmds[i].K]; present {
				d := dpair.lastWrite
				if cmds[i].Op != state.GET {
					d = dpair.last
				}
				if int32(q) == replica && d >= instance {
					continue
				}

				if d > deps[q] {
					deps[q] = d
					if seq <= r.InstanceSpace[q][d].Seq {
						seq = r.InstanceSpace[q][d].Seq + 1
					}
					changed = true
					break
				}
			}
		}
	}
	for i := 0; i < len(cmds); i++ {
		if s, present := r.maxSeqPerKey[cmds[i].K]; present {
			if seq <= s {
				changed = true
				seq = s + 1
			}
		}
	}

	return seq, deps, changed
}

func (r *Replica) mergeAttributes(seq1 int32, deps1 []int32, seq2 int32, deps2 []int32) (int32, []int32, bool) {
	equal := true
	if seq1 != seq2 {
		equal = false
		if seq2 > seq1 {
			seq1 = seq2
		}
	}
	for q := 0; q < r.N; q++ {
		if deps1[q] != deps2[q] {
			equal = false
			if deps2[q] > deps1[q] {
				deps1[q] = deps2[q]
			}
		}
	}
	return seq1, deps1, equal
}

func equal(deps1 []int32, deps2 []int32) bool {
	if len(deps1) != len(deps2) {
		return false
	}
	for i := 0; i < len(deps1); i++ {
		if deps1[i] != deps2[i] {
			return false
		}
	}
	return true
}

func (r *Replica) handlePropose(propose *defs.GPropose) {
	//TODO!! Handle client retries

	proposalQueue := len(r.ProposeChan)
	batchSize := proposalQueue + 1
	if maxBatch := r.maxBatchCommands(); batchSize > maxBatch {
		batchSize = maxBatch
	}
	r.M.Lock()
	r.Stats.M["totalBatching"]++
	r.Stats.M["totalBatchingSize"] += batchSize
	r.Stats.M["proposedCommands"] += batchSize
	if batchSize > r.Stats.M["maxBatchSize"] {
		r.Stats.M["maxBatchSize"] = batchSize
	}
	if proposalQueue > r.Stats.M["maxProposalQueue"] {
		r.Stats.M["maxProposalQueue"] = proposalQueue
	}
	r.M.Unlock()

	r.crtInstance[r.Id]++

	cmds := make([]state.Command, batchSize)
	proposals := make([]*defs.GPropose, batchSize)
	cmds[0] = propose.Command
	proposals[0] = propose
	for i := 1; i < batchSize; i++ {
		prop := <-r.ProposeChan
		cmds[i] = prop.Command
		proposals[i] = prop
	}

	r.startPhase1(cmds, r.Id, r.crtInstance[r.Id], r.Id, proposals)

	cpcounter += len(cmds)

}

func (r *Replica) startPhase1(cmds []state.Command, replica int32, instance int32, ballot int32, proposals []*defs.GPropose) {
	r.active[instanceId{replica, instance}] = true
	if instance > r.crtInstance[replica] {
		r.crtInstance[replica] = instance
	}
	// init command attributes
	seq := int32(0)
	deps := make([]int32, r.N)
	for q := 0; q < r.N; q++ {
		deps[q] = -1
	}
	deps[replica] = instance - 1
	seq, deps, _ = r.updateAttributes(cmds, seq, deps, replica, instance)
	comDeps := make([]int32, r.N)
	for i := 0; i < r.N; i++ {
		comDeps[i] = -1
	}

	inst := r.newInstance(replica, instance, cmds, ballot, ballot, PREACCEPTED, seq, deps)
	inst.lb = r.newLeaderBookkeeping(proposals, append([]int32(nil), deps...), comDeps, append([]int32(nil), deps...), ballot, cmds, PREACCEPTED, seq)
	r.InstanceSpace[replica][instance] = inst
	inst.lb.preparing = false

	r.updateConflicts(cmds, replica, instance, seq)

	if seq >= r.maxSeq {
		r.maxSeq = seq
	}

	r.recordInstanceMetadata(r.InstanceSpace[replica][instance])
	r.recordCommands(cmds)
	r.sync()

	r.bcastPreAccept(replica, instance)
}

func (r *Replica) handlePreAccept(preAccept *PreAccept) {
	inst := r.InstanceSpace[preAccept.Replica][preAccept.Instance]

	if preAccept.Seq >= r.maxSeq {
		r.maxSeq = preAccept.Seq + 1
	}

	if preAccept.Ballot > r.maxRecvBallot {
		r.maxRecvBallot = preAccept.Ballot
	}

	if preAccept.Instance > r.crtInstance[preAccept.Replica] {
		r.crtInstance[preAccept.Replica] = preAccept.Instance
	}

	if inst == nil {
		inst = r.newInstanceDefault(preAccept.Replica, preAccept.Instance)
		r.InstanceSpace[preAccept.Replica][preAccept.Instance] = inst
	}

	if inst != nil && preAccept.Ballot < inst.bal {
		return
	}

	inst.bal = preAccept.Ballot

	if inst.Status >= ACCEPTED {
		if inst.Cmds == nil {
			r.InstanceSpace[preAccept.Replica][preAccept.Instance].Cmds = preAccept.Command
			r.updateConflicts(preAccept.Command, preAccept.Replica, preAccept.Instance, preAccept.Seq)
			r.recordCommands(preAccept.Command)
			r.sync()
		}

	} else if inst.vbal != preAccept.Ballot || inst.Status == NONE {
		seq, deps, changed := r.updateAttributes(preAccept.Command, preAccept.Seq, preAccept.Deps, preAccept.Replica, preAccept.Instance)
		status := PREACCEPTED_EQ
		if changed {
			status = PREACCEPTED
		}
		inst.Cmds = preAccept.Command
		inst.Seq = seq
		inst.Deps = deps
		inst.bal = preAccept.Ballot
		inst.vbal = preAccept.Ballot
		inst.Status = status

		r.updateConflicts(preAccept.Command, preAccept.Replica, preAccept.Instance, preAccept.Seq)
		r.recordInstanceMetadata(r.InstanceSpace[preAccept.Replica][preAccept.Instance])
		r.recordCommands(preAccept.Command)
		r.sync()

	}

	reply := &PreAcceptReply{
		preAccept.Replica,
		preAccept.Instance,
		inst.bal,
		inst.vbal,
		inst.Seq,
		inst.Deps,
		r.CommittedUpTo,
		inst.Status, r.Id}
	r.replyPreAccept(preAccept.LeaderId, reply)
}

func (r *Replica) handlePreAcceptReply(pareply *PreAcceptReply) {
	inst := r.InstanceSpace[pareply.Replica][pareply.Instance]
	if inst == nil || inst.lb == nil {
		return
	}
	lb := inst.lb
	if lb.preparing || lb.tryingToPreAccept || inst.bal != lb.lastTriedBallot {
		return
	}

	if pareply.Ballot > r.maxRecvBallot {
		r.maxRecvBallot = pareply.Ballot
	}

	if lb.lastTriedBallot > pareply.Ballot {
		return
	}

	if lb.lastTriedBallot < pareply.Ballot {
		lb.nacks++
		if lb.nacks+1 > r.N>>1 {
			if r.IsLeader {
				r.makeBallot(pareply.Replica, pareply.Instance)
				r.bcastPrepare(pareply.Replica, pareply.Instance)
			}
		}
		return
	}

	if lb.status != PREACCEPTED && lb.status != PREACCEPTED_EQ {
		return
	}

	if pareply.AcceptorId < 0 || pareply.AcceptorId >= int32(r.N) || lb.preVoters[pareply.AcceptorId] {
		return
	}
	lb.preVoters[pareply.AcceptorId] = true
	inst.lb.preAcceptOKs++

	if pareply.VBallot > lb.ballot {
		lb.ballot = pareply.VBallot
		lb.seq = pareply.Seq
		lb.deps = pareply.Deps
		lb.status = pareply.Status
	}

	isInitialBallot := isInitialBallot(lb.lastTriedBallot, pareply.Replica, pareply.Instance)

	seq, deps, allEqual := r.mergeAttributes(lb.seq, lb.deps, pareply.Seq, pareply.Deps)
	if r.N <= 3 && r.Thrifty {
		// no need to check for equality
	} else {
		inst.lb.allEqual = inst.lb.allEqual && allEqual
		if !allEqual {
			r.M.Lock()
			r.Stats.M["conflicted"]++
			r.M.Unlock()
		}
	}

	allCommitted := true
	if r.N > 7 {
		for q := 0; q < r.N; q++ {
			if inst.lb.committedDeps[q] < pareply.CommittedDeps[q] {
				inst.lb.committedDeps[q] = pareply.CommittedDeps[q]
			}
			if inst.lb.committedDeps[q] < r.CommittedUpTo[q] {
				inst.lb.committedDeps[q] = r.CommittedUpTo[q]
			}
			if inst.lb.committedDeps[q] < inst.Deps[q] {
				allCommitted = false
			}
		}
	}

	if lb.status <= PREACCEPTED_EQ {
		lb.deps = deps
		lb.seq = seq
	}

	precondition := inst.lb.allEqual && allCommitted && isInitialBallot

	if inst.lb.preAcceptOKs >= (r.Replica.FastQuorumSize()-1) && precondition {
		lb.status = COMMITTED

		inst.Status = lb.status
		inst.bal = lb.ballot
		inst.Cmds = lb.cmds
		inst.Deps = lb.deps
		inst.Seq = lb.seq
		r.recordInstanceMetadata(inst)
		r.sync()

		r.updateCommitted(pareply.Replica)
		r.clearRecovery(pareply.Replica, pareply.Instance)
		if inst.lb.clientProposals != nil && !r.Dreply {
			for i := 0; i < len(inst.lb.clientProposals); i++ {
				r.ReplyProposeTS(
					&defs.ProposeReplyTS{OK: TRUE, CommandId: inst.lb.clientProposals[i].CommandId, Value: state.NIL(), Timestamp: inst.lb.clientProposals[i].Timestamp},
					inst.lb.clientProposals[i].Reply,
					inst.lb.clientProposals[i].Mutex)
				r.M.Lock()
				r.Stats.M["clientReplies"]++
				r.M.Unlock()
			}
		}

		r.bcastCommit(pareply.Replica, pareply.Instance)

		r.M.Lock()
		r.Stats.M["fast"]++
		if inst.proposeTime != 0 {
			r.Stats.M["totalCommitTime"] += int(time.Now().UnixNano() - inst.proposeTime)
		}
		r.M.Unlock()
	} else if inst.lb.preAcceptOKs >= r.N/2 && (!precondition || !r.fastQuorumAvailable()) {
		lb.status = ACCEPTED

		inst.Status = lb.status
		inst.bal = lb.ballot
		inst.Cmds = lb.cmds
		inst.Deps = lb.deps
		inst.Seq = lb.seq
		r.recordInstanceMetadata(inst)
		r.sync()

		r.bcastAccept(pareply.Replica, pareply.Instance)

		r.M.Lock()
		r.Stats.M["slow"]++
		if !allCommitted {
			r.Stats.M["weird"]++
		}
		r.M.Unlock()
	}
}

func (r *Replica) handleAccept(accept *Accept) {
	inst := r.InstanceSpace[accept.Replica][accept.Instance]

	if accept.Ballot > r.maxRecvBallot {
		r.maxRecvBallot = accept.Ballot
	}

	if accept.Instance > r.crtInstance[accept.Replica] {
		r.crtInstance[accept.Replica] = accept.Instance
	}

	if inst == nil {
		inst = r.newInstanceDefault(accept.Replica, accept.Instance)
		r.InstanceSpace[accept.Replica][accept.Instance] = inst
	}

	if accept.Ballot < inst.bal {
		r.Printf("Smaller ballot %d < %d\n", accept.Ballot, inst.bal)
	} else if inst.Status >= COMMITTED {
		r.Printf("Already committed / executed \n")
	} else {
		inst.Deps = accept.Deps
		inst.Seq = accept.Seq
		inst.bal = accept.Ballot
		inst.vbal = accept.Ballot
		inst.Status = ACCEPTED
		inst.Cmds = accept.Command
		r.updateConflicts(inst.Cmds, accept.Replica, accept.Instance, inst.Seq)
		r.recordInstanceMetadata(r.InstanceSpace[accept.Replica][accept.Instance])
		r.sync()
	}

	reply := &AcceptReply{accept.Replica, accept.Instance, inst.bal, r.Id}
	r.replyAccept(accept.LeaderId, reply)

}

func (r *Replica) handleAcceptReply(areply *AcceptReply) {
	inst := r.InstanceSpace[areply.Replica][areply.Instance]
	if inst == nil || inst.lb == nil {
		return
	}
	lb := inst.lb
	if lb.preparing || inst.bal != lb.lastTriedBallot {
		return
	}

	if areply.Ballot > r.maxRecvBallot {
		r.maxRecvBallot = areply.Ballot
	}

	if lb.status != ACCEPTED {
		return
	}

	if lb.lastTriedBallot != areply.Ballot {
		return
	}

	if areply.Ballot > lb.lastTriedBallot {
		lb.nacks++
		if lb.nacks+1 > r.N>>1 {
			if r.IsLeader {
				r.makeBallot(areply.Replica, areply.Instance)
				r.bcastPrepare(areply.Replica, areply.Instance)
			}
		}
		return
	}

	if areply.AcceptorId < 0 || areply.AcceptorId >= int32(r.N) || lb.acceptVoters[areply.AcceptorId] {
		return
	}
	lb.acceptVoters[areply.AcceptorId] = true
	inst.lb.acceptOKs++

	if inst.lb.acceptOKs+1 > r.N/2 {
		lb.status = COMMITTED
		inst.Status = COMMITTED
		r.updateCommitted(areply.Replica)
		r.clearRecovery(areply.Replica, areply.Instance)
		r.recordInstanceMetadata(inst)
		r.sync()

		if inst.lb.clientProposals != nil && !r.Dreply {
			for i := 0; i < len(inst.lb.clientProposals); i++ {
				r.ReplyProposeTS(
					&defs.ProposeReplyTS{OK: TRUE, CommandId: inst.lb.clientProposals[i].CommandId, Value: state.NIL(), Timestamp: inst.lb.clientProposals[i].Timestamp},
					inst.lb.clientProposals[i].Reply,
					inst.lb.clientProposals[i].Mutex)
				r.M.Lock()
				r.Stats.M["clientReplies"]++
				r.M.Unlock()
			}
		}

		r.bcastCommit(areply.Replica, areply.Instance)
		r.M.Lock()
		if inst.proposeTime != 0 {
			r.Stats.M["totalCommitTime"] += int(time.Now().UnixNano() - inst.proposeTime)
		}
		r.M.Unlock()
	}
}

func (r *Replica) handleCommit(commit *Commit) {
	inst := r.InstanceSpace[commit.Replica][commit.Instance]

	if commit.Instance > r.crtInstance[commit.Replica] {
		r.crtInstance[commit.Replica] = commit.Instance
	}

	if commit.Ballot > r.maxRecvBallot {
		r.maxRecvBallot = commit.Ballot
	}

	if inst == nil {
		r.InstanceSpace[commit.Replica][commit.Instance] = r.newInstanceDefault(commit.Replica, commit.Instance)
		inst = r.InstanceSpace[commit.Replica][commit.Instance]
	}

	if inst.Status >= COMMITTED {
		return
	}

	// FIXME timeout on client side?
	if commit.Replica == r.Id {
		if len(commit.Command) == 1 && commit.Command[0].Op == state.NONE && inst.lb != nil && inst.lb.clientProposals != nil {
			for _, p := range inst.lb.clientProposals {
				r.Printf("In %d.%d, re-proposing %s \n", commit.Replica, commit.Instance, p.Command.String())
				r.ProposeChan <- p
			}
			inst.lb.clientProposals = nil
		}
	}

	if inst.bal < commit.Ballot {
		inst.bal = commit.Ballot
	}
	inst.vbal = commit.Ballot
	inst.Cmds = commit.Command
	inst.Seq = commit.Seq
	inst.Deps = commit.Deps
	inst.Status = COMMITTED

	r.updateConflicts(commit.Command, commit.Replica, commit.Instance, commit.Seq)
	r.updateCommitted(commit.Replica)
	r.clearRecovery(commit.Replica, commit.Instance)
	r.recordInstanceMetadata(r.InstanceSpace[commit.Replica][commit.Instance])
	r.recordCommands(commit.Command)

}

/**********************************************************************

                     RECOVERY ACTIONS

***********************************************************************/

func (r *Replica) BeTheLeader(args *defs.BeTheLeaderArgs, reply *defs.BeTheLeaderReply) error {
	r.IsLeader = true
	r.Println("I am the leader")
	return nil
}

func (r *Replica) startRecoveryForInstance(replica int32, instance int32) {
	r.active[instanceId{replica, instance}] = true
	inst := r.InstanceSpace[replica][instance]
	if inst == nil {
		inst = r.newInstanceDefault(replica, instance)
		r.InstanceSpace[replica][instance] = inst
	} else if inst.Status >= COMMITTED && inst.Cmds != nil {
		r.clearRecovery(replica, instance)
		r.Printf("No need to recover %d.%d", replica, instance)
		return
	}

	// no TLA guidance here (some difference with the original implementation)
	var proposals []*defs.GPropose = nil
	if inst.lb != nil {
		proposals = inst.lb.clientProposals
	}
	inst.lb = r.newLeaderBookkeepingDefault()
	lb := inst.lb
	lb.clientProposals = proposals
	lb.ballot = inst.vbal
	lb.seq = inst.Seq
	lb.cmds = inst.Cmds
	lb.deps = inst.Deps
	lb.status = inst.Status
	r.makeBallot(replica, instance)

	inst.bal = lb.lastTriedBallot
	preply := &PrepareReply{
		r.Id,
		replica,
		instance,
		inst.bal,
		inst.vbal,
		inst.Status,
		inst.Cmds,
		inst.Seq,
		inst.Deps}

	lb.prepareVoters[r.Id] = true
	lb.prepareReplies = append(lb.prepareReplies, preply)
	lb.leaderResponded = r.Id == replica

	r.bcastPrepare(replica, instance)
}

func (r *Replica) handlePrepare(prepare *Prepare) {
	inst := r.InstanceSpace[prepare.Replica][prepare.Instance]
	var preply *PrepareReply

	if prepare.Ballot > r.maxRecvBallot {
		r.maxRecvBallot = prepare.Ballot
	}

	if inst == nil {
		r.InstanceSpace[prepare.Replica][prepare.Instance] = r.newInstanceDefault(prepare.Replica, prepare.Instance)
		inst = r.InstanceSpace[prepare.Replica][prepare.Instance]
	}

	if prepare.Ballot < inst.bal {
		r.Printf("Joined higher ballot %d < %d", prepare.Ballot, inst.bal)
	} else if inst.bal < prepare.Ballot {
		r.Printf("Joining ballot %d ", prepare.Ballot)
		inst.bal = prepare.Ballot
	}

	preply = &PrepareReply{
		r.Id,
		prepare.Replica,
		prepare.Instance,
		inst.bal,
		inst.vbal,
		inst.Status,
		inst.Cmds,
		inst.Seq,
		inst.Deps}
	r.replyPrepare(prepare.LeaderId, preply)
}

func (r *Replica) handlePrepareReply(preply *PrepareReply) {
	inst := r.InstanceSpace[preply.Replica][preply.Instance]
	if inst == nil || inst.lb == nil {
		return
	}
	lb := inst.lb
	if inst.bal != lb.lastTriedBallot {
		return
	}

	if preply.Ballot > r.maxRecvBallot {
		r.maxRecvBallot = preply.Ballot
	}

	if inst == nil || lb == nil || !lb.preparing {
		return
	}

	if preply.Ballot != lb.lastTriedBallot {
		lb.nacks++
		return
	}

	if preply.AcceptorId < 0 || preply.AcceptorId >= int32(r.N) || lb.prepareVoters[preply.AcceptorId] {
		return
	}
	lb.prepareVoters[preply.AcceptorId] = true
	lb.prepareReplies = append(lb.prepareReplies, preply)
	if len(lb.prepareReplies) < r.Replica.SlowQuorumSize() {
		return
	}

	lb.preparing = false

	// A promise is not an accepted value. Select the strongest returned
	// evidence independently of the new recovery ballot.
	var chosen *PrepareReply
	for _, reply := range lb.prepareReplies {
		if reply.AcceptorId == preply.Replica {
			lb.leaderResponded = true
		}
		if reply.Status >= COMMITTED {
			chosen = reply
			break
		}
		if reply.Status == NONE {
			continue
		}
		if chosen == nil || reply.VBallot > chosen.VBallot ||
			(reply.VBallot == chosen.VBallot && reply.Status > chosen.Status) {
			chosen = reply
		}
	}
	if chosen == nil {
		r.startPhase1(state.NOOP(), preply.Replica, preply.Instance, lb.lastTriedBallot, lb.clientProposals)
		return
	}
	lb.ballot, lb.cmds, lb.seq, lb.status = chosen.VBallot, chosen.Command, chosen.Seq, chosen.Status
	lb.deps = append([]int32(nil), chosen.Deps...)
	if chosen.Status >= COMMITTED {
		r.handleCommit(&Commit{r.Id, preply.Replica, preply.Instance, chosen.VBallot, chosen.Command, chosen.Seq, chosen.Deps})
		for q := int32(0); q < int32(r.N); q++ {
			if q != r.Id {
				r.SendMsg(q, r.commitRPC, &Commit{r.Id, preply.Replica, preply.Instance, chosen.VBallot, chosen.Command, chosen.Seq, chosen.Deps})
			}
		}
		return
	}
	matching := 0
	allEqual := true
	for _, reply := range lb.prepareReplies {
		if reply.Status != PREACCEPTED && reply.Status != PREACCEPTED_EQ {
			continue
		}
		if reply.VBallot != chosen.VBallot || reply.Seq != chosen.Seq || !equal(reply.Deps, chosen.Deps) || !sameCommands(reply.Command, chosen.Command) {
			allEqual = false
		} else if reply.AcceptorId != preply.Replica {
			matching++
		}
	}
	inst.Cmds, inst.Seq, inst.Deps = lb.cmds, lb.seq, append([]int32(nil), lb.deps...)
	inst.Status, inst.vbal = chosen.Status, chosen.VBallot
	initial := isInitialBallot(chosen.VBallot, preply.Replica, preply.Instance)
	if chosen.Status == ACCEPTED || (initial && allEqual && !lb.leaderResponded && matching >= r.N/2) {
		inst.Status, lb.status = ACCEPTED, ACCEPTED
		inst.vbal, lb.ballot = lb.lastTriedBallot, lb.lastTriedBallot
		r.bcastAccept(preply.Replica, preply.Instance)
	} else if initial && allEqual && !lb.leaderResponded && matching >= (r.F+1)/2 {
		// Original optimized recovery (TentativePreAccept), including its
		// published limitations; this is not the EPaxos* recovery algorithm.
		lb.tryingToPreAccept = true
		lb.preAcceptOKs, lb.tpaReps = 0, 0
		lb.preVoters = make(map[int32]bool)
		r.bcastTryPreAccept(preply.Replica, preply.Instance)
		r.handleTryPreAccept(&TryPreAccept{r.Id, preply.Replica, preply.Instance, lb.lastTriedBallot, lb.cmds, lb.seq, lb.deps})
	} else {
		r.startPhase1(lb.cmds, preply.Replica, preply.Instance, lb.lastTriedBallot, lb.clientProposals)
	}
}

func (r *Replica) handleTryPreAccept(tpa *TryPreAccept) {
	inst := r.InstanceSpace[tpa.Replica][tpa.Instance]

	if inst == nil {
		r.InstanceSpace[tpa.Replica][tpa.Instance] = r.newInstanceDefault(tpa.Replica, tpa.Instance)
		inst = r.InstanceSpace[tpa.Replica][tpa.Instance]
	}

	confRep, confInst := int32(-1), int32(-1)
	confStatus := int8(NONE)
	if inst.bal <= tpa.Ballot {
		inst.bal = tpa.Ballot
		if conflict, cr, ci := r.findPreAcceptConflicts(tpa.Command, tpa.Replica, tpa.Instance, tpa.Seq, tpa.Deps); conflict {
			confRep, confInst = cr, ci
			confStatus = r.InstanceSpace[cr][ci].Status
		} else {
			if tpa.Instance > r.crtInstance[tpa.Replica] {
				r.crtInstance[tpa.Replica] = tpa.Instance
			}
			inst.Cmds, inst.Seq, inst.Deps = tpa.Command, tpa.Seq, append([]int32(nil), tpa.Deps...)
			inst.Status, inst.vbal = PREACCEPTED, tpa.Ballot
			r.updateConflicts(inst.Cmds, tpa.Replica, tpa.Instance, inst.Seq)
		}
	}
	reply := &TryPreAcceptReply{r.Id, tpa.Replica, tpa.Instance, inst.bal, inst.vbal, confRep, confInst, confStatus}
	if tpa.LeaderId == r.Id {
		r.handleTryPreAcceptReply(reply)
	} else {
		r.replyTryPreAccept(tpa.LeaderId, reply)
	}

}

func (r *Replica) findPreAcceptConflicts(cmds []state.Command, replica int32, instance int32, seq int32, deps []int32) (bool, int32, int32) {
	inst := r.InstanceSpace[replica][instance]
	if inst != nil && len(inst.Cmds) > 0 {
		if inst.Status >= ACCEPTED {
			// already ACCEPTED or COMMITTED
			// we consider this a conflict because we shouldn't regress to PRE-ACCEPTED
			return true, replica, instance
		}
		if inst.Seq == seq && equal(inst.Deps, deps) {
			// already PRE-ACCEPTED, no point looking for conflicts again
			return false, replica, instance
		}
	}
	for q := int32(0); q < int32(r.N); q++ {
		for i := r.ExecedUpTo[q]; i <= r.crtInstance[q]; i++ { // FIXME this is not enough imho.
			if i == -1 {
				//do not check placeholder
				continue
			}
			if replica == q && instance == i {
				// no point checking past instance in replica's row, since replica would have
				// set the dependencies correctly for anything started after instance
				break
			}
			if i == deps[q] {
				//the instance cannot be a dependency for itself
				continue
			}
			inst := r.InstanceSpace[q][i]
			if inst == nil || inst.Cmds == nil || len(inst.Cmds) == 0 {
				continue
			}
			if inst.Deps[replica] >= instance {
				// instance q.i depends on instance replica.instance, it is not a conflict
				continue
			}
			if r.LRead || state.ConflictBatch(inst.Cmds, cmds) {
				if i > deps[q] ||
					(i < deps[q] && inst.Seq >= seq && (q != replica || inst.Status > PREACCEPTED_EQ)) {
					// this is a conflict
					return true, q, i
				}
			}
		}
	}
	return false, -1, -1
}

func (r *Replica) handleTryPreAcceptReply(tpar *TryPreAcceptReply) {
	inst := r.InstanceSpace[tpar.Replica][tpar.Instance]

	if tpar.Ballot > r.maxRecvBallot {
		r.maxRecvBallot = tpar.Ballot
	}

	if inst == nil {
		r.InstanceSpace[tpar.Replica][tpar.Instance] = r.newInstanceDefault(tpar.Replica, tpar.Instance)
		inst = r.InstanceSpace[tpar.Replica][tpar.Instance]
	}

	lb := inst.lb
	if lb == nil || !lb.tryingToPreAccept {
		return
	}

	if tpar.Ballot != lb.lastTriedBallot {
		return
	}

	if tpar.AcceptorId < 0 || tpar.AcceptorId >= int32(r.N) || lb.preVoters[tpar.AcceptorId] {
		return
	}
	lb.preVoters[tpar.AcceptorId] = true
	lb.tpaReps++

	if tpar.VBallot == lb.lastTriedBallot && tpar.ConflictReplica < 0 {
		lb.preAcceptOKs++
		if lb.preAcceptOKs >= r.Replica.SlowQuorumSize() {
			//it's safe to start Accept phase
			lb.status = ACCEPTED
			lb.tryingToPreAccept = false
			lb.acceptOKs = 0

			inst.Cmds = lb.cmds
			inst.Seq = lb.seq
			inst.Deps = lb.deps
			inst.Status = lb.status
			inst.vbal = lb.lastTriedBallot
			inst.bal = lb.lastTriedBallot

			r.bcastAccept(tpar.Replica, tpar.Instance)
			return
		}
	} else {
		lb.nacks++
		lb.possibleQuorum[tpar.AcceptorId] = false
		if tpar.ConflictReplica >= 0 && tpar.ConflictReplica < int32(r.N) {
			lb.possibleQuorum[tpar.ConflictReplica] = false
		}
	}

	lb.tpaAccepted = lb.tpaAccepted || (tpar.ConflictStatus >= ACCEPTED) // TLA spec. (page 39)

	if lb.tpaReps >= r.Replica.SlowQuorumSize()-1 && lb.tpaAccepted {
		//abandon recovery, restart from phase 1
		lb.tryingToPreAccept = false
		r.startPhase1(lb.cmds, tpar.Replica, tpar.Instance, lb.lastTriedBallot, lb.clientProposals)
		return
	}

	// the code below is not checked in TLA (liveness)
	notInQuorum := 0
	for q := 0; q < r.N; q++ {
		if !lb.possibleQuorum[q] {
			notInQuorum++
		}
	}

	if notInQuorum == r.N/2 {
		//this is to prevent defer cycles
		if present, dq, _ := deferredByInstance(tpar.Replica, tpar.Instance); present {
			if lb.possibleQuorum[dq] {
				//an instance whose leader must have been in this instance's quorum has been deferred for this instance => contradiction
				//abandon recovery, restart from phase 1
				lb.tryingToPreAccept = false
				r.makeBallot(tpar.Replica, tpar.Instance)
				r.startPhase1(lb.cmds, tpar.Replica, tpar.Instance, lb.lastTriedBallot, lb.clientProposals)
				return
			}
		}
	}

	if lb.tpaReps >= r.Replica.SlowQuorumSize() && tpar.ConflictReplica >= 0 {
		//defer recovery and update deferred information
		updateDeferred(tpar.Replica, tpar.Instance, tpar.ConflictReplica, tpar.ConflictInstance)
		lb.tryingToPreAccept = false
	}
}

// helper functions and structures to prevent defer cycles while recovering

var deferMap = make(map[uint64]uint64)

func updateDeferred(dr int32, di int32, r int32, i int32) {
	daux := (uint64(dr) << 32) | uint64(di)
	aux := (uint64(r) << 32) | uint64(i)
	deferMap[aux] = daux
}

func deferredByInstance(q int32, i int32) (bool, int32, int32) {
	aux := (uint64(q) << 32) | uint64(i)
	daux, present := deferMap[aux]
	if !present {
		return false, 0, 0
	}
	dq := int32(daux >> 32)
	di := int32(daux)
	return true, dq, di
}

func (r *Replica) newInstanceDefault(replica int32, instance int32) *Instance {
	return r.newInstance(replica, instance, nil, -1, -1, NONE, -1, nil)
}

func (r *Replica) newInstance(replica int32, instance int32, cmds []state.Command, cballot int32, lballot int32, status int8, seq int32, deps []int32) *Instance {
	return &Instance{cmds, cballot, lballot, status, seq, deps, nil, 0, 0, false, nil, time.Now().UnixNano(), &instanceId{replica, instance}}
}

func (r *Replica) newLeaderBookkeepingDefault() *LeaderBookkeeping {
	return r.newLeaderBookkeeping(nil, r.newNilDeps(), r.newNilDeps(), r.newNilDeps(), 0, nil, NONE, -1)
}

func (r *Replica) newLeaderBookkeeping(p []*defs.GPropose, originalDeps []int32, committedDeps []int32, deps []int32, lastTriedBallot int32, cmds []state.Command, status int8, seq int32) *LeaderBookkeeping {
	return &LeaderBookkeeping{clientProposals: p, ballot: lastTriedBallot, allEqual: true, originalDeps: originalDeps, committedDeps: committedDeps, preparing: true, possibleQuorum: allPossible(r.N), lastTriedBallot: lastTriedBallot, cmds: cmds, status: status, seq: seq, deps: deps, preVoters: map[int32]bool{r.Id: true}, acceptVoters: map[int32]bool{r.Id: true}, prepareVoters: make(map[int32]bool), phaseStarted: time.Now()}
}

func (r *Replica) newNilDeps() []int32 {
	nildeps := make([]int32, r.N)
	for i := 0; i < r.N; i++ {
		nildeps[i] = -1
	}
	return nildeps
}
