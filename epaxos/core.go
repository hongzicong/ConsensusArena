package epaxos

// Protocol state and normal-case transitions.
import (
	"github.com/hongzicong/ConsensusArena/dlog"
	"github.com/hongzicong/ConsensusArena/protocol"
	"github.com/hongzicong/ConsensusArena/replica/defs"
	"github.com/hongzicong/ConsensusArena/replicaset"
	fastrpc "github.com/hongzicong/ConsensusArena/rpc"
	"github.com/hongzicong/ConsensusArena/state"
	"sync"
	"time"
)

// corePorts supplies synchronous I/O ports and shared runtime observations.
// It owns no sockets or state-machine storage; callbacks preserve existing ordering.
type corePorts struct {
	n, f int
	id   int32
	*dlog.Logger
	m     *sync.Mutex
	stats *defs.Stats
	*rpcCodes
	send                   func(int32, uint8, fastrpc.Serializable)
	broadcast              func([]int32, int, uint8, fastrpc.Serializable)
	replyResult            func(*proposal, state.Value, int) error
	requeueProposal        func(*proposal)
	peerAlive              func(int32) bool
	preferredPeerOrder     func() []int32
	executeEnabled         func() bool
	replyAfterExecution    func() bool
	recordInstanceMetadata func(*Instance)
	recordCommands         func([]state.Command)
	sync                   func()
	replyBeacon            func(*defs.GBeacon)
	beaconTick             func()
}

// core owns protocol state, ordering, recovery decisions and completion semantics.
type core struct {
	corePorts
	instanceSpace      [][]*Instance
	crtInstance        []int32
	committedUpTo      []int32
	executedUpTo       []int32
	scheduler          *executionScheduler
	conflicts          []map[state.Key]*InstPair
	maxSeqPerKey       map[state.Key]int32
	maxSeq             int32
	latestCPReplica    int32
	latestCPInstance   int32
	instancesToRecover chan *instanceID
	isLeader           bool
	maxRecvBallot      int32
	batchWait          int
	transconf          bool
	recoveryMu         sync.Mutex
	recoveryAttempts   map[uint64]recoveryAttempt
	deferMap           map[uint64]uint64
	*repairState
	transportState
	thrifty bool
	execute func(state.Command) state.Value
}

func newCore(ports corePorts, batchWait int, transconf bool) *core {
	c := &core{
		corePorts:          ports,
		instanceSpace:      make([][]*Instance, ports.n),
		crtInstance:        make([]int32, ports.n),
		committedUpTo:      make([]int32, ports.n),
		executedUpTo:       make([]int32, ports.n),
		conflicts:          make([]map[state.Key]*InstPair, ports.n),
		maxSeqPerKey:       make(map[state.Key]int32),
		latestCPInstance:   -1,
		instancesToRecover: make(chan *instanceID, defs.CHAN_BUFFER_SIZE),
		maxRecvBallot:      -1,
		batchWait:          batchWait,
		transconf:          transconf,
		recoveryAttempts:   make(map[uint64]recoveryAttempt),
		deferMap:           make(map[uint64]uint64),
		repairState:        newRepairState(),
		transportState:     newTransportState(int(ports.id), ports.n),
		thrifty:            true,
	}
	for i := 0; i < c.n; i++ {
		c.instanceSpace[i] = make([]*Instance, MAX_INSTANCE)
		c.crtInstance[i] = -1
		c.executedUpTo[i] = -1
		c.committedUpTo[i] = -1
		c.conflicts[i] = make(map[state.Key]*InstPair, HT_INIT_SIZE)
	}
	c.scheduler = &executionScheduler{core: c, stack: make([]*Instance, 0, 100)}
	return c
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
	id             *instanceID
}

type instanceID struct {
	replica  int32
	instance int32
}

type LeaderBookkeeping struct {
	clientProposals                        []*proposal
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
	preVoters, acceptVoters, prepareVoters replicaset.Set
	phaseStarted                           time.Time
}

func (c *core) BatchingEnabled() bool {
	return c.batchWait > 0
}

func (c *core) maxBatchCommands() int {
	if !c.BatchingEnabled() {
		return 1
	}
	return MAX_BATCH
}

func isInitialBallot(ballot int32, replica int32, instance int32) bool {
	return ballot == replica
}

func (c *core) makeBallot(replica int32, instance int32) {
	inst := c.instanceSpace[replica][instance]
	high := inst.bal
	if c.maxRecvBallot > high {
		high = c.maxRecvBallot
	}
	inst.lb.lastTriedBallot = (high/int32(c.n)+1)*int32(c.n) + c.id
}

func (c *core) updateCommitted(replica int32) {
	c.m.Lock()
	for c.instanceSpace[replica][c.committedUpTo[replica]+1] != nil &&
		(c.instanceSpace[replica][c.committedUpTo[replica]+1].Status == COMMITTED ||
			c.instanceSpace[replica][c.committedUpTo[replica]+1].Status == EXECUTED) {
		c.committedUpTo[replica] = c.committedUpTo[replica] + 1
	}
	c.m.Unlock()
}

func (c *core) updateConflicts(cmds []state.Command, replica int32, instance int32, seq int32) {
	for i := 0; i < len(cmds); i++ {
		if dpair, present := c.conflicts[replica][cmds[i].K]; present {
			if dpair.last < instance {
				c.conflicts[replica][cmds[i].K].last = instance
			}
			if dpair.lastWrite < instance && cmds[i].Op != state.GET {
				c.conflicts[replica][cmds[i].K].lastWrite = instance
			}
		} else {
			c.conflicts[replica][cmds[i].K] = &InstPair{
				last:      instance,
				lastWrite: -1,
			}
			if cmds[i].Op != state.GET {
				c.conflicts[replica][cmds[i].K].lastWrite = instance
			}
		}
		if s, present := c.maxSeqPerKey[cmds[i].K]; present {
			if s < seq {
				c.maxSeqPerKey[cmds[i].K] = seq
			}
		} else {
			c.maxSeqPerKey[cmds[i].K] = seq
		}
	}
}

func (c *core) updateAttributes(cmds []state.Command, seq int32, deps []int32, replica int32, instance int32) (int32, []int32, bool) {
	changed := false
	for q := 0; q < c.n; q++ {
		if c.id != replica && int32(q) == replica {
			continue
		}
		for i := 0; i < len(cmds); i++ {
			if dpair, present := (c.conflicts[q])[cmds[i].K]; present {
				d := dpair.lastWrite
				if cmds[i].Op != state.GET {
					d = dpair.last
				}
				if int32(q) == replica && d >= instance {
					continue
				}

				if d > deps[q] {
					deps[q] = d
					if seq <= c.instanceSpace[q][d].Seq {
						seq = c.instanceSpace[q][d].Seq + 1
					}
					changed = true
					break
				}
			}
		}
	}
	for i := 0; i < len(cmds); i++ {
		if s, present := c.maxSeqPerKey[cmds[i].K]; present {
			if seq <= s {
				changed = true
				seq = s + 1
			}
		}
	}

	return seq, deps, changed
}

func (c *core) mergeAttributes(seq1 int32, deps1 []int32, seq2 int32, deps2 []int32) (int32, []int32, bool) {
	equal := true
	if seq1 != seq2 {
		equal = false
		if seq2 > seq1 {
			seq1 = seq2
		}
	}
	for q := 0; q < c.n; q++ {
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

func (c *core) beginProposalBatch(proposalQueue int) int {
	batchSize := proposalQueue + 1
	if maxBatch := c.maxBatchCommands(); batchSize > maxBatch {
		batchSize = maxBatch
	}
	c.m.Lock()
	c.stats.M["totalBatching"]++
	c.stats.M["totalBatchingSize"] += batchSize
	c.stats.M["proposedCommands"] += batchSize
	if batchSize > c.stats.M["maxBatchSize"] {
		c.stats.M["maxBatchSize"] = batchSize
	}
	if proposalQueue > c.stats.M["maxProposalQueue"] {
		c.stats.M["maxProposalQueue"] = proposalQueue
	}
	c.m.Unlock()

	c.crtInstance[c.id]++

	return batchSize
}

func (c *core) submit(proposals []*proposal) {
	cmds := make([]state.Command, len(proposals))
	for i, p := range proposals {
		cmds[i] = p.Command
	}
	c.startPhase1(cmds, c.id, c.crtInstance[c.id], c.id, proposals)
}

func (c *core) startPhase1(cmds []state.Command, replica int32, instance int32, ballot int32, proposals []*proposal) {
	c.active[instanceID{replica, instance}] = true
	if instance > c.crtInstance[replica] {
		c.crtInstance[replica] = instance
	}
	// init command attributes
	seq := int32(0)
	deps := make([]int32, c.n)
	for q := 0; q < c.n; q++ {
		deps[q] = -1
	}
	deps[replica] = instance - 1
	seq, deps, _ = c.updateAttributes(cmds, seq, deps, replica, instance)
	comDeps := make([]int32, c.n)
	for i := 0; i < c.n; i++ {
		comDeps[i] = -1
	}

	inst := c.newInstance(replica, instance, cmds, ballot, ballot, PREACCEPTED, seq, deps)
	inst.lb = c.newLeaderBookkeeping(proposals, append([]int32(nil), deps...), comDeps, append([]int32(nil), deps...), ballot, cmds, PREACCEPTED, seq)
	c.instanceSpace[replica][instance] = inst
	inst.lb.preparing = false

	c.updateConflicts(cmds, replica, instance, seq)

	if seq >= c.maxSeq {
		c.maxSeq = seq
	}

	c.recordInstanceMetadata(c.instanceSpace[replica][instance])
	c.recordCommands(cmds)
	c.sync()

	c.bcastPreAccept(replica, instance)
}

func (c *core) handlePreAccept(preAccept *PreAccept) {
	inst := c.instanceSpace[preAccept.Replica][preAccept.Instance]

	if preAccept.Seq >= c.maxSeq {
		c.maxSeq = preAccept.Seq + 1
	}

	if preAccept.Ballot > c.maxRecvBallot {
		c.maxRecvBallot = preAccept.Ballot
	}

	if preAccept.Instance > c.crtInstance[preAccept.Replica] {
		c.crtInstance[preAccept.Replica] = preAccept.Instance
	}

	if inst == nil {
		inst = c.newInstanceDefault(preAccept.Replica, preAccept.Instance)
		c.instanceSpace[preAccept.Replica][preAccept.Instance] = inst
	}

	if inst != nil && preAccept.Ballot < inst.bal {
		return
	}

	inst.bal = preAccept.Ballot

	if inst.Status >= ACCEPTED {
		if inst.Cmds == nil {
			c.instanceSpace[preAccept.Replica][preAccept.Instance].Cmds = preAccept.Command
			c.updateConflicts(preAccept.Command, preAccept.Replica, preAccept.Instance, preAccept.Seq)
			c.recordCommands(preAccept.Command)
			c.sync()
		}

	} else if inst.vbal != preAccept.Ballot || inst.Status == NONE {
		seq, deps, changed := c.updateAttributes(preAccept.Command, preAccept.Seq, preAccept.Deps, preAccept.Replica, preAccept.Instance)
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

		c.updateConflicts(preAccept.Command, preAccept.Replica, preAccept.Instance, preAccept.Seq)
		c.recordInstanceMetadata(c.instanceSpace[preAccept.Replica][preAccept.Instance])
		c.recordCommands(preAccept.Command)
		c.sync()

	}

	reply := &PreAcceptReply{
		preAccept.Replica,
		preAccept.Instance,
		inst.bal,
		inst.vbal,
		inst.Seq,
		inst.Deps,
		c.committedUpTo,
		inst.Status, c.id}
	c.send(preAccept.LeaderId, c.preAcceptReplyRPC, reply)
}

func (c *core) handlePreAcceptReply(pareply *PreAcceptReply) {
	inst := c.instanceSpace[pareply.Replica][pareply.Instance]
	if inst == nil || inst.lb == nil {
		return
	}
	lb := inst.lb
	if lb.preparing || lb.tryingToPreAccept || inst.bal != lb.lastTriedBallot {
		return
	}

	if pareply.Ballot > c.maxRecvBallot {
		c.maxRecvBallot = pareply.Ballot
	}

	if lb.lastTriedBallot > pareply.Ballot {
		return
	}

	if lb.lastTriedBallot < pareply.Ballot {
		lb.nacks++
		if lb.nacks+1 > c.n>>1 {
			if c.isLeader {
				c.makeBallot(pareply.Replica, pareply.Instance)
				c.bcastPrepare(pareply.Replica, pareply.Instance)
			}
		}
		return
	}

	if lb.status != PREACCEPTED && lb.status != PREACCEPTED_EQ {
		return
	}

	if pareply.AcceptorId < 0 || pareply.AcceptorId >= int32(c.n) || !lb.preVoters.Add(int(pareply.AcceptorId)) {
		return
	}
	inst.lb.preAcceptOKs++

	if pareply.VBallot > lb.ballot {
		lb.ballot = pareply.VBallot
		lb.seq = pareply.Seq
		lb.deps = pareply.Deps
		lb.status = pareply.Status
	}

	isInitialBallot := isInitialBallot(lb.lastTriedBallot, pareply.Replica, pareply.Instance)

	seq, deps, allEqual := c.mergeAttributes(lb.seq, lb.deps, pareply.Seq, pareply.Deps)
	if c.n <= 3 && c.thrifty {
		// no need to check for equality
	} else {
		inst.lb.allEqual = inst.lb.allEqual && allEqual
		if !allEqual {
			c.m.Lock()
			c.stats.M["conflicted"]++
			c.m.Unlock()
		}
	}

	allCommitted := true
	if c.n > 7 {
		for q := 0; q < c.n; q++ {
			if inst.lb.committedDeps[q] < pareply.CommittedDeps[q] {
				inst.lb.committedDeps[q] = pareply.CommittedDeps[q]
			}
			if inst.lb.committedDeps[q] < c.committedUpTo[q] {
				inst.lb.committedDeps[q] = c.committedUpTo[q]
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

	if inst.lb.preAcceptOKs >= (c.FastQuorumSize()-1) && precondition {
		lb.status = COMMITTED

		inst.Status = lb.status
		inst.bal = lb.ballot
		inst.Cmds = lb.cmds
		inst.Deps = lb.deps
		inst.Seq = lb.seq
		c.recordInstanceMetadata(inst)
		c.sync()

		c.updateCommitted(pareply.Replica)
		c.clearRecovery(pareply.Replica, pareply.Instance)
		if inst.lb.clientProposals != nil && !c.replyAfterExecution() {
			for i := 0; i < len(inst.lb.clientProposals); i++ {
				_ = c.replyResult(inst.lb.clientProposals[i], state.NIL(), -1)
				c.m.Lock()
				c.stats.M["clientReplies"]++
				c.m.Unlock()
			}
		}

		c.bcastCommit(pareply.Replica, pareply.Instance)

		c.m.Lock()
		c.stats.M["fast"]++
		if inst.proposeTime != 0 {
			c.stats.M["totalCommitTime"] += int(time.Now().UnixNano() - inst.proposeTime)
		}
		c.m.Unlock()
	} else if inst.lb.preAcceptOKs >= c.n/2 && (!precondition || !c.fastQuorumAvailable()) {
		lb.status = ACCEPTED

		inst.Status = lb.status
		inst.bal = lb.ballot
		inst.Cmds = lb.cmds
		inst.Deps = lb.deps
		inst.Seq = lb.seq
		c.recordInstanceMetadata(inst)
		c.sync()

		c.bcastAccept(pareply.Replica, pareply.Instance)

		c.m.Lock()
		c.stats.M["slow"]++
		if !allCommitted {
			c.stats.M["weird"]++
		}
		c.m.Unlock()
	}
}

func (c *core) handleAccept(accept *Accept) {
	inst := c.instanceSpace[accept.Replica][accept.Instance]

	if accept.Ballot > c.maxRecvBallot {
		c.maxRecvBallot = accept.Ballot
	}

	if accept.Instance > c.crtInstance[accept.Replica] {
		c.crtInstance[accept.Replica] = accept.Instance
	}

	if inst == nil {
		inst = c.newInstanceDefault(accept.Replica, accept.Instance)
		c.instanceSpace[accept.Replica][accept.Instance] = inst
	}

	if accept.Ballot < inst.bal {
		c.Printf("Smaller ballot %d < %d\n", accept.Ballot, inst.bal)
	} else if inst.Status >= COMMITTED {
		c.Printf("Already committed / executed \n")
	} else {
		inst.Deps = accept.Deps
		inst.Seq = accept.Seq
		inst.bal = accept.Ballot
		inst.vbal = accept.Ballot
		inst.Status = ACCEPTED
		inst.Cmds = accept.Command
		c.updateConflicts(inst.Cmds, accept.Replica, accept.Instance, inst.Seq)
		c.recordInstanceMetadata(c.instanceSpace[accept.Replica][accept.Instance])
		c.sync()
	}

	reply := &AcceptReply{accept.Replica, accept.Instance, inst.bal, c.id}
	c.send(accept.LeaderId, c.acceptReplyRPC, reply)

}

func (c *core) handleAcceptReply(areply *AcceptReply) {
	inst := c.instanceSpace[areply.Replica][areply.Instance]
	if inst == nil || inst.lb == nil {
		return
	}
	lb := inst.lb
	if lb.preparing || inst.bal != lb.lastTriedBallot {
		return
	}

	if areply.Ballot > c.maxRecvBallot {
		c.maxRecvBallot = areply.Ballot
	}

	if lb.status != ACCEPTED {
		return
	}

	if lb.lastTriedBallot != areply.Ballot {
		return
	}

	if areply.Ballot > lb.lastTriedBallot {
		lb.nacks++
		if lb.nacks+1 > c.n>>1 {
			if c.isLeader {
				c.makeBallot(areply.Replica, areply.Instance)
				c.bcastPrepare(areply.Replica, areply.Instance)
			}
		}
		return
	}

	if areply.AcceptorId < 0 || areply.AcceptorId >= int32(c.n) || !lb.acceptVoters.Add(int(areply.AcceptorId)) {
		return
	}
	inst.lb.acceptOKs++

	if inst.lb.acceptOKs+1 > c.n/2 {
		lb.status = COMMITTED
		inst.Status = COMMITTED
		c.updateCommitted(areply.Replica)
		c.clearRecovery(areply.Replica, areply.Instance)
		c.recordInstanceMetadata(inst)
		c.sync()

		if inst.lb.clientProposals != nil && !c.replyAfterExecution() {
			for i := 0; i < len(inst.lb.clientProposals); i++ {
				_ = c.replyResult(inst.lb.clientProposals[i], state.NIL(), -1)
				c.m.Lock()
				c.stats.M["clientReplies"]++
				c.m.Unlock()
			}
		}

		c.bcastCommit(areply.Replica, areply.Instance)
		c.m.Lock()
		if inst.proposeTime != 0 {
			c.stats.M["totalCommitTime"] += int(time.Now().UnixNano() - inst.proposeTime)
		}
		c.m.Unlock()
	}
}

func (c *core) handleCommit(commit *Commit) {
	inst := c.instanceSpace[commit.Replica][commit.Instance]

	if commit.Instance > c.crtInstance[commit.Replica] {
		c.crtInstance[commit.Replica] = commit.Instance
	}

	if commit.Ballot > c.maxRecvBallot {
		c.maxRecvBallot = commit.Ballot
	}

	if inst == nil {
		c.instanceSpace[commit.Replica][commit.Instance] = c.newInstanceDefault(commit.Replica, commit.Instance)
		inst = c.instanceSpace[commit.Replica][commit.Instance]
	}

	if inst.Status >= COMMITTED {
		return
	}

	// FIXME timeout on client side?
	if commit.Replica == c.id {
		if len(commit.Command) == 1 && commit.Command[0].Op == state.NONE && inst.lb != nil && inst.lb.clientProposals != nil {
			for _, p := range inst.lb.clientProposals {
				c.Printf("In %d.%d, re-proposing %s \n", commit.Replica, commit.Instance, p.Command.String())
				c.requeueProposal(p)
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

	c.updateConflicts(commit.Command, commit.Replica, commit.Instance, commit.Seq)
	c.updateCommitted(commit.Replica)
	c.clearRecovery(commit.Replica, commit.Instance)
	c.recordInstanceMetadata(c.instanceSpace[commit.Replica][commit.Instance])
	c.recordCommands(commit.Command)

}

func (c *core) newInstanceDefault(replica int32, instance int32) *Instance {
	return c.newInstance(replica, instance, nil, -1, -1, NONE, -1, nil)
}

func (c *core) newInstance(replica int32, instance int32, cmds []state.Command, cballot int32, lballot int32, status int8, seq int32, deps []int32) *Instance {
	return &Instance{cmds, cballot, lballot, status, seq, deps, nil, 0, 0, false, nil, time.Now().UnixNano(), &instanceID{replica, instance}}
}

func (c *core) newLeaderBookkeepingDefault() *LeaderBookkeeping {
	return c.newLeaderBookkeeping(nil, c.newNilDeps(), c.newNilDeps(), c.newNilDeps(), 0, nil, NONE, -1)
}

func (c *core) newLeaderBookkeeping(p []*proposal, originalDeps []int32, committedDeps []int32, deps []int32, lastTriedBallot int32, cmds []state.Command, status int8, seq int32) *LeaderBookkeeping {
	return &LeaderBookkeeping{clientProposals: p, ballot: lastTriedBallot, allEqual: true, originalDeps: originalDeps, committedDeps: committedDeps, preparing: true, possibleQuorum: allPossible(c.n), lastTriedBallot: lastTriedBallot, cmds: cmds, status: status, seq: seq, deps: deps, preVoters: replicaset.New(int(c.id)), acceptVoters: replicaset.New(int(c.id)), prepareVoters: replicaset.New(), phaseStarted: time.Now()}
}

func (c *core) newNilDeps() []int32 {
	nildeps := make([]int32, c.n)
	for i := 0; i < c.n; i++ {
		nildeps[i] = -1
	}
	return nildeps
}

// Legacy persistence hooks; inactive when Durable is false.

func (c *core) fastQuorumAvailable() bool {
	live := 0
	for id := int32(0); id < int32(c.n); id++ {
		if c.peerAlive(id) {
			live++
		}
	}
	return live >= c.FastQuorumSize()
}

func (c *core) FastQuorumSize() int {
	return c.f + (c.f+1)/2
}

func (c *core) SlowQuorumSize() int {
	return (c.n + 1) / 2
}

func (c *core) recoverBroadcast(phase string) {
	if err := recover(); err != nil {
		c.Println(phase+" bcast failed:", err)
	}
}

func (c *core) bcastPrepare(replica int32, instance int32) {
	defer c.recoverBroadcast("Prepare")
	lb := c.instanceSpace[replica][instance].lb
	msg := &Prepare{LeaderId: c.id, Replica: replica, Instance: instance, Ballot: lb.lastTriedBallot}
	c.broadcast(c.cyclicPeers, c.n-1, c.prepareRPC, msg)
}

func (c *core) bcastPreAccept(replica int32, instance int32) {
	defer c.recoverBroadcast("PreAccept")
	lb := c.instanceSpace[replica][instance].lb
	msg := &PreAccept{
		LeaderId: c.id, Replica: replica, Instance: instance,
		Ballot: lb.lastTriedBallot, Command: lb.cmds, Seq: lb.seq, Deps: lb.deps,
	}
	limit := c.n - 1
	if c.thrifty {
		limit = c.FastQuorumSize() - 1
	}
	c.broadcast(c.preferredPeerOrder()[:c.n-1], limit, c.preAcceptRPC, msg)
}

func (c *core) bcastTryPreAccept(replica int32, instance int32) {
	defer c.recoverBroadcast("PreAccept")
	lb := c.instanceSpace[replica][instance].lb
	msg := &TryPreAccept{
		LeaderId: c.id, Replica: replica, Instance: instance,
		Ballot: lb.lastTriedBallot, Command: lb.cmds, Seq: lb.seq, Deps: lb.deps,
	}
	c.broadcast(nil, c.n-1, c.tryPreAcceptRPC, msg)
}

func (c *core) bcastAccept(replica int32, instance int32) {
	defer c.recoverBroadcast("Accept")
	lb := c.instanceSpace[replica][instance].lb
	msg := &Accept{
		LeaderId: c.id, Replica: replica, Instance: instance,
		Ballot: lb.lastTriedBallot, Seq: lb.seq, Deps: lb.deps, Command: lb.cmds,
	}
	limit := c.n - 1
	if c.thrifty {
		limit = c.n / 2
	}
	c.broadcast(c.preferredPeerOrder()[:c.n-1], limit, c.acceptRPC, msg)
}

func (c *core) bcastCommit(replica int32, instance int32) {
	defer c.recoverBroadcast("Commit")
	lb := c.instanceSpace[replica][instance].lb
	msg := &Commit{
		LeaderId: c.id, Replica: replica, Instance: instance,
		Ballot: lb.ballot, Command: lb.cmds, Seq: lb.seq, Deps: lb.deps,
	}
	c.broadcast(c.preferredPeerOrder()[:c.n-1], c.n-1, c.commitRPC, msg)
}

func (c *core) receive(event any, _ time.Time) error {
	switch m := event.(type) {
	case *Prepare:
		if m == nil {
			return protocol.UnsupportedEvent("epaxos", event)
		}
		c.handlePrepare(m)
	case *PreAccept:
		if m == nil {
			return protocol.UnsupportedEvent("epaxos", event)
		}
		c.handlePreAccept(m)
	case *Accept:
		if m == nil {
			return protocol.UnsupportedEvent("epaxos", event)
		}
		c.handleAccept(m)
	case *Commit:
		if m == nil {
			return protocol.UnsupportedEvent("epaxos", event)
		}
		c.handleCommit(m)
	case *PrepareReply:
		if m == nil {
			return protocol.UnsupportedEvent("epaxos", event)
		}
		c.handlePrepareReply(m)
	case *PreAcceptReply:
		if m == nil {
			return protocol.UnsupportedEvent("epaxos", event)
		}
		c.handlePreAcceptReply(m)
	case *AcceptReply:
		if m == nil {
			return protocol.UnsupportedEvent("epaxos", event)
		}
		c.handleAcceptReply(m)
	case *TryPreAccept:
		if m == nil {
			return protocol.UnsupportedEvent("epaxos", event)
		}
		c.handleTryPreAccept(m)
	case *TryPreAcceptReply:
		if m == nil {
			return protocol.UnsupportedEvent("epaxos", event)
		}
		c.handleTryPreAcceptReply(m)
	case *repairRequest:
		if m == nil {
			return protocol.UnsupportedEvent("epaxos", event)
		}
		c.handleRepairRequest(m)
	case *defs.GBeacon:
		if m == nil {
			return protocol.UnsupportedEvent("epaxos", event)
		}
		c.replyBeacon(m)
	case *instanceID:
		if m == nil {
			return protocol.UnsupportedEvent("epaxos", event)
		}
		c.startRecoveryForInstance(m.replica, m.instance)
	case leaderCall:
		c.handleLeaderCall()
		m.done <- struct{}{}
	default:
		return protocol.UnsupportedEvent("epaxos", event)
	}
	return nil
}

func (c *core) tick(t protocol.Tick) error {
	switch t.Kind {
	case protocol.Maintenance:
		c.repairTick(t.Now)
		if c.executeEnabled() {
			c.apply(t.Now)
		}
	case protocol.Slow:
		c.beaconTick()
	default:
		return protocol.UnsupportedTimer("epaxos", t.Kind)
	}
	return nil
}
