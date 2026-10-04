package epaxos

// Protocol state and normal-case transitions.
import (
	"encoding/binary"
	"io"
	"time"

	"github.com/hongzicong/ConsensusArena/replica/defs"
	"github.com/hongzicong/ConsensusArena/replicaset"
	"github.com/hongzicong/ConsensusArena/state"
)

var cpMarker []state.Command

var cpcounter = 0

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
	preVoters, acceptVoters, prepareVoters replicaset.Set
	phaseStarted                           time.Time
}

var fastClockChan chan bool

var slowClockChan chan bool

func (r *Replica) BatchingEnabled() bool {
	return r.batchWait > 0
}

func (r *Replica) maxBatchCommands() int {
	if !r.BatchingEnabled() {
		return 1
	}
	return MAX_BATCH
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
	r.Send(preAccept.LeaderId, r.preAcceptReplyRPC, reply)
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

	if pareply.AcceptorId < 0 || pareply.AcceptorId >= int32(r.N) || !lb.preVoters.Add(int(pareply.AcceptorId)) {
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
				_ = r.ReplyResult(inst.lb.clientProposals[i], state.NIL(), -1)
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
	r.Send(accept.LeaderId, r.acceptReplyRPC, reply)

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

	if areply.AcceptorId < 0 || areply.AcceptorId >= int32(r.N) || !lb.acceptVoters.Add(int(areply.AcceptorId)) {
		return
	}
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
				_ = r.ReplyResult(inst.lb.clientProposals[i], state.NIL(), -1)
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

var deferMap = make(map[uint64]uint64)

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
	return &LeaderBookkeeping{clientProposals: p, ballot: lastTriedBallot, allEqual: true, originalDeps: originalDeps, committedDeps: committedDeps, preparing: true, possibleQuorum: allPossible(r.N), lastTriedBallot: lastTriedBallot, cmds: cmds, status: status, seq: seq, deps: deps, preVoters: replicaset.New(int(r.Id)), acceptVoters: replicaset.New(int(r.Id)), prepareVoters: replicaset.New(), phaseStarted: time.Now()}
}

func (r *Replica) newNilDeps() []int32 {
	nildeps := make([]int32, r.N)
	for i := 0; i < r.N; i++ {
		nildeps[i] = -1
	}
	return nildeps
}

// Legacy persistence hooks; inactive when Durable is false.
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

func (r *Replica) fastQuorumAvailable() bool {
	live := 0
	for id := int32(0); id < int32(r.N); id++ {
		if r.peerAlive(id) {
			live++
		}
	}
	return live >= r.Replica.FastQuorumSize()
}
