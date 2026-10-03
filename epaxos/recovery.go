package epaxos

// Recovery coordination, evidence collection, and log-gap repair.
import (
	"time"

	"github.com/hongzicong/ConsensusArena/replica/defs"
	fastrpc "github.com/hongzicong/ConsensusArena/rpc"
	"github.com/hongzicong/ConsensusArena/state"
)

type recoveryAttempt struct {
	nextAttempt time.Time
	backoff     time.Duration
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

type repairState struct {
	blocked     map[uint64]time.Time
	active      map[instanceId]bool
	requestChan chan fastrpc.Serializable
	requestRPC  uint8
	nextGapScan time.Time
}

func newRepairState() *repairState {
	return &repairState{blocked: make(map[uint64]time.Time), active: make(map[instanceId]bool), requestChan: make(chan fastrpc.Serializable, 4096)}
}

func (r *Replica) recoveryCoordinator(owner int32) int32 {
	if r.peerAlive(owner) {
		return owner
	}
	for i := int32(0); i < int32(r.N); i++ {
		if r.peerAlive(i) {
			return i
		}
	}
	return r.Id
}

func (r *Replica) handleRepairRequest(m *repairRequest) {
	if m.Owner < 0 || m.Owner >= int32(r.N) || m.Slot < 0 || m.Slot >= MAX_INSTANCE {
		return
	}
	i := r.InstanceSpace[m.Owner][m.Slot]
	if i != nil && i.Status >= COMMITTED && i.Cmds != nil {
		c := &Commit{r.Id, m.Owner, m.Slot, i.vbal, i.Cmds, i.Seq, i.Deps}
		for q := int32(0); q < int32(r.N); q++ {
			if q != r.Id {
				r.SendMsg(q, r.commitRPC, c)
			}
		}
		return
	}
	if r.recoveryCoordinator(m.Owner) == r.Id {
		r.scheduleRecovery(m.Owner, m.Slot, time.Now())
	}
}

func (r *Replica) blockedOn(owner, slot int32, now time.Time) {
	if slot > r.crtInstance[owner] {
		r.crtInstance[owner] = slot
	}
	key := recoveryKey(owner, slot)
	first, ok := r.blocked[key]
	if !ok {
		r.blocked[key] = now
		r.Stats.M["dependencyBlocks"]++
		return
	}
	if now.Sub(first) < COMMIT_GRACE_PERIOD {
		return
	}
	r.blocked[key] = now
	target := r.recoveryCoordinator(owner)
	if target == r.Id {
		r.scheduleRecovery(owner, slot, now)
	} else {
		r.SendMsg(target, r.requestRPC, &repairRequest{owner, slot})
	}
}

func (r *Replica) repairTick(now time.Time) {
	if !now.Before(r.nextGapScan) {
		r.nextGapScan = now.Add(200 * time.Millisecond)
		// Repair a bounded suffix in parallel. Waiting for DFS to uncover
		// one predecessor every grace period takes minutes after an owner
		// crashes with a window of unfinished instances.
		for owner := int32(0); owner < int32(r.N); owner++ {
			end := r.crtInstance[owner]
			if limit := r.ExecedUpTo[owner] + 256; end > limit {
				end = limit
			}
			for slot := r.ExecedUpTo[owner] + 1; slot <= end; slot++ {
				inst := r.InstanceSpace[owner][slot]
				if inst == nil || inst.Status < COMMITTED || inst.Cmds == nil {
					r.blockedOn(owner, slot, now)
				}
			}
		}
	}
	// Retry every discovered gap, even if a later DFS encounters a different
	// endpoint first. Otherwise an open component can continually move the
	// traversal frontier and starve recovery of earlier missing instances.
	for key := range r.blocked {
		owner, slot := int32(key>>32), int32(key)
		inst := r.InstanceSpace[owner][slot]
		if inst != nil && inst.Status >= COMMITTED && inst.Cmds != nil {
			delete(r.blocked, key)
			continue
		}
		r.blockedOn(owner, slot, now)
	}
	for id := range r.active {
		inst := r.InstanceSpace[id.replica][id.instance]
		if inst == nil || inst.Status >= COMMITTED || inst.lb == nil {
			delete(r.active, id)
			continue
		}
		lb := inst.lb
		if inst.bal != lb.lastTriedBallot {
			continue
		}
		age := now.Sub(lb.phaseStarted)
		if age < time.Second {
			continue
		}
		if !lb.preparing && !lb.tryingToPreAccept && (lb.status == PREACCEPTED || lb.status == PREACCEPTED_EQ) && lb.preAcceptOKs >= r.N/2 {
			lb.status = ACCEPTED
			inst.Status = ACCEPTED
			inst.Cmds = lb.cmds
			inst.Seq = lb.seq
			inst.Deps = append([]int32(nil), lb.deps...)
			inst.vbal = lb.lastTriedBallot
			lb.ballot = lb.lastTriedBallot
			r.bcastAccept(id.replica, id.instance)
			lb.phaseStarted = now
		} else if age >= COMMIT_GRACE_PERIOD {
			if r.recoveryCoordinator(id.replica) == r.Id {
				r.startRecoveryForInstance(id.replica, id.instance)
			}
		}
	}
}
