package epaxos

// Recovery coordination, evidence collection, and log-gap repair.
import (
	"github.com/hongzicong/ConsensusArena/replicaset"
	"github.com/hongzicong/ConsensusArena/state"
	"time"
)

type recoveryAttempt struct {
	nextAttempt time.Time
	backoff     time.Duration
}

func (c *core) handleLeaderCall() {
	c.isLeader = true
	c.Println("I am the leader")
}

func recoveryKey(replica, instance int32) uint64 {
	return uint64(uint32(replica))<<32 | uint64(uint32(instance))
}

func (c *core) scheduleRecovery(replica, instance int32, now time.Time) bool {
	key := recoveryKey(replica, instance)
	c.recoveryMu.Lock()
	attempt, exists := c.recoveryAttempts[key]
	if exists && now.Before(attempt.nextAttempt) {
		c.recoveryMu.Unlock()
		c.m.Lock()
		c.stats.M["recoverySuppressed"]++
		c.m.Unlock()
		return false
	}
	backoff := INITIAL_RECOVERY_BACKOFF
	if exists {
		backoff = attempt.backoff * 2
		if backoff > MAX_RECOVERY_BACKOFF {
			backoff = MAX_RECOVERY_BACKOFF
		}
	}
	c.recoveryAttempts[key] = recoveryAttempt{nextAttempt: now.Add(backoff), backoff: backoff}
	c.recoveryMu.Unlock()

	select {
	case c.instancesToRecover <- &instanceID{replica: replica, instance: instance}:
		c.m.Lock()
		c.stats.M["recoveryScheduled"]++
		c.m.Unlock()
		if c.Logger != nil {
			c.Printf("EPAXOS_RECOVERY_SCHEDULED replica=%d instance=%d next_backoff=%s", replica, instance, backoff)
		}
		return true
	default:
		c.m.Lock()
		c.stats.M["recoveryQueueFull"]++
		c.m.Unlock()
		return false
	}
}

func (c *core) clearRecovery(replica, instance int32) {
	c.recoveryMu.Lock()
	delete(c.recoveryAttempts, recoveryKey(replica, instance))
	c.recoveryMu.Unlock()
}

func (c *core) startRecoveryForInstance(replica int32, instance int32) {
	c.active[instanceID{replica, instance}] = true
	inst := c.instanceSpace[replica][instance]
	if inst == nil {
		inst = c.newInstanceDefault(replica, instance)
		c.instanceSpace[replica][instance] = inst
	} else if inst.Status >= COMMITTED && inst.Cmds != nil {
		c.clearRecovery(replica, instance)
		c.Printf("No need to recover %d.%d", replica, instance)
		return
	}

	// no TLA guidance here (some difference with the original implementation)
	var proposals []*proposal = nil
	if inst.lb != nil {
		proposals = inst.lb.clientProposals
	}
	inst.lb = c.newLeaderBookkeepingDefault()
	lb := inst.lb
	lb.clientProposals = proposals
	lb.ballot = inst.vbal
	lb.seq = inst.Seq
	lb.cmds = inst.Cmds
	lb.deps = inst.Deps
	lb.status = inst.Status
	c.makeBallot(replica, instance)

	inst.bal = lb.lastTriedBallot
	preply := &PrepareReply{
		c.id,
		replica,
		instance,
		inst.bal,
		inst.vbal,
		inst.Status,
		inst.Cmds,
		inst.Seq,
		inst.Deps}

	lb.prepareVoters.Add(int(c.id))
	lb.prepareReplies = append(lb.prepareReplies, preply)
	lb.leaderResponded = c.id == replica

	c.bcastPrepare(replica, instance)
}

func (c *core) handlePrepare(prepare *Prepare) {
	inst := c.instanceSpace[prepare.Replica][prepare.Instance]
	var preply *PrepareReply

	if prepare.Ballot > c.maxRecvBallot {
		c.maxRecvBallot = prepare.Ballot
	}

	if inst == nil {
		c.instanceSpace[prepare.Replica][prepare.Instance] = c.newInstanceDefault(prepare.Replica, prepare.Instance)
		inst = c.instanceSpace[prepare.Replica][prepare.Instance]
	}

	if prepare.Ballot < inst.bal {
		c.Printf("Joined higher ballot %d < %d", prepare.Ballot, inst.bal)
	} else if inst.bal < prepare.Ballot {
		c.Printf("Joining ballot %d ", prepare.Ballot)
		inst.bal = prepare.Ballot
	}

	preply = &PrepareReply{
		c.id,
		prepare.Replica,
		prepare.Instance,
		inst.bal,
		inst.vbal,
		inst.Status,
		inst.Cmds,
		inst.Seq,
		inst.Deps}
	c.send(prepare.LeaderId, c.prepareReplyRPC, preply)
}

func (c *core) handlePrepareReply(preply *PrepareReply) {
	inst := c.instanceSpace[preply.Replica][preply.Instance]
	if inst == nil || inst.lb == nil {
		return
	}
	lb := inst.lb
	if inst.bal != lb.lastTriedBallot {
		return
	}

	if preply.Ballot > c.maxRecvBallot {
		c.maxRecvBallot = preply.Ballot
	}

	if inst == nil || lb == nil || !lb.preparing {
		return
	}

	if preply.Ballot != lb.lastTriedBallot {
		lb.nacks++
		return
	}

	if preply.AcceptorId < 0 || preply.AcceptorId >= int32(c.n) || !lb.prepareVoters.Add(int(preply.AcceptorId)) {
		return
	}
	lb.prepareReplies = append(lb.prepareReplies, preply)
	if len(lb.prepareReplies) < c.SlowQuorumSize() {
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
		c.startPhase1(state.NOOP(), preply.Replica, preply.Instance, lb.lastTriedBallot, lb.clientProposals)
		return
	}
	lb.ballot, lb.cmds, lb.seq, lb.status = chosen.VBallot, chosen.Command, chosen.Seq, chosen.Status
	lb.deps = append([]int32(nil), chosen.Deps...)
	if chosen.Status >= COMMITTED {
		c.handleCommit(&Commit{c.id, preply.Replica, preply.Instance, chosen.VBallot, chosen.Command, chosen.Seq, chosen.Deps})
		c.broadcast(nil, c.n-1, c.commitRPC, &Commit{c.id, preply.Replica, preply.Instance, chosen.VBallot, chosen.Command, chosen.Seq, chosen.Deps})
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
	if chosen.Status == ACCEPTED || (initial && allEqual && !lb.leaderResponded && matching >= c.n/2) {
		inst.Status, lb.status = ACCEPTED, ACCEPTED
		inst.vbal, lb.ballot = lb.lastTriedBallot, lb.lastTriedBallot
		c.bcastAccept(preply.Replica, preply.Instance)
	} else if initial && allEqual && !lb.leaderResponded && matching >= (c.f+1)/2 {
		// Original optimized recovery (TentativePreAccept), including its
		// published limitations; this is not the EPaxos* recovery algorithm.
		lb.tryingToPreAccept = true
		lb.preAcceptOKs, lb.tpaReps = 0, 0
		lb.preVoters = replicaset.New()
		c.bcastTryPreAccept(preply.Replica, preply.Instance)
		c.handleTryPreAccept(&TryPreAccept{c.id, preply.Replica, preply.Instance, lb.lastTriedBallot, lb.cmds, lb.seq, lb.deps})
	} else {
		c.startPhase1(lb.cmds, preply.Replica, preply.Instance, lb.lastTriedBallot, lb.clientProposals)
	}
}

func (c *core) handleTryPreAccept(tpa *TryPreAccept) {
	inst := c.instanceSpace[tpa.Replica][tpa.Instance]

	if inst == nil {
		c.instanceSpace[tpa.Replica][tpa.Instance] = c.newInstanceDefault(tpa.Replica, tpa.Instance)
		inst = c.instanceSpace[tpa.Replica][tpa.Instance]
	}

	confRep, confInst := int32(-1), int32(-1)
	confStatus := int8(NONE)
	if inst.bal <= tpa.Ballot {
		inst.bal = tpa.Ballot
		if conflict, cr, ci := c.findPreAcceptConflicts(tpa.Command, tpa.Replica, tpa.Instance, tpa.Seq, tpa.Deps); conflict {
			confRep, confInst = cr, ci
			confStatus = c.instanceSpace[cr][ci].Status
		} else {
			if tpa.Instance > c.crtInstance[tpa.Replica] {
				c.crtInstance[tpa.Replica] = tpa.Instance
			}
			inst.Cmds, inst.Seq, inst.Deps = tpa.Command, tpa.Seq, append([]int32(nil), tpa.Deps...)
			inst.Status, inst.vbal = PREACCEPTED, tpa.Ballot
			c.updateConflicts(inst.Cmds, tpa.Replica, tpa.Instance, inst.Seq)
		}
	}
	reply := &TryPreAcceptReply{c.id, tpa.Replica, tpa.Instance, inst.bal, inst.vbal, confRep, confInst, confStatus}
	if tpa.LeaderId == c.id {
		c.handleTryPreAcceptReply(reply)
	} else {
		c.send(tpa.LeaderId, c.tryPreAcceptReplyRPC, reply)
	}

}

func (c *core) findPreAcceptConflicts(cmds []state.Command, replica int32, instance int32, seq int32, deps []int32) (bool, int32, int32) {
	inst := c.instanceSpace[replica][instance]
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
	for q := int32(0); q < int32(c.n); q++ {
		for i := c.executedUpTo[q]; i <= c.crtInstance[q]; i++ { // FIXME this is not enough imho.
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
			inst := c.instanceSpace[q][i]
			if inst == nil || inst.Cmds == nil || len(inst.Cmds) == 0 {
				continue
			}
			if inst.Deps[replica] >= instance {
				// instance q.i depends on instance replica.instance, it is not a conflict
				continue
			}
			if state.ConflictBatch(inst.Cmds, cmds) {
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

func (c *core) handleTryPreAcceptReply(tpar *TryPreAcceptReply) {
	inst := c.instanceSpace[tpar.Replica][tpar.Instance]

	if tpar.Ballot > c.maxRecvBallot {
		c.maxRecvBallot = tpar.Ballot
	}

	if inst == nil {
		c.instanceSpace[tpar.Replica][tpar.Instance] = c.newInstanceDefault(tpar.Replica, tpar.Instance)
		inst = c.instanceSpace[tpar.Replica][tpar.Instance]
	}

	lb := inst.lb
	if lb == nil || !lb.tryingToPreAccept {
		return
	}

	if tpar.Ballot != lb.lastTriedBallot {
		return
	}

	if tpar.AcceptorId < 0 || tpar.AcceptorId >= int32(c.n) || !lb.preVoters.Add(int(tpar.AcceptorId)) {
		return
	}
	lb.tpaReps++

	if tpar.VBallot == lb.lastTriedBallot && tpar.ConflictReplica < 0 {
		lb.preAcceptOKs++
		if lb.preAcceptOKs >= c.SlowQuorumSize() {
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

			c.bcastAccept(tpar.Replica, tpar.Instance)
			return
		}
	} else {
		lb.nacks++
		lb.possibleQuorum[tpar.AcceptorId] = false
		if tpar.ConflictReplica >= 0 && tpar.ConflictReplica < int32(c.n) {
			lb.possibleQuorum[tpar.ConflictReplica] = false
		}
	}

	lb.tpaAccepted = lb.tpaAccepted || (tpar.ConflictStatus >= ACCEPTED) // TLA spec. (page 39)

	if lb.tpaReps >= c.SlowQuorumSize()-1 && lb.tpaAccepted {
		//abandon recovery, restart from phase 1
		lb.tryingToPreAccept = false
		c.startPhase1(lb.cmds, tpar.Replica, tpar.Instance, lb.lastTriedBallot, lb.clientProposals)
		return
	}

	// the code below is not checked in TLA (liveness)
	notInQuorum := 0
	for q := 0; q < c.n; q++ {
		if !lb.possibleQuorum[q] {
			notInQuorum++
		}
	}

	if notInQuorum == c.n/2 {
		//this is to prevent defer cycles
		if present, dq, _ := c.deferredByInstance(tpar.Replica, tpar.Instance); present {
			if lb.possibleQuorum[dq] {
				//an instance whose leader must have been in this instance's quorum has been deferred for this instance => contradiction
				//abandon recovery, restart from phase 1
				lb.tryingToPreAccept = false
				c.makeBallot(tpar.Replica, tpar.Instance)
				c.startPhase1(lb.cmds, tpar.Replica, tpar.Instance, lb.lastTriedBallot, lb.clientProposals)
				return
			}
		}
	}

	if lb.tpaReps >= c.SlowQuorumSize() && tpar.ConflictReplica >= 0 {
		//defer recovery and update deferred information
		c.updateDeferred(tpar.Replica, tpar.Instance, tpar.ConflictReplica, tpar.ConflictInstance)
		lb.tryingToPreAccept = false
	}
}

func (c *core) updateDeferred(dr int32, di int32, r int32, i int32) {
	daux := (uint64(dr) << 32) | uint64(di)
	aux := (uint64(r) << 32) | uint64(i)
	c.deferMap[aux] = daux
}

func (c *core) deferredByInstance(q int32, i int32) (bool, int32, int32) {
	aux := (uint64(q) << 32) | uint64(i)
	daux, present := c.deferMap[aux]
	if !present {
		return false, 0, 0
	}
	dq := int32(daux >> 32)
	di := int32(daux)
	return true, dq, di
}

type repairState struct {
	blocked     map[uint64]time.Time
	active      map[instanceID]bool
	nextGapScan time.Time
}

func newRepairState() *repairState {
	return &repairState{blocked: make(map[uint64]time.Time), active: make(map[instanceID]bool)}
}

func (c *core) recoveryCoordinator(owner int32) int32 {
	if c.peerAlive(owner) {
		return owner
	}
	for i := int32(0); i < int32(c.n); i++ {
		if c.peerAlive(i) {
			return i
		}
	}
	return c.id
}

func (c *core) handleRepairRequest(m *repairRequest) {
	if m.Owner < 0 || m.Owner >= int32(c.n) || m.Slot < 0 || m.Slot >= MAX_INSTANCE {
		return
	}
	i := c.instanceSpace[m.Owner][m.Slot]
	if i != nil && i.Status >= COMMITTED && i.Cmds != nil {
		commit := &Commit{c.id, m.Owner, m.Slot, i.vbal, i.Cmds, i.Seq, i.Deps}
		c.broadcast(nil, c.n-1, c.commitRPC, commit)
		return
	}
	if c.recoveryCoordinator(m.Owner) == c.id {
		c.scheduleRecovery(m.Owner, m.Slot, time.Now())
	}
}

func (c *core) blockedOn(owner, slot int32, now time.Time) {
	if slot > c.crtInstance[owner] {
		c.crtInstance[owner] = slot
	}
	key := recoveryKey(owner, slot)
	first, ok := c.blocked[key]
	if !ok {
		c.blocked[key] = now
		c.stats.M["dependencyBlocks"]++
		return
	}
	if now.Sub(first) < COMMIT_GRACE_PERIOD {
		return
	}
	c.blocked[key] = now
	target := c.recoveryCoordinator(owner)
	if target == c.id {
		c.scheduleRecovery(owner, slot, now)
	} else {
		c.send(target, c.requestRPC, &repairRequest{owner, slot})
	}
}

func (c *core) repairTick(now time.Time) {
	if !now.Before(c.nextGapScan) {
		c.nextGapScan = now.Add(200 * time.Millisecond)
		// Repair a bounded suffix in parallel. Waiting for DFS to uncover
		// one predecessor every grace period takes minutes after an owner
		// crashes with a window of unfinished instances.
		for owner := int32(0); owner < int32(c.n); owner++ {
			end := c.crtInstance[owner]
			if limit := c.executedUpTo[owner] + 256; end > limit {
				end = limit
			}
			for slot := c.executedUpTo[owner] + 1; slot <= end; slot++ {
				inst := c.instanceSpace[owner][slot]
				if inst == nil || inst.Status < COMMITTED || inst.Cmds == nil {
					c.blockedOn(owner, slot, now)
				}
			}
		}
	}
	// Retry every discovered gap, even if a later DFS encounters a different
	// endpoint first. Otherwise an open component can continually move the
	// traversal frontier and starve recovery of earlier missing instances.
	for key := range c.blocked {
		owner, slot := int32(key>>32), int32(key)
		inst := c.instanceSpace[owner][slot]
		if inst != nil && inst.Status >= COMMITTED && inst.Cmds != nil {
			delete(c.blocked, key)
			continue
		}
		c.blockedOn(owner, slot, now)
	}
	for id := range c.active {
		inst := c.instanceSpace[id.replica][id.instance]
		if inst == nil || inst.Status >= COMMITTED || inst.lb == nil {
			delete(c.active, id)
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
		if !lb.preparing && !lb.tryingToPreAccept && (lb.status == PREACCEPTED || lb.status == PREACCEPTED_EQ) && lb.preAcceptOKs >= c.n/2 {
			lb.status = ACCEPTED
			inst.Status = ACCEPTED
			inst.Cmds = lb.cmds
			inst.Seq = lb.seq
			inst.Deps = append([]int32(nil), lb.deps...)
			inst.vbal = lb.lastTriedBallot
			lb.ballot = lb.lastTriedBallot
			c.bcastAccept(id.replica, id.instance)
			lb.phaseStarted = now
		} else if age >= COMMIT_GRACE_PERIOD {
			if c.recoveryCoordinator(id.replica) == c.id {
				c.startRecoveryForInstance(id.replica, id.instance)
			}
		}
	}
}
