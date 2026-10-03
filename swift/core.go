package swift

// Protocol state and normal-case transitions.
import (
	"sync"

	"github.com/hongzicong/ConsensusArena/hook"
	"github.com/hongzicong/ConsensusArena/replica/defs"
	"github.com/hongzicong/ConsensusArena/state"
)

type commandDesc struct {
	deferFast  bool
	phase      int
	cmd        state.Command
	dep        Dep
	hs         []SHash
	propose    *defs.GPropose
	proposeDep Dep

	slowPathH      *MsgSet
	fastPathH      *MsgSet
	afterPropagate *hook.OptCondF

	msgs     chan interface{}
	active   bool
	slowPath bool
	seq      bool
	stopChan chan *sync.WaitGroup

	successors  []defs.RequestID
	successorsL sync.Mutex

	// execute before sending an MSync message
	defered func()
}

type commandStaticDesc struct {
	cmdId    defs.RequestID
	phase    int
	cmd      state.Command
	dep      Dep
	slowPath bool
	defered  func()
}

type readDesc struct {
	hs      []SHash
	dep     Dep
	propose *defs.GPropose
}

type deferredProposal struct{ propose *defs.GPropose }

func (r *Replica) handlePropose(msg *defs.GPropose, desc *commandDesc, cmdId defs.RequestID) {
	if r.status != NORMAL || desc.propose != nil {
		return
	}

	desc.propose = msg
	if _, recovered := r.recoveryCmds[cmdId]; recovered {
		// Sync can arrive before this replica receives the original proposal.
		// Keep its installed command/phase/dependencies and use recovery evidence;
		// a restored descriptor has no normal-case hash proposal to broadcast.
		finishAck := r.sendRecoveryAck(cmdId, desc.dep, msg, desc)
		desc.afterPropagate.Recall()
		if !r.delivered.Has(cmdId.String()) {
			finishAck()
		}
		return
	}
	desc.cmd = msg.Command

	if !r.FQ.Contains(int(r.Id)) || desc.deferFast {
		desc.afterPropagate.Recall()
		return
	}

	desc.dep = desc.proposeDep
	desc.phase = PRE_ACCEPT
	if (desc.afterPropagate.Recall() && desc.slowPath) ||
		r.delivered.Has(cmdId.String()) {
		// in this case a process already sent an MSlowAck
		// message, hence, no need to send MFastAck
		return
	}

	if _, exists := r.slowAddrs[msg.Addr]; exists && r.Id != r.leader() {
		return
	}

	fastAck := newFastAck()
	fastAck.Replica = r.Id
	fastAck.Ballot = r.ballot
	fastAck.CmdId = cmdId
	fastAck.Dep = desc.dep
	fastAck.Checksum = desc.hs
	if r.Id == r.leader() {
		fastAck.Seqnum = r.seqnum
		r.seqnum++
	}

	isRead := r.fastRead && msg.Command.Op != state.PUT

	fastAckSend := copyFastAck(fastAck)
	if !r.optExec {
		r.batcher.SendFastAck(fastAckSend)
	} else {
		if (r.Id == r.leader() && !isRead) || (isRead && desc.propose.Proxy) {
			r.batcher.SendFastAck(fastAckSend)
			// TODO: save old state
			r.deliver(desc, cmdId)
		} else {
			r.batcher.SendFastAckClient(fastAckSend, msg.ClientId)
		}
	}
	r.handleFastAck(fastAck, desc)
}

func (r *Replica) handleFastAck(msg *MFastAck, desc *commandDesc) {
	if msg.Replica == r.leader() {
		r.fastAckFromLeader(msg, desc)
	} else {
		r.commonCaseFastAck(msg, desc)
	}
}

func (r *Replica) fastAckFromLeader(msg *MFastAck, desc *commandDesc) {
	desc.afterPropagate.Call(func() {
		if r.status != NORMAL || r.ballot != msg.Ballot {
			return
		}

		// TODO: make sure that
		//    ∀ id' ∈ d. phase[id'] ∈ {ACCEPT, COMMIT}
		//
		// seems to be satisfied already

		desc.phase = ACCEPT
		dep := Dep(msg.Dep)
		hs := desc.hs
		neq := !desc.dep.Equals(dep)
		fast := r.FQ.Contains(int(r.Id))
		slow := r.SQ.Contains(int(r.Id))
		sendSlowAck := r.leader() != r.Id && (slow || (fast && neq))
		msgCmdId := msg.CmdId
		msgChecksum := msg.Checksum

		defer func() {
			if r.leader() == r.Id || r.delivered.Has(msgCmdId.String()) {
				return
			}
			if !sendSlowAck && r.optExec && !SHashesEq(hs, msgChecksum) {
				lightSlowAck := &MLightSlowAck{
					Replica: r.Id,
					Ballot:  r.ballot,
					CmdId:   msgCmdId,
				}

				r.SendClientMsg(msgCmdId.Client, r.cs.lightSlowAckRPC, lightSlowAck)
			}
		}()

		desc.slowPathH.Add(msg.Replica, true, msg)
		delivered := r.delivered.Has(msgCmdId.String())
		if !delivered {
			desc.fastPathH.Add(msg.Replica, true, msg)
			delivered = r.delivered.Has(msgCmdId.String())
		}

		if sendSlowAck {
			if neq && !delivered {
				desc.dep = dep
			}
			desc.slowPath = true

			lightSlowAck := &MLightSlowAck{
				Replica: r.Id,
				Ballot:  r.ballot,
				CmdId:   msgCmdId,
			}

			if !r.optExec {
				r.batcher.SendLightSlowAck(lightSlowAck)
			} else {
				r.batcher.SendLightSlowAckClient(lightSlowAck, msgCmdId.Client)
			}
			if !delivered {
				r.handleLightSlowAck(lightSlowAck, desc)
			}
		}
	})
}

func (r *Replica) commonCaseFastAck(msg *MFastAck, desc *commandDesc) {
	if r.status != NORMAL || r.ballot != msg.Ballot {
		return
	}

	msgCmdId := msg.CmdId
	if msg.Dep == nil {
		desc.slowPathH.Add(msg.Replica, msg.Replica == r.leader(), msg)
		if r.delivered.Has(msgCmdId.String()) {
			return
		}
	}
	desc.fastPathH.Add(msg.Replica, msg.Replica == r.leader(), msg)
}

func getFastAndSlowAcksHandler(r *Replica, desc *commandDesc) MsgSetHandler {
	return func(leaderMsg interface{}, msgs []interface{}) {

		if leaderMsg == nil {
			return
		}

		leaderFastAck := leaderMsg.(*MFastAck)

		desc.phase = COMMIT

		for _, depCmdId := range desc.dep {
			depDesc := r.getCmdDesc(depCmdId, nil, nil)
			if depDesc == nil {
				continue
			}
			depDesc.successorsL.Lock()
			depDesc.successors = append(depDesc.successors, leaderFastAck.CmdId)
			depDesc.successorsL.Unlock()
		}

		r.deliver(desc, leaderFastAck.CmdId)
	}
}

func (r *Replica) handleLightSlowAck(msg *MLightSlowAck, desc *commandDesc) {
	fastAck := newFastAck()
	fastAck.Replica = msg.Replica
	fastAck.Ballot = msg.Ballot
	fastAck.CmdId = msg.CmdId
	fastAck.Dep = nil
	r.commonCaseFastAck(fastAck, desc)
}

func (r *Replica) getCmdDesc(cmdId defs.RequestID, msg interface{}, dep Dep) *commandDesc {
	return r.getCmdDescSeq(cmdId, msg, dep, nil, false)
}

func (r *Replica) getCmdDescSeq(cmdId defs.RequestID, msg interface{}, dep Dep, hs []SHash, seq bool) *commandDesc {
	key := cmdId.String()
	if r.delivered.Has(key) {
		return nil
	}

	var desc *commandDesc

	r.cmdDescs.Upsert(key, nil,
		func(exists bool, mapV, _ interface{}) interface{} {
			defer func() {
				if dep != nil && desc.proposeDep == nil {
					desc.proposeDep = dep
					if hs != nil {
						desc.hs = hs
					}
				}
			}()

			if exists {
				desc = mapV.(*commandDesc)
				return desc
			}

			desc = r.newDesc()
			desc.seq = seq || desc.seq
			if !desc.seq {
				go r.handleDesc(desc, cmdId)
				r.routineCount++
			}

			return desc
		})

	if msg != nil {
		if desc.seq {
			r.handleMsg(msg, desc, cmdId)
		} else {
			desc.msgs <- msg
		}
	}

	return desc
}

func (r *Replica) newDesc() *commandDesc {
	desc := r.allocDesc()
	desc.dep = nil
	desc.hs = nil
	if desc.msgs == nil {
		desc.msgs = make(chan interface{}, 8)
	} else {
		for len(desc.msgs) != 0 {
			<-desc.msgs
		}
	}
	desc.active = true
	desc.phase = START
	desc.successors = nil
	desc.slowPath = false
	desc.deferFast = false
	desc.seq = (r.routineCount >= MaxDescRoutines)
	desc.defered = func() {}
	desc.propose = nil
	desc.proposeDep = nil
	if desc.stopChan == nil {
		desc.stopChan = make(chan *sync.WaitGroup, 8)
	} else {
		for len(desc.stopChan) != 0 {
			<-desc.stopChan
		}
	}

	desc.afterPropagate = desc.afterPropagate.ReinitCondF(func() bool {
		return desc.propose != nil
	})

	acceptFastAndSlowAck := func(msg, leaderMsg interface{}) bool {
		if leaderMsg == nil {
			return true
		}
		leaderFastAck := leaderMsg.(*MFastAck)
		fastAck := msg.(*MFastAck)
		return fastAck.Dep == nil ||
			(Dep(leaderFastAck.Dep)).Equals(fastAck.Dep)
	}

	h := getFastAndSlowAcksHandler(r, desc)
	desc.slowPathH = desc.slowPathH.ReinitMsgSet(r.SQ, acceptFastAndSlowAck, func(interface{}) {}, h)
	desc.fastPathH = desc.fastPathH.ReinitMsgSet(r.FQ, acceptFastAndSlowAck, func(interface{}) {}, h)

	return desc
}

func (r *Replica) allocDesc() *commandDesc {
	if r.poolLevel > 0 {
		return r.descPool.Get().(*commandDesc)
	}
	return &commandDesc{}
}

func (r *Replica) freeDesc(desc *commandDesc) {
	if r.poolLevel > 0 {
		r.descPool.Put(desc)
	}
}

func (r *Replica) handleDesc(desc *commandDesc, cmdId defs.RequestID) {
	defer func() {
		for len(desc.stopChan) != 0 {
			(<-desc.stopChan).Done()
		}
	}()

	for desc.active {
		select {
		case wg := <-desc.stopChan:
			desc.active = false
			wg.Done()
			return
		case msg := <-desc.msgs:
			if r.handleMsg(msg, desc, cmdId) {
				r.routineCount--
				return
			}
		}
	}
}

func (r *Replica) handleMsg(m interface{}, desc *commandDesc, cmdId defs.RequestID) bool {
	switch msg := m.(type) {

	case *defs.GPropose:
		r.handlePropose(msg, desc, cmdId)
	case *deferredProposal:
		desc.deferFast = true
		r.handlePropose(msg.propose, desc, cmdId)

	case *MFastAck:
		if msg.CmdId == cmdId {
			r.handleFastAck(msg, desc)
		}

	case *MLightSlowAck:
		if msg.CmdId == cmdId {
			r.handleLightSlowAck(msg, desc)
		}

	case string:
		if msg == "deliver" {
			r.deliver(desc, cmdId)
		}

	case int:
		r.history[msg].cmdId = cmdId
		r.history[msg].phase = desc.phase
		r.history[msg].cmd = desc.cmd
		r.history[msg].dep = desc.dep
		r.history[msg].slowPath = desc.slowPath
		r.history[msg].defered = desc.defered
		desc.active = false
		desc.slowPathH.Free()
		desc.fastPathH.Free()
		r.cmdDescs.Remove(cmdId.String())
		r.freeDesc(desc)
		return true
	}

	return false
}

func (r *Replica) leader() int32 {
	return Leader(r.ballot, r.N)
}

func (r *Replica) getDepAndHashes(cmd state.Command, cmdId defs.RequestID) (Dep, []SHash) {
	return r.getDepAndHashesWithUpdate(cmd, cmdId, nil, false)
}

func (r *Replica) getDepAndHashesWithUpdate(cmd state.Command, cmdId defs.RequestID, update *UpdateEntry, deferHash bool) (Dep, []SHash) {
	dep := []defs.RequestID{}
	hashes := []SHash{}
	keysOfCmd := keysOf(cmd)
	if update != nil && len(update.hash) != len(keysOfCmd) {
		r.Fatal("the number of hashes does not match number of objects", " cmd=", cmdId)
	}

	for i, key := range keysOfCmd {
		info, exists := r.keys[key]
		if exists {
			cdep := info.getConflictCmds(cmd)
			dep = append(dep, cdep...)
		} else {
			info = newLightKeyInfo()
			r.keys[key] = info
		}
		info.add(cmd, cmdId)

		l, exists := r.hlog[key]
		if !exists {
			l = NewHashLog()
			l.BeginBallot(r.ballot)
			r.hlog[key] = l
		}
		if update == nil && deferHash {
			l.AppendDeferred(cmdId)
		} else if update == nil {
			hashes = append(hashes, l.Append(cmd, cmdId))
		} else {
			l.AppendAndUpdate(cmdId, update.seqnum, update.hash[i])
		}
	}
	if update != nil || deferHash {
		return dep, nil
	}

	return dep, hashes
}

func (r *Replica) recordLeaderHash(cmdId defs.RequestID, s int, hs []SHash) {
	p, exists := r.proposes[cmdId]
	if !exists {
		r.pendingHashUpds[cmdId] = &UpdateEntry{
			hash:   hs,
			seqnum: s,
		}
		return
	}
	r.updateLogs(p.Command, cmdId, s, hs)
}

func (r *Replica) updateLogs(cmd state.Command, cmdId defs.RequestID, s int, hs []SHash) {
	keys := keysOf(cmd)

	if len(keys) != len(hs) {
		r.Fatal("the number of hashes does not match number of objects", " cmd=", cmdId,
			" ballot=", r.ballot, " seqnum=", s, " objects=", len(keys), " hashes=", len(hs))
	}

	for i, key := range keys {
		l, exists := r.hlog[key]
		if !exists {
			l = NewHashLog()
			l.BeginBallot(r.ballot)
			r.hlog[key] = l
		}
		l.Update(cmdId, s, hs[i])
	}
}

// Drain only already-ready proposals, preserving FIFO and bounding the delay
// before other message classes can run. Never wait to assemble a batch.
func (r *Replica) handleProposalBatch(first *defs.GPropose) {
	additional := len(r.ProposeChan)
	if additional > 255 {
		additional = 255
	}
	r.proposalBatches++
	r.proposalBatchCommands += uint64(additional + 1)
	if additional+1 > r.proposalBatchMax {
		r.proposalBatchMax = additional + 1
	}
	r.handleQueuedProposal(first)
	for i := 0; i < additional; i++ {
		r.handleQueuedProposal(<-r.ProposeChan)
	}
}

func (r *Replica) handleQueuedProposal(propose *defs.GPropose) {
	cmdId := propose.RequestID()
	if _, seen := r.proposedInBallot[cmdId]; seen {
		return
	}
	r.proposedInBallot[cmdId] = struct{}{}
	r.proposes[cmdId] = propose
	if _, recovered := r.recoveryCmds[cmdId]; recovered {
		// Sync already installed this command's dependencies. Do not append
		// a hash proposal before handlePropose takes the recovery branch.
		r.getCmdDescSeq(cmdId, propose, nil, nil, r.leader() == r.Id)
		return
	}
	upd, deferred := r.pendingHashUpds[cmdId]
	var dep Dep
	var hs []SHash
	deferFast := !deferred && r.shouldDeferFast(propose.Command)
	if deferred && r.leader() != r.Id && r.SQ.Contains(int(r.Id)) {
		// The already-enqueued leader transition will send SlowAck and make
		// handlePropose return before any local FastAck can consume this hash.
		dep, hs = r.getDepAndHashesWithUpdate(propose.Command, cmdId, upd, false)
		delete(r.pendingHashUpds, cmdId)
		r.deferredHashElisions++
	} else if deferFast {
		dep, hs = r.getDepAndHashesWithUpdate(propose.Command, cmdId, nil, true)
		r.hashBacklogDeferrals++
	} else {
		dep, hs = r.getDepAndHashes(propose.Command, cmdId)
	}
	if deferred && r.leader() != r.Id && !r.SQ.Contains(int(r.Id)) {
		// TODO: when pipelining this can break ordering, disabling fast paths.
		delete(r.pendingHashUpds, cmdId)
		r.recordLeaderHash(cmdId, upd.seqnum, upd.hash)
	}
	// FIXME: leader can receive fastAck before Propose,
	//        in this case desc is not seq
	var message interface{} = propose
	if deferFast {
		message = &deferredProposal{propose: propose}
	}
	desc := r.getCmdDescSeq(cmdId, message, dep, hs, r.leader() == r.Id)
	if desc == nil {
		r.Fatal("Got proposal for the delivered command", cmdId)
	}
}

func (r *Replica) shouldDeferFast(cmd state.Command) bool {
	// Optional speculative reads may need the local proxy's execution/reply.
	// Keep their existing path rather than suppressing that local fast vote.
	if r.fastRead || r.leader() == r.Id || !r.SQ.Contains(int(r.Id)) {
		return false
	}
	for _, key := range keysOf(cmd) {
		if l := r.hlog[key]; l != nil && len(l.nodes) >= 1024 {
			return true
		}
	}
	return false
}
