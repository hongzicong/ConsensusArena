package swift

// Protocol state and normal-case transitions.
import (
	"github.com/hongzicong/ConsensusArena/dlog"
	"github.com/hongzicong/ConsensusArena/protocol"
	"github.com/hongzicong/ConsensusArena/replica/defs"
	fastrpc "github.com/hongzicong/ConsensusArena/rpc"
	"github.com/hongzicong/ConsensusArena/state"
	"time"
)

// corePorts exposes runtime observations, output and scheduling capabilities.
// Ingress queues and output delivery remain owned by Replica.
type corePorts struct {
	n  int
	id int32
	*dlog.Logger
	codes               *rpcCodes
	send                func(int, uint8, fastrpc.Serializable) error
	broadcast           func(fastrpc.Serializable, uint8) int
	sendClient          func(int32, uint8, fastrpc.Serializable)
	peerAlive           func(int) bool
	executeEnabled      func() bool
	replyAfterExecution func() bool
	allocDesc           func() *commandDesc
	freeDesc            func(*commandDesc)
	batcher             ackBatcher
}
type ackBatcher interface {
	SendFastAck(*MFastAck)
	SendFastAckClient(*MFastAck)
	SendLightSlowAck(*MLightSlowAck)
	SendLightSlowAckClient(*MLightSlowAck)
}

// core owns protocol state, ordering, recovery decisions and completion semantics.
type core struct {
	corePorts
	execute func(state.Command) state.Value

	ballot  int32
	cballot int32
	status  int

	descriptors map[defs.RequestID]*commandDesc
	delivered   map[defs.RequestID]struct{}
	localWork   []localWork
	localHead   int
	generation  uint64

	keys map[state.Key]keyInfo
	hlog map[state.Key]*HashLog

	proposalBatches       uint64
	deferredHashElisions  uint64
	hashBacklogDeferrals  uint64
	proposalBatchCommands uint64
	proposalBatchMax      int
	proposedInBallot      map[defs.RequestID]struct{}
	seqnum                int
	pendingHashUpds       map[defs.RequestID]*UpdateEntry
	recoveryCmds          map[defs.RequestID]int // commands installed by the current Sync

	history      []commandStaticDesc
	historySize  int
	historyStart int

	qs *QuorumSystem
	sq QuorumI
	fq QuorumI

	fixedMajority bool

	optExec  bool
	fastRead bool

	recStart       time.Time
	newLeaderAckNs *MsgSet

	proposes map[defs.RequestID]*proposal

	// take only slow paths for these addresses
	slowAddrs map[string]struct{}
}

func newCore(ports corePorts, fastRead, optExec bool, slowAddrs []string, initial Quorum, initialLeader int32) *core {
	c := &core{
		corePorts: ports,

		ballot:  0,
		cballot: 0,
		status:  NORMAL,

		descriptors: make(map[defs.RequestID]*commandDesc),
		delivered:   make(map[defs.RequestID]struct{}),

		keys:             make(map[state.Key]keyInfo),
		hlog:             make(map[state.Key]*HashLog),
		seqnum:           0,
		proposedInBallot: make(map[defs.RequestID]struct{}),
		pendingHashUpds:  make(map[defs.RequestID]*UpdateEntry),

		history:      make([]commandStaticDesc, HISTORY_SIZE),
		historySize:  0,
		historyStart: 0,

		fixedMajority: false,

		optExec:  optExec,
		fastRead: fastRead,

		proposes: make(map[defs.RequestID]*proposal),

		slowAddrs: make(map[string]struct{}),
	}

	for _, addr := range slowAddrs {
		c.slowAddrs[addr] = struct{}{}
	}
	c.sq = NewMajorityOf(c.n)
	c.fq = NewThreeQuartersOf(c.n)
	qs, err := NewQuorumSystem(c.n/2+1, c.n, initial, initialLeader)
	if err != nil {
		c.Fatal(err)
	}
	c.qs = qs
	c.ballot = c.qs.BallotAt(0)
	c.cballot = c.ballot
	c.fixedMajority = true
	c.fq = c.qs.AQ(c.ballot)
	return c
}

type commandDesc struct {
	deferFast  bool
	phase      int
	cmd        state.Command
	dep        Dep
	hs         []SHash
	propose    *proposal
	proposeDep Dep

	slowPathH *MsgSet
	fastPathH *MsgSet
	// Leader acknowledgements wait for the original proposal, in arrival order.
	pendingLeaderAcks []*MFastAck

	active     bool
	slowPath   bool
	generation uint64

	successors []defs.RequestID

	// execute before sending an MSync message
	defered func()
}

type commandStaticDesc struct {
	cmdID    defs.RequestID
	phase    int
	cmd      state.Command
	dep      Dep
	slowPath bool
	defered  func()
}

type readDesc struct {
	hs      []SHash
	dep     Dep
	propose *proposal
}

type deferredProposal struct{ propose *proposal }

func (c *core) handlePropose(msg *proposal, desc *commandDesc, cmdID defs.RequestID) {
	if c.status != NORMAL || desc.propose != nil {
		return
	}

	desc.propose = msg
	if _, recovered := c.recoveryCmds[cmdID]; recovered {
		// Sync can arrive before this replica receives the original proposal.
		// Keep its installed command/phase/dependencies and use recovery evidence;
		// a restored descriptor has no normal-case hash proposal to broadcast.
		finishAck := c.sendRecoveryAck(cmdID, desc.dep, msg, desc)
		c.replayPendingLeaderAcks(desc)
		if !c.hasDelivered(cmdID) {
			finishAck()
		}
		return
	}
	desc.cmd = msg.Command

	if !c.fq.Contains(int(c.id)) || desc.deferFast {
		c.replayPendingLeaderAcks(desc)
		return
	}

	desc.dep = desc.proposeDep
	desc.phase = PRE_ACCEPT
	if (c.replayPendingLeaderAcks(desc) && desc.slowPath) ||
		c.hasDelivered(cmdID) {
		// in this case a process already sent an MSlowAck
		// message, hence, no need to send MFastAck
		return
	}

	if _, exists := c.slowAddrs[msg.Addr]; exists && c.id != c.leader() {
		return
	}

	fastAck := newFastAck()
	fastAck.Replica = c.id
	fastAck.Ballot = c.ballot
	fastAck.CmdId = cmdID
	fastAck.Dep = desc.dep
	fastAck.Checksum = desc.hs
	if c.id == c.leader() {
		fastAck.Seqnum = c.seqnum
		c.seqnum++
	}

	isRead := c.fastRead && msg.Command.Op != state.PUT

	fastAckSend := copyFastAck(fastAck)
	if !c.optExec {
		c.batcher.SendFastAck(fastAckSend)
	} else {
		if (c.id == c.leader() && !isRead) || (isRead && desc.propose.Proxy) {
			c.batcher.SendFastAck(fastAckSend)
			// TODO: save old state
			c.apply(desc, cmdID)
		} else {
			c.batcher.SendFastAckClient(fastAckSend)
		}
	}
	c.handleFastAck(fastAck, desc)
}

func (c *core) handleFastAck(msg *MFastAck, desc *commandDesc) {
	if msg.Replica == c.leader() {
		c.fastAckFromLeader(msg, desc)
	} else {
		c.commonCaseFastAck(msg, desc)
	}
}

func (c *core) fastAckFromLeader(msg *MFastAck, desc *commandDesc) {
	if desc.propose == nil {
		desc.pendingLeaderAcks = append(desc.pendingLeaderAcks, msg)
		return
	}
	c.replayPendingLeaderAcks(desc)
	c.processLeaderFastAck(msg, desc)
}

// replayPendingLeaderAcks runs at the original proposal-processing boundaries.
// Its result reports pending work consumed, even if a stale ballot rejects it.
func (c *core) replayPendingLeaderAcks(desc *commandDesc) bool {
	if desc.propose == nil || len(desc.pendingLeaderAcks) == 0 {
		return false
	}
	pending := desc.pendingLeaderAcks
	desc.pendingLeaderAcks = nil
	for _, msg := range pending {
		c.processLeaderFastAck(msg, desc)
	}
	clear(pending)
	return true
}

func (c *core) processLeaderFastAck(msg *MFastAck, desc *commandDesc) {
	if c.status != NORMAL || c.ballot != msg.Ballot {
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
	fast := c.fq.Contains(int(c.id))
	slow := c.sq.Contains(int(c.id))
	sendSlowAck := c.leader() != c.id && (slow || (fast && neq))
	msgCmdID := msg.CmdId
	msgChecksum := msg.Checksum

	defer func() {
		if c.leader() == c.id || c.hasDelivered(msgCmdID) {
			return
		}
		if !sendSlowAck && c.optExec && !SHashesEq(hs, msgChecksum) {
			lightSlowAck := &MLightSlowAck{
				Replica: c.id,
				Ballot:  c.ballot,
				CmdId:   msgCmdID,
			}

			c.sendClient(msgCmdID.Client, c.codes.lightSlowAckRPC, lightSlowAck)
		}
	}()

	desc.slowPathH.Add(msg.Replica, true, msg)
	delivered := c.hasDelivered(msgCmdID)
	if !delivered {
		desc.fastPathH.Add(msg.Replica, true, msg)
		delivered = c.hasDelivered(msgCmdID)
	}

	if sendSlowAck {
		if neq && !delivered {
			desc.dep = dep
		}
		desc.slowPath = true

		lightSlowAck := &MLightSlowAck{
			Replica: c.id,
			Ballot:  c.ballot,
			CmdId:   msgCmdID,
		}

		if !c.optExec {
			c.batcher.SendLightSlowAck(lightSlowAck)
		} else {
			c.batcher.SendLightSlowAckClient(lightSlowAck)
		}
		if !delivered {
			c.handleLightSlowAck(lightSlowAck, desc)
		}
	}
}

func (c *core) commonCaseFastAck(msg *MFastAck, desc *commandDesc) {
	if c.status != NORMAL || c.ballot != msg.Ballot {
		return
	}

	msgCmdID := msg.CmdId
	if msg.Dep == nil {
		desc.slowPathH.Add(msg.Replica, msg.Replica == c.leader(), msg)
		if c.hasDelivered(msgCmdID) {
			return
		}
	}
	desc.fastPathH.Add(msg.Replica, msg.Replica == c.leader(), msg)
}

func getFastAndSlowAcksHandler(c *core, desc *commandDesc) MsgSetHandler {
	return func(leaderMsg interface{}, msgs []interface{}) {

		if leaderMsg == nil {
			return
		}

		leaderFastAck := leaderMsg.(*MFastAck)

		desc.phase = COMMIT

		for _, depCmdID := range desc.dep {
			depDesc := c.getCmdDesc(depCmdID, nil, nil)
			if depDesc == nil {
				continue
			}
			depDesc.successors = append(depDesc.successors, leaderFastAck.CmdId)
		}

		c.apply(desc, leaderFastAck.CmdId)
	}
}

func (c *core) handleLightSlowAck(msg *MLightSlowAck, desc *commandDesc) {
	fastAck := newFastAck()
	fastAck.Replica = msg.Replica
	fastAck.Ballot = msg.Ballot
	fastAck.CmdId = msg.CmdId
	fastAck.Dep = nil
	c.commonCaseFastAck(fastAck, desc)
}

func (c *core) getCmdDesc(cmdID defs.RequestID, msg interface{}, dep Dep) *commandDesc {
	return c.getCmdDescWithHashes(cmdID, msg, dep, nil)
}

func (c *core) getCmdDescWithHashes(cmdID defs.RequestID, msg interface{}, dep Dep, hs []SHash) *commandDesc {
	if c.hasDelivered(cmdID) {
		return nil
	}
	desc := c.descriptors[cmdID]
	if desc == nil {
		desc = c.newDesc()
		c.descriptors[cmdID] = desc
	}
	if dep != nil && desc.proposeDep == nil {
		desc.proposeDep = dep
		if hs != nil {
			desc.hs = hs
		}
	}
	if msg != nil {
		c.localWork = append(c.localWork, localWork{kind: descriptorWork, desc: desc, generation: desc.generation, cmdID: cmdID, message: msg})
	}
	return desc
}

func (c *core) newDesc() *commandDesc {
	desc := c.allocDesc()
	desc.dep = nil
	desc.hs = nil
	desc.active = true
	c.generation++
	desc.generation = c.generation
	desc.phase = START
	desc.successors = nil
	desc.slowPath = false
	desc.deferFast = false
	desc.defered = func() {}
	desc.propose = nil
	desc.proposeDep = nil

	clear(desc.pendingLeaderAcks)
	desc.pendingLeaderAcks = nil

	acceptFastAndSlowAck := func(msg, leaderMsg interface{}) bool {
		if leaderMsg == nil {
			return true
		}
		leaderFastAck := leaderMsg.(*MFastAck)
		fastAck := msg.(*MFastAck)
		return fastAck.Dep == nil ||
			(Dep(leaderFastAck.Dep)).Equals(fastAck.Dep)
	}

	h := getFastAndSlowAcksHandler(c, desc)
	desc.slowPathH = desc.slowPathH.ReinitMsgSet(c.sq, acceptFastAndSlowAck, func(interface{}) {}, h)
	desc.fastPathH = desc.fastPathH.ReinitMsgSet(c.fq, acceptFastAndSlowAck, func(interface{}) {}, h)

	return desc
}

func (c *core) handleMsg(m interface{}, desc *commandDesc, cmdID defs.RequestID) {
	switch msg := m.(type) {

	case *proposal:
		c.handlePropose(msg, desc, cmdID)
	case *deferredProposal:
		desc.deferFast = true
		c.handlePropose(msg.propose, desc, cmdID)

	case *MFastAck:
		if msg.CmdId == cmdID {
			c.handleFastAck(msg, desc)
		}

	case *MLightSlowAck:
		if msg.CmdId == cmdID {
			c.handleLightSlowAck(msg, desc)
		}

	case string:
		if msg == "deliver" {
			c.apply(desc, cmdID)
		}

	}
}

func (c *core) leader() int32 {
	return Leader(c.ballot, c.n)
}

func (c *core) getDepAndHashes(cmd state.Command, cmdID defs.RequestID) (Dep, []SHash) {
	return c.getDepAndHashesWithUpdate(cmd, cmdID, nil, false)
}

func (c *core) getDepAndHashesWithUpdate(cmd state.Command, cmdID defs.RequestID, update *UpdateEntry, deferHash bool) (Dep, []SHash) {
	dep := []defs.RequestID{}
	hashes := []SHash{}
	keysOfCmd := keysOf(cmd)
	if update != nil && len(update.hash) != len(keysOfCmd) {
		c.Fatal("the number of hashes does not match number of objects", " cmd=", cmdID)
	}

	for i, key := range keysOfCmd {
		info, exists := c.keys[key]
		if exists {
			cdep := info.getConflictCmds(cmd)
			dep = append(dep, cdep...)
		} else {
			info = newLightKeyInfo()
			c.keys[key] = info
		}
		info.add(cmd, cmdID)

		l, exists := c.hlog[key]
		if !exists {
			l = NewHashLog()
			l.BeginBallot(c.ballot)
			c.hlog[key] = l
		}
		if update == nil && deferHash {
			l.AppendDeferred(cmdID)
		} else if update == nil {
			hashes = append(hashes, l.Append(cmd, cmdID))
		} else {
			l.AppendAndUpdate(cmdID, update.seqnum, update.hash[i])
		}
	}
	if update != nil || deferHash {
		return dep, nil
	}

	return dep, hashes
}

func (c *core) recordLeaderHash(cmdID defs.RequestID, s int, hs []SHash) {
	p, exists := c.proposes[cmdID]
	if !exists {
		c.pendingHashUpds[cmdID] = &UpdateEntry{
			hash:   hs,
			seqnum: s,
		}
		return
	}
	c.updateLogs(p.Command, cmdID, s, hs)
}

func (c *core) updateLogs(cmd state.Command, cmdID defs.RequestID, s int, hs []SHash) {
	keys := keysOf(cmd)

	if len(keys) != len(hs) {
		c.Fatal("the number of hashes does not match number of objects", " cmd=", cmdID,
			" ballot=", c.ballot, " seqnum=", s, " objects=", len(keys), " hashes=", len(hs))
	}

	for i, key := range keys {
		l, exists := c.hlog[key]
		if !exists {
			l = NewHashLog()
			l.BeginBallot(c.ballot)
			c.hlog[key] = l
		}
		l.Update(cmdID, s, hs[i])
	}
}

// Drain only already-ready proposals, preserving FIFO and bounding the delay
// before other message classes can run. Never wait to assemble a batch.
func (c *core) beginProposalBatch(count int) {
	additional := count - 1
	c.proposalBatches++
	c.proposalBatchCommands += uint64(additional + 1)
	if additional+1 > c.proposalBatchMax {
		c.proposalBatchMax = additional + 1
	}
}

func (c *core) submit(propose *proposal) {
	cmdID := propose.RequestID()
	if _, seen := c.proposedInBallot[cmdID]; seen {
		return
	}
	c.proposedInBallot[cmdID] = struct{}{}
	c.proposes[cmdID] = propose
	if _, recovered := c.recoveryCmds[cmdID]; recovered {
		// Sync already installed this command's dependencies. Do not append
		// a hash proposal before handlePropose takes the recovery branch.
		c.getCmdDescWithHashes(cmdID, propose, nil, nil)
		return
	}
	upd, deferred := c.pendingHashUpds[cmdID]
	var dep Dep
	var hs []SHash
	deferFast := !deferred && c.shouldDeferFast(propose.Command)
	if deferred && c.leader() != c.id && c.sq.Contains(int(c.id)) {
		// The already-enqueued leader transition will send SlowAck and make
		// handlePropose return before any local FastAck can consume this hash.
		dep, hs = c.getDepAndHashesWithUpdate(propose.Command, cmdID, upd, false)
		delete(c.pendingHashUpds, cmdID)
		c.deferredHashElisions++
	} else if deferFast {
		dep, hs = c.getDepAndHashesWithUpdate(propose.Command, cmdID, nil, true)
		c.hashBacklogDeferrals++
	} else {
		dep, hs = c.getDepAndHashes(propose.Command, cmdID)
	}
	if deferred && c.leader() != c.id && !c.sq.Contains(int(c.id)) {
		// TODO: when pipelining this can break ordering, disabling fast paths.
		delete(c.pendingHashUpds, cmdID)
		c.recordLeaderHash(cmdID, upd.seqnum, upd.hash)
	}
	var message interface{} = propose
	if deferFast {
		message = &deferredProposal{propose: propose}
	}
	desc := c.getCmdDescWithHashes(cmdID, message, dep, hs)
	if desc == nil {
		c.Fatal("Got proposal for the delivered command", cmdID)
	}
}

func (c *core) shouldDeferFast(cmd state.Command) bool {
	// Optional speculative reads may need the local proxy's execution/reply.
	// Keep their existing path rather than suppressing that local fast vote.
	if c.fastRead || c.leader() == c.id || !c.sq.Contains(int(c.id)) {
		return false
	}
	for _, key := range keysOf(cmd) {
		if l := c.hlog[key]; l != nil && len(l.nodes) >= 1024 {
			return true
		}
	}
	return false
}

func (c *core) receive(event any, _ time.Time) error {
	switch m := event.(type) {
	case *MFastAck:
		if m == nil {
			return protocol.UnsupportedEvent("swiftpaxos", event)
		}
		fastAck := m
		c.receiveFastAck(fastAck)

	case *MLightSlowAck:
		if m == nil {
			return protocol.UnsupportedEvent("swiftpaxos", event)
		}
		lightSlowAck := m
		c.getCmdDesc(lightSlowAck.CmdId, lightSlowAck, nil)

	case *MAcks:
		if m == nil {
			return protocol.UnsupportedEvent("swiftpaxos", event)
		}
		acks := m
		for _, f := range acks.FastAcks {
			c.receiveFastAck(copyFastAck(&f))
		}
		for _, s := range acks.LightSlowAcks {
			ls := s
			c.getCmdDesc(s.CmdId, &ls, nil)
		}

	case *MOptAcks:
		if m == nil {
			return protocol.UnsupportedEvent("swiftpaxos", event)
		}
		optAcks := m
		for _, ack := range optAcks.Acks {
			if optAcks.Ballot != c.ballot {
				continue
			}
			if IsNilDepOfCmdID(ack.CmdId, ack.Dep) && ack.Seqnum != recoveryAckSeqnum {
				// The batcher's sentinel represents a SlowAck, not hash evidence.
				ls := &MLightSlowAck{Replica: optAcks.Replica, Ballot: optAcks.Ballot, CmdId: ack.CmdId}
				c.getCmdDesc(ls.CmdId, ls, nil)
				continue
			}
			fastAck := newFastAck()
			fastAck.Replica = optAcks.Replica
			fastAck.Ballot = optAcks.Ballot
			fastAck.CmdId = ack.CmdId
			fastAck.Checksum = ack.Checksum
			fastAck.Seqnum = ack.Seqnum
			if !IsNilDepOfCmdID(ack.CmdId, ack.Dep) {
				fastAck.Dep = ack.Dep
			} else {
				fastAck.Dep = nil
			}
			c.receiveFastAck(fastAck)
		}

	case *MNewLeader:
		if m == nil {
			return protocol.UnsupportedEvent("swiftpaxos", event)
		}
		newLeader := m
		c.handleNewLeader(newLeader)

	case *MSync:
		if m == nil {
			return protocol.UnsupportedEvent("swiftpaxos", event)
		}
		sync := m
		c.handleSync(sync)

	case *MNewLeaderAckN:
		if m == nil {
			return protocol.UnsupportedEvent("swiftpaxos", event)
		}
		c.handleNewLeaderAckN(m)
	case recoveryEvent:
		c.beginRecovery(int32(m))
	case leaderCall:
		m.done <- c.handleLeaderCall()
	case defs.RequestID:
		c.getCmdDesc(m, "deliver", nil)
	default:
		return protocol.UnsupportedEvent("swiftpaxos", event)
	}
	return nil
}

func (c *core) tick(t protocol.Tick) error {
	if t.Kind != protocol.Maintenance {
		return protocol.UnsupportedTimer("swiftpaxos", t.Kind)
	}
	// This implementation starts recovery from control events, not a periodic
	// protocol timer. Do not invent a heartbeat or election deadline here.
	return nil
}
