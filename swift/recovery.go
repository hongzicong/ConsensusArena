package swift

// Protocol recovery transitions.
import (
	"log"
	"sort"
	"time"

	"github.com/hongzicong/ConsensusArena/replica/defs"
	"github.com/hongzicong/ConsensusArena/state"
	cmap "github.com/orcaman/concurrent-map"
)

func (c *core) handleNewLeader(msg *MNewLeader) {
	if c.ballot >= msg.Ballot {
		return
	}
	log.Println("Recovering... with the ballot", msg.Ballot)

	c.status = RECOVERING
	c.ballot = msg.Ballot
	c.recStart = time.Now()

	c.stopReplies()
	c.stopDescriptors()

	newLeaderAckN := &MNewLeaderAckN{
		Replica: c.id,
		Ballot:  c.ballot,
		Cballot: c.cballot,
	}
	c.fillNewLeaderAckN(newLeaderAckN)

	if msg.Replica != c.id {
		_ = c.send(int(msg.Replica), c.codes.newLeaderAckNRPC, newLeaderAckN)
	} else {
		c.handleNewLeaderAckN(newLeaderAckN)
	}

	c.awaitRecovery()
}

func (c *core) handleNewLeaderAckN(msg *MNewLeaderAckN) {
	if c.status != RECOVERING || c.ballot != msg.Ballot {
		return
	}

	c.newLeaderAckNs.Add(msg.Replica, false, msg)
}

func (c *core) handleNewLeaderAckNs(_ interface{}, msgs []interface{}) {
	maxCbal := int32(-1)
	var U map[*MNewLeaderAckN]struct{}

	for _, msg := range msgs {
		newLeaderAck := msg.(*MNewLeaderAckN)
		if maxCbal < newLeaderAck.Cballot {
			U = make(map[*MNewLeaderAckN]struct{})
			maxCbal = newLeaderAck.Cballot
		}
		if maxCbal == newLeaderAck.Cballot {
			U[newLeaderAck] = struct{}{}
		}
	}

	phases := make(map[defs.RequestID]int)
	cmds := make(map[defs.RequestID]state.Command)
	deps := make(map[defs.RequestID]Dep)

	for newLeaderAckN := range U {
		for i, phase := range newLeaderAckN.Phases {
			if phase == COMMIT || phase == ACCEPT {
				cmdID := newLeaderAckN.CmdIds[i]
				phases[cmdID] = phase
				cmds[cmdID] = newLeaderAckN.Cmds[i]
				deps[cmdID] = Dep(newLeaderAckN.Deps[i].Dep)
			}
		}
	}

	sync := &MSync{Replica: c.id, Ballot: c.ballot, PhasesPresent: phases != nil, CmdsPresent: cmds != nil, DepsPresent: deps != nil}
	for id, phase := range phases {
		sync.Phases = append(sync.Phases, MSyncPhasesEntry{Key: id, Value: phase})
	}
	for id, cmd := range cmds {
		sync.Cmds = append(sync.Cmds, MSyncCmdsEntry{Key: id, Value: cmd})
	}
	for id, dep := range deps {
		sync.Deps = append(sync.Deps, MSyncDepsEntry{Key: id, Value: dep})
	}
	c.broadcast(sync, c.codes.syncRPC)
	c.handleSync(sync)
}

func (c *core) fillNewLeaderAckN(newLeaderAckN *MNewLeaderAckN) {
	cmdIDs := []defs.RequestID{}
	phases := []int{}
	cmds := []state.Command{}
	deps := []SDep{}
	seen := make(map[defs.RequestID]struct{})

	c.descriptors.IterCb(func(_ string, v interface{}) {
		desc := v.(*commandDesc)
		if desc.propose != nil {
			cmdID := desc.propose.RequestID()
			if _, exists := seen[cmdID]; !exists {
				seen[cmdID] = struct{}{}
				cmdIDs = append(cmdIDs, cmdID)
				phases = append(phases, desc.phase)
				cmds = append(cmds, desc.cmd)
				deps = append(deps, SDep{desc.dep})
			}
		}
	})

	for cmdID := range c.proposes {
		if _, exists := seen[cmdID]; exists || c.delivered.Has(cmdID.String()) {
			continue
		}
		cmdIDs = append(cmdIDs, cmdID)
		phases = append(phases, ACCEPT)
		cmds = append(cmds, state.NOOP()[0])
		deps = append(deps, SDep{[]defs.RequestID{}})
	}

	newLeaderAckN.CmdIds = cmdIDs
	newLeaderAckN.Phases = phases
	newLeaderAckN.Cmds = cmds
	newLeaderAckN.Deps = deps
}

func (c *core) handleSync(msg *MSync) {
	if c.ballot > msg.Ballot || (c.ballot == msg.Ballot && c.status == NORMAL) {
		return
	}
	phases, cmds, deps := msg.recoveryIndex()

	if c.status == NORMAL {
		c.recStart = time.Now()
		c.stopReplies()
		c.stopDescriptors()
	}

	// clear descriptors:
	c.descriptors.IterCb(func(_ string, v interface{}) {
		desc := v.(*commandDesc)
		if desc.propose != nil {
			cmdID := desc.propose.RequestID()
			if _, exists := phases[cmdID]; !exists {
				c.requeueProposal(desc.propose)
			}
		}
		desc.msgs = nil
		desc.stopChan = nil
		desc.slowPathH.Free()
		desc.fastPathH.Free()
		c.freeDesc(desc)
	})

	c.keys = make(map[state.Key]keyInfo)
	c.resetDescriptors()
	c.descriptors = cmap.New()
	c.status = NORMAL
	c.ballot = msg.Ballot
	c.cballot = msg.Ballot
	c.installRecoveryHashBoundary(phases)
	if c.fixedMajority {
		c.fq = c.qs.AQ(c.ballot)
	}
	c.resetReplies()
	c.historySize = 0

	i := 0
	sorted := make([]defs.RequestID, len(phases))
	for cmdID := range phases {
		sorted[i] = cmdID
		i++
	}
	sort.Slice(sorted, func(i, j int) bool {
		return deps[sorted[j]].Contains(sorted[i])
	})

	for _, cmdID := range sorted {
		desc := c.getCmdDesc(cmdID, nil, nil)
		if desc != nil {
			desc.phase = phases[cmdID]
			desc.cmd = cmds[cmdID]
			desc.dep = deps[cmdID]
			desc.proposeDep = deps[cmdID]

			for _, cmdIDPrime := range deps[cmdID] {
				descPrime := c.getCmdDesc(cmdIDPrime, nil, nil)
				if descPrime != nil {
					descPrime.successors = append(descPrime.successors, cmdID)
				}
				c.scheduleDelivery(cmdIDPrime)
			}

			c.scheduleDelivery(cmdID)

			if desc.phase != COMMIT && desc.phase != ACCEPT {
				desc.phase = ACCEPT
			}
		}

		if propose, exists := c.proposes[cmdID]; exists {
			if desc != nil {
				desc.propose = propose
			}

			finishAck := c.sendRecoveryAck(cmdID, deps[cmdID], propose, desc)
			defer finishAck()
		}
	}

	for cmdID, propose := range c.proposes {
		if _, exists := phases[cmdID]; exists {
			continue
		}
		acc := &MAccept{
			Replica: c.id,
			Ballot:  c.ballot,
			CmdId:   cmdID,
		}
		c.sendClient(propose.ClientId, c.codes.acceptRPC, acc)
	}

	log.Println("Recovered!")
	log.Println("Ballot:", c.ballot)
	log.Println("FQ:", c.fq, "SQ:", c.sq)
	log.Println("recovered in", time.Now().Sub(c.recStart))
}

func (c *core) reinitNewLeaderAckNs() {
	accept := func(_, _ interface{}) bool {
		return true
	}
	free := func(_ interface{}) {}
	Q := NewMajorityOf(c.n)
	c.newLeaderAckNs = c.newLeaderAckNs.ReinitMsgSet(Q, accept, free, c.handleNewLeaderAckNs)
}

type leaderDecision struct {
	reply   defs.BeTheLeaderReply
	recover bool
	ballot  int32
}

func (c *core) handleLeaderCall() leaderDecision {
	decision := leaderDecision{}
	if !c.delivered.IsEmpty() {
		decision.recover = true
		decision.ballot = c.qs.BallotAt(1)
		decision.reply.Leader = c.id
	} else {
		decision.reply.Leader = c.leader()
	}
	decision.reply.NextLeader = Leader(c.qs.BallotAt(1), c.n)
	if decision.reply.Leader == 0 {
		decision.reply.Leader = -2
	}
	if decision.reply.NextLeader == 0 {
		decision.reply.NextLeader = -2
	}
	return decision
}

func (c *core) beginRecovery(newBallot int32) {
	newLeader := &MNewLeader{
		Replica: c.id,
		Ballot:  c.ballot,
	}
	if newBallot != -1 {
		if newBallot > newLeader.Ballot {
			newLeader.Ballot = newBallot
		} else {
			newLeader.Ballot = c.qs.SameHigher(newBallot, newLeader.Ballot)
		}
	} else {
		newLeader.Ballot = NextBallotOf(c.id, newLeader.Ballot, c.n)
	}
	for quorumIsAlive := false; c.fixedMajority && !quorumIsAlive; {
		quorumIsAlive = true
		c.qs.AQ(newLeader.Ballot).Range(func(rid int) bool {
			if rid != int(c.id) && !c.peerAlive(rid) {
				newLeader.Ballot = NextBallotOf(c.id, newLeader.Ballot, c.n)
				quorumIsAlive = false
				return false
			}
			return true
		})
	}
	c.broadcast(newLeader, c.codes.newLeaderRPC)
	c.reinitNewLeaderAckNs()
	c.handleNewLeader(newLeader)

}
