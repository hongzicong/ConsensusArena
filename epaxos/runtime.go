package epaxos

// Event scheduling, protocol integration, and control/status handling.
import (
	"time"

	"github.com/hongzicong/ConsensusArena/protocol"
	"github.com/hongzicong/ConsensusArena/replica/defs"
)

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
	defer r.CloseSenders()
	r.sends = r.PeerSenders

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
			protocol.Must(r.Propose(propose, time.Time{}))
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
			protocol.Must(r.Handle(prepareS, time.Time{}))
			break

		case preAcceptS := <-r.preAcceptChan:
			protocol.Must(r.Handle(preAcceptS, time.Time{}))
			break

		case acceptS := <-r.acceptChan:
			protocol.Must(r.Handle(acceptS, time.Time{}))
			break

		case commitS := <-r.commitChan:
			protocol.Must(r.Handle(commitS, time.Time{}))
			break

		case prepareReplyS := <-r.prepareReplyChan:
			protocol.Must(r.Handle(prepareReplyS, time.Time{}))
			break

		case preAcceptReplyS := <-r.preAcceptReplyChan:
			protocol.Must(r.Handle(preAcceptReplyS, time.Time{}))
			break

		case acceptReplyS := <-r.acceptReplyChan:
			protocol.Must(r.Handle(acceptReplyS, time.Time{}))
			break

		case tryPreAcceptS := <-r.tryPreAcceptChan:
			protocol.Must(r.Handle(tryPreAcceptS, time.Time{}))
			break

		case tryPreAcceptReplyS := <-r.tryPreAcceptReplyChan:
			protocol.Must(r.Handle(tryPreAcceptReplyS, time.Time{}))
			break

		case beacon := <-r.BeaconChan:
			protocol.Must(r.Handle(beacon, time.Time{}))
			break

		case <-slowClockChan:
			protocol.Must(r.Tick(protocol.Tick{Kind: protocol.Slow}))

		case now := <-execTicker.C:
			protocol.Must(r.Tick(protocol.Tick{Now: now}))
			if r.crtInstance[r.Id]-r.ExecedUpTo[r.Id] >= MAX_INFLIGHT_INSTANCES {
				onOffProposeChan = nil
			} else if !r.BatchingEnabled() {
				onOffProposeChan = r.ProposeChan
			}

		case <-progressTicker.C:
			r.logProgress()
			break

		case req := <-r.requestChan:
			protocol.Must(r.Handle(req, time.Time{}))

		case iid := <-r.instancesToRecover:
			protocol.Must(r.Handle(iid, time.Time{}))
		}
	}
}

func (r *Replica) BeTheLeader(args *defs.BeTheLeaderArgs, reply *defs.BeTheLeaderReply) error {
	r.IsLeader = true
	r.Println("I am the leader")
	return nil
}

func (r *Replica) peerAlive(id int32) bool {
	r.M.Lock()
	defer r.M.Unlock()
	return id == r.Id || r.Alive[id]
}

var _ protocol.Machine = (*Replica)(nil)

func (r *Replica) Propose(p *defs.GPropose, _ time.Time) error {
	if err := protocol.ValidateProposal(p); err != nil {
		return err
	}
	r.handlePropose(p)
	return nil
}

func (r *Replica) Handle(event any, _ time.Time) error {
	switch m := event.(type) {
	case *Prepare:
		if m == nil {
			return protocol.UnsupportedEvent("epaxos", event)
		}
		r.handlePrepare(m)
	case *PreAccept:
		if m == nil {
			return protocol.UnsupportedEvent("epaxos", event)
		}
		r.handlePreAccept(m)
	case *Accept:
		if m == nil {
			return protocol.UnsupportedEvent("epaxos", event)
		}
		r.handleAccept(m)
	case *Commit:
		if m == nil {
			return protocol.UnsupportedEvent("epaxos", event)
		}
		r.handleCommit(m)
	case *PrepareReply:
		if m == nil {
			return protocol.UnsupportedEvent("epaxos", event)
		}
		r.handlePrepareReply(m)
	case *PreAcceptReply:
		if m == nil {
			return protocol.UnsupportedEvent("epaxos", event)
		}
		r.handlePreAcceptReply(m)
	case *AcceptReply:
		if m == nil {
			return protocol.UnsupportedEvent("epaxos", event)
		}
		r.handleAcceptReply(m)
	case *TryPreAccept:
		if m == nil {
			return protocol.UnsupportedEvent("epaxos", event)
		}
		r.handleTryPreAccept(m)
	case *TryPreAcceptReply:
		if m == nil {
			return protocol.UnsupportedEvent("epaxos", event)
		}
		r.handleTryPreAcceptReply(m)
	case *repairRequest:
		if m == nil {
			return protocol.UnsupportedEvent("epaxos", event)
		}
		r.handleRepairRequest(m)
	case *defs.GBeacon:
		if m == nil {
			return protocol.UnsupportedEvent("epaxos", event)
		}
		r.ReplyBeacon(m)
	case *instanceId:
		if m == nil {
			return protocol.UnsupportedEvent("epaxos", event)
		}
		r.startRecoveryForInstance(m.replica, m.instance)
	default:
		return protocol.UnsupportedEvent("epaxos", event)
	}
	return nil
}

func (r *Replica) Tick(t protocol.Tick) error {
	switch t.Kind {
	case protocol.Maintenance:
		r.repairTick(t.Now)
		if r.Exec {
			r.executeReady(t.Now)
		}
	case protocol.Slow:
		r.beaconTick()
	default:
		return protocol.UnsupportedTimer("epaxos", t.Kind)
	}
	return nil
}

func (r *Replica) beaconTick() {
	if r.Beacon {
		r.Printf("weird %d; conflicted %d; slow %d; fast %d\n", r.Stats.M["weird"], r.Stats.M["conflicted"], r.Stats.M["slow"], r.Stats.M["fast"])
		for q := int32(0); q < int32(r.N); q++ {
			if q == r.Id {
				continue
			}
			r.SendBeacon(q)
		}
	}

}
