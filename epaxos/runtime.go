package epaxos

// Event scheduling, protocol integration, and control/status handling.
import (
	"encoding/binary"
	"github.com/hongzicong/ConsensusArena/protocol"
	"github.com/hongzicong/ConsensusArena/replica/defs"
	"github.com/hongzicong/ConsensusArena/state"
	"io"
	"time"
)

func (r *Replica) fastClock() {
	for !r.Shutdown {
		time.Sleep(time.Duration(r.core.batchWait) * time.Millisecond)
		r.fastClockChan <- true
	}
}

func (r *Replica) slowClock() {
	for !r.Shutdown {
		time.Sleep(150 * time.Millisecond)
		r.slowClockChan <- true
	}
}

func (r *Replica) observeRTT(peer int, rtt time.Duration) {
	r.EWMA[peer] = 0.99*r.EWMA[peer] + 0.01*float64(rtt)
}

func (r *Replica) stopAdapting() {
	time.Sleep(1000 * 1000 * 1000 * ADAPT_TIME_SEC)
	r.Beacon = false
	time.Sleep(1000 * 1000 * 1000)

	for i := 0; i < r.N-1; i++ {
		min := i
		for j := i + 1; j < r.N-1; j++ {
			if r.EWMA[r.PreferredPeerOrder[j]] < r.EWMA[r.PreferredPeerOrder[min]] {
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
		proposed, batches, maxBatch, maxProposalQueue, fast, slow, executed, replies, recoveryScheduled, recoverySuppressed, len(r.core.instancesToRecover))
	r.Printf("EPAXOS_REPAIR active=%d blocked=%d send_drops=%d executed_prefix=%v known_prefix=%v", len(r.core.active), len(r.core.blocked), r.Stats.M["sendQueueDrops"], r.core.ExecutedUpTo, r.core.crtInstance)
	for q := int32(0); q < int32(r.N); q++ {
		slot := r.core.ExecutedUpTo[q] + 1
		if slot > r.core.crtInstance[q] {
			continue
		}
		inst := r.core.InstanceSpace[q][slot]
		if inst != nil {
			r.Printf("EPAXOS_HEAD owner=%d slot=%d status=%d promise=%d value_ballot=%d deps=%v", q, slot, inst.Status, inst.bal, inst.vbal, inst.Deps)
		}
	}
}

func (r *Replica) run() {
	r.ConnectToPeers()
	defer r.CloseSenders()

	r.ComputeClosestPeers()

	execTicker := time.NewTicker(2 * time.Millisecond)
	defer execTicker.Stop()

	r.slowClockChan = make(chan bool, 1)
	r.fastClockChan = make(chan bool, 1)
	go r.slowClock()

	if r.core.BatchingEnabled() {
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
		case call := <-r.control:
			protocol.Must(r.Handle(call, time.Time{}))

		case propose := <-onOffProposeChan:
			protocol.Must(r.Propose(propose, time.Time{}))
			if r.core.BatchingEnabled() || r.core.crtInstance[r.ID]-r.core.ExecutedUpTo[r.ID] >= MAX_INFLIGHT_INSTANCES {
				onOffProposeChan = nil
			}
			break

		case <-r.fastClockChan:
			if r.core.crtInstance[r.ID]-r.core.ExecutedUpTo[r.ID] < MAX_INFLIGHT_INSTANCES {
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

		case <-r.slowClockChan:
			protocol.Must(r.Tick(protocol.Tick{Kind: protocol.Slow}))

		case now := <-execTicker.C:
			protocol.Must(r.Tick(protocol.Tick{Now: now}))
			if r.core.crtInstance[r.ID]-r.core.ExecutedUpTo[r.ID] >= MAX_INFLIGHT_INSTANCES {
				onOffProposeChan = nil
			} else if !r.core.BatchingEnabled() {
				onOffProposeChan = r.ProposeChan
			}

		case <-progressTicker.C:
			r.logProgress()
			break

		case req := <-r.requestChan:
			protocol.Must(r.Handle(req, time.Time{}))

		case iid := <-r.core.instancesToRecover:
			protocol.Must(r.Handle(iid, time.Time{}))
		}
	}
}

type leaderCall struct{ done chan struct{} }

func (r *Replica) BeTheLeader(_ *defs.BeTheLeaderArgs, _ *defs.BeTheLeaderReply) error {
	done := make(chan struct{}, 1)
	r.control <- leaderCall{done}
	<-done
	return nil
}

func (r *Replica) peerAlive(id int32) bool {
	r.M.Lock()
	defer r.M.Unlock()
	return id == r.ID || r.Alive[id]
}

var _ protocol.Machine = (*Replica)(nil)

func (r *Replica) beaconTick() {
	if r.Beacon {
		r.Printf("weird %d; conflicted %d; slow %d; fast %d\n", r.Stats.M["weird"], r.Stats.M["conflicted"], r.Stats.M["slow"], r.Stats.M["fast"])
		for q := int32(0); q < int32(r.N); q++ {
			if q == r.ID {
				continue
			}
			r.SendBeacon(q)
		}
	}

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

func (r *Replica) Propose(p *defs.GPropose, _ time.Time) error {
	if err := protocol.ValidateProposal(p); err != nil {
		return err
	}
	count := r.core.beginProposalBatch(len(r.ProposeChan))
	batch := make([]*proposal, count)
	batch[0] = r.admitProposal(p)
	for i := 1; i < count; i++ {
		batch[i] = r.admitProposal(<-r.ProposeChan)
	}
	r.core.submit(batch)
	return nil
}
func (r *Replica) Handle(event any, now time.Time) error { return r.core.receive(event, now) }
func (r *Replica) Tick(t protocol.Tick) error            { return r.core.tick(t) }

func (r *Replica) admitProposal(p *defs.GPropose) *proposal {
	return &proposal{Propose: p.Propose, context: p}
}
func (r *Replica) requeueProposal(p *proposal) { r.ProposeChan <- p.context.(*defs.GPropose) }
func (r *Replica) replyResult(p *proposal, v state.Value, capacity int) error {
	return r.ReplyResult(p.context.(*defs.GPropose), v, capacity)
}
