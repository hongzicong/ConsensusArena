package swift

// Event scheduling, protocol integration, and control/status handling.
import (
	"sync"
	"time"

	"github.com/hongzicong/ConsensusArena/protocol"
	"github.com/hongzicong/ConsensusArena/replica"
	"github.com/hongzicong/ConsensusArena/replica/defs"
)

type leaderCall struct{ done chan leaderDecision }

func (r *Replica) BeTheLeader(_ *defs.BeTheLeaderArgs, reply *defs.BeTheLeaderReply) error {
	done := make(chan leaderDecision, 1)
	r.control <- leaderCall{done}
	decision := <-done
	if decision.recover {
		// Keep recovery asynchronous and retain the existing queue admission:
		// the RPC returns after enqueueing, not after recovery completes.
		r.recover <- decision.ballot
	}
	*reply = decision.reply
	return nil
}

func (r *Replica) run() {
	// Swift has no admission-retry callback; retain pending frames per peer.
	r.PeerSendOptions = replica.SenderOptions{Capacity: -1}
	r.ConnectToPeers()
	defer r.CloseSenders()
	latencies := r.ComputeClosestPeers()
	for _, l := range latencies {
		d := time.Duration(l*1000*1000) * time.Nanosecond
		if d > r.channels.maxLatency {
			r.channels.maxLatency = d
		}
	}

	go r.WaitForClientConnections()

	for !r.Shutdown {
		select {
		case call := <-r.control:
			protocol.Must(r.Step(call, time.Time{}))

		case newBallot := <-r.recover:
			protocol.Must(r.Step(recoveryEvent(newBallot), time.Time{}))

		case cmdID := <-r.delivery:
			protocol.Must(r.Step(cmdID, time.Time{}))

		case propose := <-r.ProposeChan:
			protocol.Must(r.Propose(propose, time.Time{}))

		case m := <-r.channels.fastAckChan:
			protocol.Must(r.Step(m, time.Time{}))

		case m := <-r.channels.lightSlowAckChan:
			protocol.Must(r.Step(m, time.Time{}))

		case m := <-r.channels.acksChan:
			protocol.Must(r.Step(m, time.Time{}))

		case m := <-r.channels.optAcksChan:
			protocol.Must(r.Step(m, time.Time{}))

		case m := <-r.channels.newLeaderChan:
			protocol.Must(r.Step(m, time.Time{}))

		case m := <-r.channels.syncChan:
			protocol.Must(r.Step(m, time.Time{}))

		}
	}
}

var _ protocol.Machine = (*Replica)(nil)

type recoveryEvent int32

func (r *Replica) Propose(p *defs.GPropose, _ time.Time) error {
	if err := protocol.ValidateProposal(p); err != nil {
		return err
	}
	additional := len(r.ProposeChan)
	if additional > 255 {
		additional = 255
	}
	r.core.beginProposalBatch(additional + 1)
	r.core.submit(r.admitProposal(p))
	for i := 0; i < additional; i++ {
		r.core.submit(r.admitProposal(<-r.ProposeChan))
	}
	return nil
}
func (r *Replica) Step(event any, now time.Time) error { return r.core.receive(event, now) }
func (r *Replica) Tick(t protocol.Tick) error          { return r.core.tick(t) }

func (r *Replica) allocDesc() *commandDesc {
	if r.descriptorPoolLevel > 0 {
		return r.descriptorPool.Get().(*commandDesc)
	}
	return &commandDesc{}
}

func (r *Replica) freeDesc(desc *commandDesc) {
	clear(desc.pendingLeaderAcks)
	desc.pendingLeaderAcks = nil
	if r.descriptorPoolLevel > 0 {
		r.descriptorPool.Put(desc)
	}
}

func (r *Replica) runDescriptor(desc *commandDesc, cmdID defs.RequestID) {
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
			if r.core.handleMsg(msg, desc, cmdID) {
				r.descriptorCount--
				return
			}
		}
	}
}

func (r *Replica) awaitRecovery() {
	// stop processing normal channels:
	for r.core.status == RECOVERING {
		select {
		case call := <-r.control:
			protocol.Must(r.Step(call, time.Time{}))

		case m := <-r.channels.newLeaderChan:
			protocol.Must(r.Step(m, time.Time{}))

		case m := <-r.channels.newLeaderAckNChan:
			protocol.Must(r.Step(m, time.Time{}))

		case m := <-r.channels.syncChan:
			protocol.Must(r.Step(m, time.Time{}))
		}
	}
}

func (r *Replica) stopDescriptors() {
	var wg sync.WaitGroup
	r.core.descriptors.IterCb(func(_ string, v interface{}) {
		desc := v.(*commandDesc)
		if desc.active && !desc.seq {
			wg.Add(1)
			desc.stopChan <- &wg
		}
	})
	wg.Wait()

	// TODO: maybe add to history even if stopped this way ?
}

func (r *Replica) awaitReply(desc *commandDesc, cmdID defs.RequestID) {
	// wait for the slot number and ignore any other message
	for {
		switch slot := (<-desc.msgs).(type) {
		case int:
			r.core.handleMsg(slot, desc, cmdID)
			return
		}
	}
}

func (r *Replica) admitProposal(p *defs.GPropose) *proposal {
	return &proposal{Propose: p.Propose, Proxy: p.Proxy, Addr: p.Addr, context: p}
}
func (r *Replica) requeueProposal(p *proposal) {
	go func() { r.ProposeChan <- p.context.(*defs.GPropose) }()
}
func (r *Replica) scheduleDelivery(id defs.RequestID) {
	go func() { r.delivery <- id }()
}
func (r *Replica) startDescriptor(desc *commandDesc, id defs.RequestID) {
	go r.runDescriptor(desc, id)
	r.descriptorCount++
}
