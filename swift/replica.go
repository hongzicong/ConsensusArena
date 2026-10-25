package swift

import (
	"sync"
	"time"

	"github.com/hongzicong/ConsensusArena/config"
	"github.com/hongzicong/ConsensusArena/dlog"
	"github.com/hongzicong/ConsensusArena/protocol"
	"github.com/hongzicong/ConsensusArena/replica"
	"github.com/hongzicong/ConsensusArena/replica/defs"
	"github.com/hongzicong/ConsensusArena/replicaset"
	fastrpc "github.com/hongzicong/ConsensusArena/rpc"
	"github.com/hongzicong/ConsensusArena/state"
	cmap "github.com/orcaman/concurrent-map"
)

// Replica owns runtime resources and one protocol core.
type Replica struct {
	*replica.Replica
	core                *core
	channels            transportChannels
	control             chan leaderCall
	recover             chan int32
	delivery            chan defs.RequestID
	replyWorker         *replyWorker
	descriptorPool      sync.Pool
	descriptorPoolLevel int
	descriptorCount     int
}

func New(alias string, replicaID int, addrs []string, executeEnabled, fastRead, optExec bool,
	descriptorPoolLevel, f int, conf *config.Config, logger *dlog.Logger, slowAddrs []string) *Replica {
	cmap.SHARD_COUNT = 32768

	r := &Replica{Replica: replica.New(alias, replicaID, f, addrs, executeEnabled, conf, logger), control: make(chan leaderCall, 8), recover: make(chan int32, 8), delivery: make(chan defs.RequestID, defs.CHAN_BUFFER_SIZE), descriptorPoolLevel: descriptorPoolLevel, descriptorPool: sync.Pool{New: func() interface{} { return &commandDesc{} }}}
	initial := replicaset.New()
	for _, member := range conf.Plan.FastQuorum {
		initial.Add(member.Rank)
	}

	r.core = newCore(corePorts{
		n: r.N, id: r.ID, Logger: r.Logger, codes: &r.channels.rpcCodes,
		send:                func(id int, code uint8, msg fastrpc.Serializable) error { return r.Messages().Send(id, code, msg) },
		broadcast:           func(msg fastrpc.Serializable, code uint8) int { return r.SendToAll(msg, code) },
		sendClient:          r.SendClientMsg,
		peerAlive:           func(id int) bool { return r.Alive[id] },
		executeEnabled:      func() bool { return r.ExecuteEnabled },
		replyAfterExecution: func() bool { return r.ReplyAfterExecution },
		requeueProposal:     r.requeueProposal, scheduleDelivery: r.scheduleDelivery,
		startDescriptor: r.startDescriptor, stopDescriptors: r.stopDescriptors, resetDescriptors: func() { r.descriptorCount = 0 },
		sequentialDescriptor: func() bool { return r.descriptorCount >= MaxDescRoutines }, allocDesc: r.allocDesc, freeDesc: r.freeDesc,
		stopReplies: func() { r.replyWorker.stop() }, resetReplies: func() { r.replyWorker = newReplyWorker(r) },
		queueReply: func(d *commandDesc, id defs.RequestID, v state.Value) { r.replyWorker.reply(d, id, v) },
		awaitReply: r.awaitReply, awaitRecovery: r.awaitRecovery,
	}, fastRead, optExec, slowAddrs, initial, conf.Plan.LeaderID())
	r.core.execute = func(cmd state.Command) state.Value { return cmd.Execute(r.State) }

	r.core.batcher = newMessageBatcher(r, 16)
	r.replyWorker = newReplyWorker(r)

	registerRPCs(&r.channels, r.RPC)

	r.Println("the leader is:", r.core.leader(), "ballot is:", r.core.ballot)

	r.Println("SQ:", r.core.sq)
	r.Println("FQ:", r.core.fq)

	go r.run()

	return r
}

var MaxDescRoutines = 100

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

func (r *Replica) Tick(t protocol.Tick) error { return r.core.tick(t) }

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
