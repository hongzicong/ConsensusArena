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
)

// Replica owns runtime resources and one protocol core.
type Replica struct {
	*replica.Replica
	core                *core
	channels            transportChannels
	control             chan leaderCall
	recover             chan int32
	localReady          chan struct{}
	batcher             *messageBatcher
	descriptorPool      sync.Pool
	descriptorPoolLevel int
}

func New(alias string, replicaID int, addrs []string, executeEnabled, fastRead, optExec bool,
	descriptorPoolLevel, f int, conf *config.Config, logger *dlog.Logger, slowAddrs []string) *Replica {
	r := &Replica{Replica: replica.New(alias, replicaID, f, addrs, executeEnabled, conf, logger), control: make(chan leaderCall, 8), recover: make(chan int32, 8), localReady: make(chan struct{}, 1), descriptorPoolLevel: descriptorPoolLevel, descriptorPool: sync.Pool{New: func() interface{} { return &commandDesc{} }}}
	initial := replicaset.New()
	for _, member := range conf.Plan.FastQuorum {
		initial.Add(member.Rank)
	}

	r.core = newCore(corePorts{
		n: r.N, id: r.ID, Logger: r.Logger, codes: &r.channels.rpcCodes,
		send:                func(id int, code uint8, msg fastrpc.Serializable) error { return r.Messages().Send(id, code, msg) },
		broadcast:           func(msg fastrpc.Serializable, code uint8) int { return r.SendToAll(msg, code) },
		sendClient:          r.SendClientMsg,
		peerAlive:           r.peerAlive,
		executeEnabled:      func() bool { return r.ExecuteEnabled },
		replyAfterExecution: func() bool { return r.ReplyAfterExecution },
		allocDesc:           r.allocDesc, freeDesc: r.freeDesc,
	}, fastRead, optExec, slowAddrs, initial, conf.Plan.LeaderID())
	r.core.execute = func(cmd state.Command) state.Value { return cmd.Execute(r.State) }

	r.batcher = newMessageBatcher(r, 16)
	r.core.batcher = r.batcher

	// Include recovery-only input, but leave client-only messages unsubscribed.
	r.channels = transportChannels{
		fastAckChan:       make(chan fastrpc.Serializable, defs.CHAN_BUFFER_SIZE),
		lightSlowAckChan:  make(chan fastrpc.Serializable, defs.CHAN_BUFFER_SIZE),
		acksChan:          make(chan fastrpc.Serializable, defs.CHAN_BUFFER_SIZE),
		optAcksChan:       make(chan fastrpc.Serializable, defs.CHAN_BUFFER_SIZE),
		newLeaderChan:     make(chan fastrpc.Serializable, defs.CHAN_BUFFER_SIZE),
		newLeaderAckNChan: make(chan fastrpc.Serializable, defs.CHAN_BUFFER_SIZE),
		syncChan:          make(chan fastrpc.Serializable, defs.CHAN_BUFFER_SIZE),
	}
	registerRPCs(&r.channels, r.RPC)

	r.Println("the leader is:", r.core.leader(), "ballot is:", r.core.ballot)

	r.Println("SQ:", r.core.sq)
	r.Println("FQ:", r.core.fq)

	go r.run()

	return r
}

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
		proposals, recoveries := r.ProposeChan, r.recover
		fast, slow := r.channels.fastAckChan, r.channels.lightSlowAckChan
		acks, optAcks := r.channels.acksChan, r.channels.optAcksChan
		local := r.localReady
		var recoveryAcks chan fastrpc.Serializable
		if r.core.status == RECOVERING {
			// Preserve the old recovery loop's input gate without nesting a
			// blocking loop inside a protocol transition.
			proposals, recoveries, fast, slow, acks, optAcks, local = nil, nil, nil, nil, nil, nil, nil
			recoveryAcks = r.channels.newLeaderAckNChan
		}
		select {
		case call := <-r.control:
			protocol.Must(r.Step(call, time.Time{}))

		case newBallot := <-recoveries:
			protocol.Must(r.Step(recoveryEvent(newBallot), time.Time{}))

		case <-local:
			r.drain()

		case propose := <-proposals:
			protocol.Must(r.Propose(propose, time.Time{}))

		case m := <-fast:
			protocol.Must(r.Step(m, time.Time{}))

		case m := <-slow:
			protocol.Must(r.Step(m, time.Time{}))

		case m := <-acks:
			protocol.Must(r.Step(m, time.Time{}))

		case m := <-optAcks:
			protocol.Must(r.Step(m, time.Time{}))

		case m := <-r.channels.newLeaderChan:
			protocol.Must(r.Step(m, time.Time{}))

		case m := <-r.channels.syncChan:
			protocol.Must(r.Step(m, time.Time{}))

		case m := <-recoveryAcks:
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
	if r.core.status == RECOVERING {
		r.core.requeueProposal(r.admitProposal(p))
		return nil
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
	r.drain()
	return nil
}

func (r *Replica) Step(event any, now time.Time) error {
	if err := r.core.receive(event, now); err != nil {
		return err
	}
	r.drain()
	return nil
}

func (r *Replica) Tick(t protocol.Tick) error {
	if err := r.core.tick(t); err != nil {
		return err
	}
	r.drain()
	return nil
}

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

func (r *Replica) admitProposal(p *defs.GPropose) *proposal {
	return &proposal{Propose: p.Propose, Proxy: p.Proxy, Addr: p.Addr, context: p}
}

func (r *Replica) drain() {
	r.core.drainLocalWork(localWorkBudget)
	r.batcher.flush()
	if r.core.status == NORMAL && r.core.hasLocalWork() {
		select {
		case r.localReady <- struct{}{}:
		default:
		}
	}
}

func (r *Replica) peerAlive(id int) bool {
	r.M.Lock()
	defer r.M.Unlock()
	return r.Alive[id]
}
