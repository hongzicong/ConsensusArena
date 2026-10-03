package swift

// Event scheduling, protocol integration, and control/status handling.
import (
	"time"

	"github.com/hongzicong/ConsensusArena/protocol"
	"github.com/hongzicong/ConsensusArena/replica"
	"github.com/hongzicong/ConsensusArena/replica/defs"
)

// TODO: do something more elegant
func (r *Replica) BeTheLeader(_ *defs.BeTheLeaderArgs, reply *defs.BeTheLeaderReply) error {
	if !r.delivered.IsEmpty() {
		b := r.qs.BallotAt(1)
		r.recover <- b
		reply.Leader = r.Id
	} else {
		reply.Leader = r.leader()
	}
	reply.NextLeader = replica.Leader(r.qs.BallotAt(1), r.N)
	if reply.Leader == 0 {
		reply.Leader = -2
	}
	if reply.NextLeader == 0 {
		reply.NextLeader = -2
	}
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
		if d > r.cs.maxLatency {
			r.cs.maxLatency = d
		}
	}

	go r.WaitForClientConnections()

	for !r.Shutdown {
		select {
		case newBallot := <-r.recover:
			protocol.Must(r.Handle(recoveryEvent(newBallot), time.Time{}))

		case cmdId := <-r.deliverChan:
			protocol.Must(r.Handle(cmdId, time.Time{}))

		case propose := <-r.ProposeChan:
			protocol.Must(r.Propose(propose, time.Time{}))

		case m := <-r.cs.fastAckChan:
			protocol.Must(r.Handle(m, time.Time{}))

		case m := <-r.cs.lightSlowAckChan:
			protocol.Must(r.Handle(m, time.Time{}))

		case m := <-r.cs.acksChan:
			protocol.Must(r.Handle(m, time.Time{}))

		case m := <-r.cs.optAcksChan:
			protocol.Must(r.Handle(m, time.Time{}))

		case m := <-r.cs.newLeaderChan:
			protocol.Must(r.Handle(m, time.Time{}))

		case m := <-r.cs.syncChan:
			protocol.Must(r.Handle(m, time.Time{}))

		}
	}
}

var _ protocol.Machine = (*Replica)(nil)

type recoveryEvent int32

func (r *Replica) Propose(p *defs.GPropose, _ time.Time) error {
	if err := protocol.ValidateProposal(p); err != nil {
		return err
	}
	r.handleProposalBatch(p)
	return nil
}

func (r *Replica) Handle(event any, _ time.Time) error {
	switch m := event.(type) {
	case *MFastAck:
		if m == nil {
			return protocol.UnsupportedEvent("swiftpaxos", event)
		}
		fastAck := m
		r.receiveFastAck(fastAck)

	case *MLightSlowAck:
		if m == nil {
			return protocol.UnsupportedEvent("swiftpaxos", event)
		}
		lightSlowAck := m
		r.getCmdDesc(lightSlowAck.CmdId, lightSlowAck, nil)

	case *MAcks:
		if m == nil {
			return protocol.UnsupportedEvent("swiftpaxos", event)
		}
		acks := m
		for _, f := range acks.FastAcks {
			r.receiveFastAck(copyFastAck(&f))
		}
		for _, s := range acks.LightSlowAcks {
			ls := s
			r.getCmdDesc(s.CmdId, &ls, nil)
		}

	case *MOptAcks:
		if m == nil {
			return protocol.UnsupportedEvent("swiftpaxos", event)
		}
		optAcks := m
		for _, ack := range optAcks.Acks {
			if optAcks.Ballot != r.ballot {
				continue
			}
			if IsNilDepOfCmdId(ack.CmdId, ack.Dep) && ack.Seqnum != recoveryAckSeqnum {
				// The batcher's sentinel represents a SlowAck, not hash evidence.
				ls := &MLightSlowAck{Replica: optAcks.Replica, Ballot: optAcks.Ballot, CmdId: ack.CmdId}
				r.getCmdDesc(ls.CmdId, ls, nil)
				continue
			}
			fastAck := newFastAck()
			fastAck.Replica = optAcks.Replica
			fastAck.Ballot = optAcks.Ballot
			fastAck.CmdId = ack.CmdId
			fastAck.Checksum = ack.Checksum
			fastAck.Seqnum = ack.Seqnum
			if !IsNilDepOfCmdId(ack.CmdId, ack.Dep) {
				fastAck.Dep = ack.Dep
			} else {
				fastAck.Dep = nil
			}
			r.receiveFastAck(fastAck)
		}

	case *MNewLeader:
		if m == nil {
			return protocol.UnsupportedEvent("swiftpaxos", event)
		}
		newLeader := m
		r.handleNewLeader(newLeader)

	case *MSync:
		if m == nil {
			return protocol.UnsupportedEvent("swiftpaxos", event)
		}
		sync := m
		r.handleSync(sync)

	case *MNewLeaderAckN:
		if m == nil {
			return protocol.UnsupportedEvent("swiftpaxos", event)
		}
		r.handleNewLeaderAckN(m)
	case recoveryEvent:
		r.beginRecovery(int32(m))
	case CommandId:
		r.getCmdDesc(m, "deliver", nil)
	default:
		return protocol.UnsupportedEvent("swiftpaxos", event)
	}
	return nil
}

func (r *Replica) Tick(t protocol.Tick) error {
	if t.Kind != protocol.Maintenance {
		return protocol.UnsupportedTimer("swiftpaxos", t.Kind)
	}
	// This implementation starts recovery from control events, not a periodic
	// protocol timer. Do not invent a heartbeat or election deadline here.
	return nil
}
