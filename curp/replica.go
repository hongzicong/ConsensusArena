package curp

// Replica state, configuration, RPC registration, and startup.
import (
	"github.com/hongzicong/ConsensusArena/config"
	"github.com/hongzicong/ConsensusArena/dlog"
	"github.com/hongzicong/ConsensusArena/replica"
	"github.com/hongzicong/ConsensusArena/replica/defs"
	fastrpc "github.com/hongzicong/ConsensusArena/rpc"
	"github.com/hongzicong/ConsensusArena/state"
)

type Replica struct {
	*replica.Replica
	recovery *protocolRuntime
}

func New(alias string, rid int, addrs []string, exec bool, f int, conf *config.Config, logger *dlog.Logger) *Replica {
	// The protocol runtime owns normal operation and recovery state.
	r := &Replica{Replica: replica.New(alias, rid, f, addrs, false, exec, false, conf, logger)}

	ballot := conf.Plan.LeaderID()

	var cs CommunicationSupply
	initCs(&cs, r.RPC)

	r.recovery = newProtocolRuntime(r.Replica, ballot)
	r.recovery.ReplyMessage = func(req Request, value state.Value, fast bool, ballot int32) (uint8, fastrpc.Serializable) {
		id := req.ID
		if fast {
			return cs.replyRPC, &MReply{Replica: r.Id, Ballot: ballot, CmdId: id, Rep: value, Ok: TRUE}
		}
		return cs.syncReplyRPC, &MSyncReply{Replica: r.Id, Ballot: ballot, CmdId: id, Rep: value}
	}
	r.recovery.RecordMessage = func(req Request, positive bool, ballot int32) (uint8, fastrpc.Serializable) {
		ok := FALSE
		if positive {
			ok = TRUE
		}
		return cs.recordAckRPC, &MRecordAck{Replica: r.Id, Ballot: ballot, CmdId: req.ID, Ok: ok}
	}
	go r.recovery.Run()

	return r
}

// Retain message registrations in their existing order for wire compatibility.
type CommunicationSupply struct {
	replyChan     chan fastrpc.Serializable
	acceptChan    chan fastrpc.Serializable
	acceptAckChan chan fastrpc.Serializable
	aacksChan     chan fastrpc.Serializable
	recordAckChan chan fastrpc.Serializable
	commitChan    chan fastrpc.Serializable
	syncChan      chan fastrpc.Serializable
	syncReplyChan chan fastrpc.Serializable

	replyRPC     uint8
	acceptRPC    uint8
	acceptAckRPC uint8
	aacksRPC     uint8
	recordAckRPC uint8
	commitRPC    uint8
	syncRPC      uint8
	syncReplyRPC uint8
}

func initCs(cs *CommunicationSupply, t *fastrpc.Table) {
	cs.replyChan = make(chan fastrpc.Serializable, defs.CHAN_BUFFER_SIZE)
	cs.acceptChan = make(chan fastrpc.Serializable, defs.CHAN_BUFFER_SIZE)
	cs.acceptAckChan = make(chan fastrpc.Serializable, defs.CHAN_BUFFER_SIZE)
	cs.aacksChan = make(chan fastrpc.Serializable, defs.CHAN_BUFFER_SIZE)
	cs.recordAckChan = make(chan fastrpc.Serializable, defs.CHAN_BUFFER_SIZE)
	cs.commitChan = make(chan fastrpc.Serializable, defs.CHAN_BUFFER_SIZE)
	cs.syncChan = make(chan fastrpc.Serializable, defs.CHAN_BUFFER_SIZE)
	cs.syncReplyChan = make(chan fastrpc.Serializable, defs.CHAN_BUFFER_SIZE)

	cs.replyRPC = t.Register(new(MReply), cs.replyChan)
	cs.acceptRPC = t.Register(new(MAccept), cs.acceptChan)
	cs.acceptAckRPC = t.Register(new(MAcceptAck), cs.acceptAckChan)
	cs.aacksRPC = t.Register(new(MAAcks), cs.aacksChan)
	cs.recordAckRPC = t.Register(new(MRecordAck), cs.recordAckChan)
	cs.commitRPC = t.Register(new(MCommit), cs.commitChan)
	cs.syncRPC = t.Register(new(MSync), cs.syncChan)
	cs.syncReplyRPC = t.Register(new(MSyncReply), cs.syncReplyChan)
}

// Register must run before Run and in the same order at every replica.
func (r *transportRuntime) Register(message fastrpc.Serializable) uint8 {
	return r.Base.RPC.Register(message, r.in)
}
