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

// Replica owns runtime resources and exposes the external protocol event boundary.
type Replica struct {
	*replica.Replica
	runtime *runtime
}

func New(alias string, replicaID int, addrs []string, executeEnabled bool, f int, conf *config.Config, logger *dlog.Logger) *Replica {
	// The protocol runtime owns normal operation and recovery state.
	r := &Replica{Replica: replica.New(alias, replicaID, f, addrs, executeEnabled, conf, logger)}

	ballot := conf.Plan.LeaderID()

	var channels transportChannels
	registerRPCs(&channels, r.RPC)

	r.runtime = newRuntime(r.Replica, ballot)
	r.runtime.ReplyMessage = func(req Request, value state.Value, fast bool, ballot int32) (uint8, fastrpc.Serializable) {
		id := req.ID
		if fast {
			return channels.replyRPC, &MReply{Replica: r.ID, Ballot: ballot, CmdId: id, Rep: value, Ok: TRUE}
		}
		return channels.syncReplyRPC, &MSyncReply{Replica: r.ID, Ballot: ballot, CmdId: id, Rep: value}
	}
	r.runtime.RecordMessage = func(req Request, positive bool, ballot int32) (uint8, fastrpc.Serializable) {
		ok := FALSE
		if positive {
			ok = TRUE
		}
		return channels.recordAckRPC, &MRecordAck{Replica: r.ID, Ballot: ballot, CmdId: req.ID, Ok: ok}
	}
	go r.runtime.Run()

	return r
}

// Retain message registrations in their existing order for wire compatibility.
type transportChannels struct {
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

func registerRPCs(channels *transportChannels, t *fastrpc.Table) {
	channels.replyChan = make(chan fastrpc.Serializable, defs.CHAN_BUFFER_SIZE)
	channels.acceptChan = make(chan fastrpc.Serializable, defs.CHAN_BUFFER_SIZE)
	channels.acceptAckChan = make(chan fastrpc.Serializable, defs.CHAN_BUFFER_SIZE)
	channels.aacksChan = make(chan fastrpc.Serializable, defs.CHAN_BUFFER_SIZE)
	channels.recordAckChan = make(chan fastrpc.Serializable, defs.CHAN_BUFFER_SIZE)
	channels.commitChan = make(chan fastrpc.Serializable, defs.CHAN_BUFFER_SIZE)
	channels.syncChan = make(chan fastrpc.Serializable, defs.CHAN_BUFFER_SIZE)
	channels.syncReplyChan = make(chan fastrpc.Serializable, defs.CHAN_BUFFER_SIZE)

	channels.replyRPC = t.Register(new(MReply), channels.replyChan)
	channels.acceptRPC = t.Register(new(MAccept), channels.acceptChan)
	channels.acceptAckRPC = t.Register(new(MAcceptAck), channels.acceptAckChan)
	channels.aacksRPC = t.Register(new(MAAcks), channels.aacksChan)
	channels.recordAckRPC = t.Register(new(MRecordAck), channels.recordAckChan)
	channels.commitRPC = t.Register(new(MCommit), channels.commitChan)
	channels.syncRPC = t.Register(new(MSync), channels.syncChan)
	channels.syncReplyRPC = t.Register(new(MSyncReply), channels.syncReplyChan)
}

// Register must run before Run and in the same order at every replica.
