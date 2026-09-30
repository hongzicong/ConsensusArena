package recoverylog

import (
	"bufio"
	"fmt"
	"io"
	"time"

	"github.com/hongzicong/ConsensusArena/consensusruntime"
	"github.com/hongzicong/ConsensusArena/replica"
	"github.com/hongzicong/ConsensusArena/replica/defs"
	fastrpc "github.com/hongzicong/ConsensusArena/rpc"
	"github.com/hongzicong/ConsensusArena/state"
)

// Runtime adapts the ordered-log core to the shared execution layer.
// Protocol state and client completion rules stay in this package.
type Runtime struct {
	*consensusruntime.Runtime
	Core          *Core
	proposals     map[Key]*defs.GPropose
	ReplyMessage  func(Request, state.Value, bool, int32) (uint8, fastrpc.Serializable)
	RecordMessage func(Request, bool, int32) (uint8, fastrpc.Serializable)
}

func NewRuntime(base *replica.Replica, leader int32, curp bool) *Runtime {
	r := &Runtime{Core: New(base.N, base.Id, leader, curp, base.State), proposals: map[Key]*defs.GPropose{}}
	r.Runtime = consensusruntime.New(base, &protocolAdapter{r})
	code := r.Register(&Packet{})
	r.Core.Send = func(id int32, p *Packet) { r.Send(id, code, p) }
	r.Core.Reply = func(req Request, v state.Value, fast bool) {
		if r.ReplyMessage != nil {
			code, msg := r.ReplyMessage(req, v, fast, int32(r.Core.Ballot))
			r.client(req.ID, code, msg, true)
		} else {
			g := r.proposals[req.ID]
			if g != nil {
				r.client(req.ID, 0, &defs.ProposeReplyTS{OK: defs.TRUE, CommandId: req.ID.Sequence, Value: v, Timestamp: g.Timestamp}, false)
			}
		}
		if !fast {
			delete(r.proposals, req.ID)
		}
	}
	r.Core.RecordReply = func(req Request, ok bool) {
		if r.RecordMessage != nil {
			code, msg := r.RecordMessage(req, ok, int32(r.Core.Ballot))
			r.client(req.ID, code, msg, true)
		}
	}
	return r
}

func (r *Runtime) client(id Key, code uint8, msg interface{ Marshal(io.Writer) }, custom bool) {
	var writer *bufio.Writer
	if g := r.proposals[id]; g != nil {
		writer = g.Reply
		if writer == nil {
			return
		}
	}
	r.Runtime.Reply(id.Client, writer, code, msg, custom)
}

type protocolAdapter struct{ runtime *Runtime }

var _ consensusruntime.Protocol = (*protocolAdapter)(nil)

func (p *protocolAdapter) Propose(g *defs.GPropose) {
	id := Key{g.ClientId, g.CommandId}
	p.runtime.proposals[id] = g
	p.runtime.Core.Propose(Request{id, g.Command})
}

func (p *protocolAdapter) Handle(message fastrpc.Serializable) {
	p.runtime.Core.Handle(message.(*Packet))
}

func (p *protocolAdapter) Tick(now time.Time, alive []bool) {
	p.runtime.Core.Tick(now, alive)
}

func (p *protocolAdapter) Leader() int32 { return p.runtime.Core.Leader }

func (p *protocolAdapter) Status() string {
	c := p.runtime.Core
	return fmt.Sprintf("BASELINE_RECOVERY curp=%t classic=%t ballot=%d leader=%d active=%t preparing=%t accepted=%d executed=%d pending=%d witness=%d recoveries=%d recovery_end=%d gap_accepts=%d fetch_requests=%d fetch_suppressed=%d", c.CURP, c.Classic, c.Ballot, c.Leader, c.Active, c.Preparing, c.High, c.Executed, len(c.Pending), len(c.Witness), c.Recoveries, c.recoveryEnd, c.GapAccepts, c.FetchRequests, c.FetchSuppressed)
}
