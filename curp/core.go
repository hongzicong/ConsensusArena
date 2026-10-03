package curp

// Protocol state and normal-case transitions.
import (
	"fmt"
	"time"

	"github.com/hongzicong/ConsensusArena/replica/defs"
	"github.com/hongzicong/ConsensusArena/replicaset"
	"github.com/hongzicong/ConsensusArena/state"
)

type Core struct {
	N          int
	ID, Leader int32

	frozen                     []Packet
	frozenBallot, frozenFloor  int64
	Ballot                     int64
	Active, Preparing          bool
	High, Executed             int64
	Log                        map[int64]*Record
	Values                     map[defs.RequestID]state.Value
	Assigned                   map[defs.RequestID]int64
	Pending                    map[defs.RequestID]Request
	Witness                    map[defs.RequestID]Request
	recorded                   map[defs.RequestID]int64
	votes                      map[int64]replicaset.Set
	pages                      map[int32]map[int32]Packet
	promises                   map[int32]bool
	queue                      []defs.RequestID
	queued                     map[defs.RequestID]bool
	recoveryEnd                int64
	lastSend, lastFetch        time.Time
	State                      *state.State
	Send                       func(int32, *Packet)
	Reply                      func(Request, state.Value, bool)
	RecordReply                func(Request, bool)
	Recoveries                 uint64
	GapAccepts                 uint64
	FetchRequests              uint64
	FetchSuppressed            uint64
	witnessIndex, pendingIndex *conflictIndex
	resume                     []Request
}

func newCore(n int, id, leader int32, st *state.State) *Core {
	if n < 3 || n > 63 || n%2 == 0 {
		panic("recovery log requires odd membership of 3..63")
	}
	return &Core{N: n, ID: id, Leader: leader, Ballot: int64(leader), Active: true,
		High: -1, Executed: -1, recoveryEnd: -1, Log: map[int64]*Record{}, Values: map[defs.RequestID]state.Value{}, Assigned: map[defs.RequestID]int64{}, Pending: map[defs.RequestID]Request{}, Witness: map[defs.RequestID]Request{}, recorded: map[defs.RequestID]int64{}, votes: map[int64]replicaset.Set{}, pages: map[int32]map[int32]Packet{}, promises: map[int32]bool{}, queued: map[defs.RequestID]bool{}, State: st, witnessIndex: newConflictIndex(), pendingIndex: newConflictIndex()}
}

func (c *Core) broadcast(p *Packet) {
	for id := 0; id < c.N; id++ {
		if int32(id) != c.ID {
			c.Send(int32(id), p)
		}
	}
}

func (c *Core) send(id int32, p *Packet) {
	if id == c.ID {
		c.Handle(p)
	} else {
		c.Send(id, p)
	}
}

func (c *Core) enqueue(r Request) {
	if _, ok := c.Values[r.ID]; ok {
		return
	}
	if _, ok := c.Assigned[r.ID]; ok {
		return
	}
	if !c.queued[r.ID] {
		c.queue = append(c.queue, r.ID)
		c.queued[r.ID] = true
	}
}

func (c *Core) Propose(r Request) {
	if v, ok := c.Values[r.ID]; ok {
		c.Reply(r, v, false)
		return
	}
	c.Pending[r.ID] = r
	if !c.Active {
		return
	}
	{
		if b, ok := c.recorded[r.ID]; !ok || b != c.Ballot {
			// Exclude a retained positive record for this same request.
			old, exists := c.Witness[r.ID]
			if exists {
				c.witnessIndex.remove(old)
			}
			positive := !c.witnessIndex.conflicts(r)
			if exists {
				c.witnessIndex.add(old)
			}

			c.recorded[r.ID] = c.Ballot
			if positive {
				if !exists {
					c.witnessIndex.add(r)
				}
				c.Witness[r.ID] = r
			}
			if c.ID != c.Leader {
				c.RecordReply(r, positive)
			}
		}
	}
	if c.Leader == c.ID {
		c.enqueue(r)
	}

}

func (c *Core) Tick(now time.Time, alive []bool) {
	if !alive[c.Leader] {
		candidate := int32(-1)
		for id, v := range alive {
			if v {
				candidate = int32(id)
				break
			}
		}
		if candidate == c.ID && !c.Preparing {
			c.Begin(now)
		}
	}
	if c.Active {
		for i := 0; i < PageSize && len(c.resume) > 0; i++ {
			r := c.resume[0]
			c.resume[0] = Request{}
			c.resume = c.resume[1:]
			if _, pending := c.Pending[r.ID]; pending {
				c.Propose(r)
			}
		}
	}
	if c.Preparing {
		if now.Sub(c.lastSend) >= time.Second {
			c.lastSend = now
			p := &Packet{Kind: packetPrepare, From: c.ID, Ballot: c.Ballot, Floor: c.Executed + 1}
			c.broadcast(p)
			c.Handle(p)
		}
		return
	}
	if c.Leader == c.ID {
		if !c.Active && c.Executed >= c.recoveryEnd {
			c.activate()
		}
		if c.Active {
			c.flush()
		}
		if now.Sub(c.lastSend) >= 200*time.Millisecond {
			c.lastSend = now
			c.broadcast(&Packet{Kind: packetReady, From: c.ID, Ballot: c.Ballot, High: c.Executed, Floor: boolInt(c.Active)})
			c.sendSuffix(c.ID, c.Executed+1, false)
		}
	}
}

func boolInt(v bool) int64 {
	if v {
		return 1
	}
	return 0
}

func (c *Core) flush() {
	if c.High-c.Executed >= AdmissionWindow {
		return
	}
	records := []Record{}
	for len(c.queue) > 0 && len(records) < PageSize && c.High-c.Executed < AdmissionWindow {
		id := c.queue[0]
		c.queue = c.queue[1:]
		delete(c.queued, id)
		if _, ok := c.Assigned[id]; ok {
			continue
		}
		if _, ok := c.Values[id]; ok {
			continue
		}
		r, ok := c.Pending[id]
		if !ok {
			continue
		}
		fast := true
		if _, ok := c.Witness[id]; !ok {
			fast = false
		}
		if fast && c.pendingIndex.conflicts(r) {
			fast = false
		}

		c.High++
		rec := Record{Slot: c.High, Ballot: c.Ballot, Request: r}
		c.Log[c.High] = &rec
		c.pendingIndex.add(r)
		c.Assigned[id] = c.High
		records = append(records, rec)
		if fast {
			switch r.Command.Op {
			case state.PUT:
				c.Reply(r, state.NIL(), true)
			case state.GET:
				c.Reply(r, r.Command.Execute(c.State), true)
			}
		}
	}
	if len(records) > 0 {
		p := &Packet{Kind: packetAccept, From: c.ID, Ballot: c.Ballot, Records: records}
		c.broadcast(p)
		c.accept(p)
	}
}

func (c *Core) Handle(p *Packet) {
	if p.From < 0 || int(p.From) >= c.N {
		return
	}
	if p.Kind == packetPrepare {
		if p.Ballot < c.Ballot {
			return
		}
		if p.Ballot > c.Ballot {
			c.Ballot = p.Ballot
			c.Leader = p.From
			c.Preparing = false
			c.votes = map[int64]replicaset.Set{}
		}
		c.Active = false
		c.promise(p)
		return
	}
	if p.Kind == packetReady && p.Ballot > c.Ballot {
		c.Ballot = p.Ballot
		c.Leader = p.From
		c.Preparing = false
		c.Active = false
		c.votes = map[int64]replicaset.Set{}
	}
	if p.Ballot != c.Ballot {
		return
	}
	switch p.Kind {
	case packetPromise:
		c.promiseReply(p)
	case packetAccept:
		if p.From == c.Leader {
			c.accept(p)
		}
	case packetAck:
		c.ack(p)
	case packetCommit:
		if p.From != c.Leader {
			return
		}
		for _, r := range p.Records {
			c.install(r)
			c.Log[r.Slot].Committed = true
		}
		c.execute()
	case packetReady:
		if p.From != c.Leader {
			return
		}
		if p.Floor == 1 && !c.Active {
			c.Active = true
			c.schedulePending()
		}
		if p.High > c.Executed {
			c.requestFetch(time.Now())
		}
	case packetFetch:
		if c.Leader == c.ID {
			c.sendSuffix(p.From, p.Floor, true)
		}
	case packetForward:
		if c.ID == c.Leader && c.Active {
			for _, r := range p.Requests {
				c.Propose(r)
			}
		}
	}
}

func (c *Core) install(r Record) {
	if old := c.Log[r.Slot]; old != nil && old.Committed {
		if old.Request.ID != r.Request.ID {
			panic(fmt.Sprintf("conflicting decision at %d", r.Slot))
		}
		return
	}
	if old := c.Log[r.Slot]; old != nil && r.Slot > c.Executed {
		c.pendingIndex.remove(old.Request)
	}
	if r.Slot > c.Executed {
		c.pendingIndex.add(r.Request)
	}
	cp := r
	c.Log[r.Slot] = &cp
	c.Assigned[r.Request.ID] = r.Slot
	if r.Slot > c.High {
		c.High = r.Slot
	}
}

func (c *Core) accept(p *Packet) {
	accepted := []Record{}
	gap := false
	for _, r := range p.Records {
		if r.Slot > 0 && c.Log[r.Slot-1] == nil {
			gap = true
			c.GapAccepts++
		}
		// A Paxos vote is per slot: a missing prefix must not prevent this
		// acceptor from voting in recovery. execute still requires every
		// preceding slot to be committed before applying any command.
		r.Ballot = p.Ballot
		c.install(r)
		accepted = append(accepted, Record{Slot: r.Slot})
	}
	if gap {
		c.requestFetch(time.Now())
	}
	if len(accepted) == 0 {
		return
	}
	ack := &Packet{Kind: packetAck, From: c.ID, Ballot: c.Ballot, Records: accepted}

	// CURP retains all-to-all phase two.
	c.broadcast(ack)
	c.ack(ack)
}

func (c *Core) ack(p *Packet) {
	if c.Preparing {
		return
	}
	committed := []Record{}
	for _, ack := range p.Records {
		if ack.Slot <= c.Executed {
			continue
		}
		c.votes[ack.Slot] = c.votes[ack.Slot].With(int(p.From))
		r := c.Log[ack.Slot]
		if r == nil || r.Ballot != c.Ballot || r.Committed || c.votes[ack.Slot].Size() < c.N/2+1 {
			continue
		}
		r.Committed = true
		committed = append(committed, *r)
	}
	if c.ID == c.Leader && len(committed) > 0 {
		c.broadcast(&Packet{Kind: packetCommit, From: c.ID, Ballot: c.Ballot, Records: committed})
	}
	c.execute()
}
