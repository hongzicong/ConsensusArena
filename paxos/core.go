package paxos

// Protocol state and normal-case transitions.
import (
	"fmt"
	"time"

	"github.com/hongzicong/ConsensusArena/replica/defs"
	"github.com/hongzicong/ConsensusArena/replicaset"
	"github.com/hongzicong/ConsensusArena/state"
)

// core owns protocol state, ordering, recovery decisions and completion semantics.
type core struct {
	n          int
	id, leader int32

	// leader-directed classic Paxos phase two
	frozen                    []Packet
	frozenBallot, frozenFloor int64
	ballot                    int64
	active, preparing         bool
	high, executed            int64
	log                       map[int64]*Record
	values                    map[defs.RequestID]state.Value
	assigned                  map[defs.RequestID]int64
	pending                   map[defs.RequestID]Request
	votes                     map[int64]replicaset.Set
	pages                     map[int32]map[int32]Packet
	promises                  map[int32]bool
	queue                     []defs.RequestID
	queued                    map[defs.RequestID]bool
	recoveryEnd               int64
	lastSend, lastFetch       time.Time
	execute                   func(state.Command) state.Value
	send                      func(int32, *Packet)
	broadcast                 func(*Packet)
	reply                     func(Request, state.Value)
	recoveries                uint64
	replayReplies             uint64
	gapAccepts                uint64
	fetchRequests             uint64
	fetchSuppressed           uint64
	resume                    []Request
}

func newCore(n int, id, leader int32, execute func(state.Command) state.Value) *core {
	if n < 3 || n > 63 || n%2 == 0 {
		panic("recovery log requires odd membership of 3..63")
	}
	return &core{n: n, id: id, leader: leader, ballot: int64(leader), active: true,
		high: -1, executed: -1, recoveryEnd: -1, log: map[int64]*Record{}, values: map[defs.RequestID]state.Value{}, assigned: map[defs.RequestID]int64{}, pending: map[defs.RequestID]Request{}, votes: map[int64]replicaset.Set{}, pages: map[int32]map[int32]Packet{}, promises: map[int32]bool{}, queued: map[defs.RequestID]bool{}, execute: execute}
}

func (c *core) enqueue(r Request) {
	if _, ok := c.values[r.ID]; ok {
		return
	}
	if _, ok := c.assigned[r.ID]; ok {
		return
	}
	if !c.queued[r.ID] {
		c.queue = append(c.queue, r.ID)
		c.queued[r.ID] = true
	}
}

func (c *core) submit(r Request) {
	if v, ok := c.values[r.ID]; ok {
		c.replayReplies++
		c.reply(r, v)
		return
	}
	c.pending[r.ID] = r
	if !c.active {
		return
	}

	if c.leader == c.id {
		c.enqueue(r)
	} else {
		c.send(c.leader, &Packet{Kind: packetForward, From: c.id, Ballot: c.ballot, Requests: []Request{r}})
	}
}

func (c *core) tick(now time.Time, alive []bool) {
	if !alive[c.leader] {
		candidate := int32(-1)
		for id, v := range alive {
			if v {
				candidate = int32(id)
				break
			}
		}
		if candidate == c.id && !c.preparing {
			c.Begin(now)
		}
	}
	if c.active {
		for i := 0; i < PageSize && len(c.resume) > 0; i++ {
			r := c.resume[0]
			c.resume[0] = Request{}
			c.resume = c.resume[1:]
			if _, pending := c.pending[r.ID]; pending {
				c.submit(r)
			}
		}
	}
	if c.preparing {
		if now.Sub(c.lastSend) >= time.Second {
			c.lastSend = now
			p := &Packet{Kind: packetPrepare, From: c.id, Ballot: c.ballot, Floor: c.executed + 1}
			c.broadcast(p)
			c.receive(p)
		}
		return
	}
	if c.leader == c.id {
		if !c.active && c.executed >= c.recoveryEnd {
			c.activate()
		}
		if c.active {
			c.flush()
		}
		if now.Sub(c.lastSend) >= 200*time.Millisecond {
			c.lastSend = now
			c.broadcast(&Packet{Kind: packetReady, From: c.id, Ballot: c.ballot, High: c.executed, Floor: boolInt(c.active)})
			c.sendSuffix(c.id, c.executed+1, false)
		}
	}
}

func boolInt(v bool) int64 {
	if v {
		return 1
	}
	return 0
}

func (c *core) flush() {
	if c.high-c.executed >= AdmissionWindow {
		return
	}
	records := []Record{}
	for len(c.queue) > 0 && len(records) < PageSize && c.high-c.executed < AdmissionWindow {
		id := c.queue[0]
		c.queue = c.queue[1:]
		delete(c.queued, id)
		if _, ok := c.assigned[id]; ok {
			continue
		}
		if _, ok := c.values[id]; ok {
			continue
		}
		r, ok := c.pending[id]
		if !ok {
			continue
		}
		c.high++
		rec := Record{Slot: c.high, Ballot: c.ballot, Request: r}
		c.log[c.high] = &rec
		c.assigned[id] = c.high
		records = append(records, rec)
	}
	if len(records) > 0 {
		p := &Packet{Kind: packetAccept, From: c.id, Ballot: c.ballot, Records: records}
		c.broadcast(p)
		c.accept(p)
	}
}

func (c *core) receive(p *Packet) {
	if p.From < 0 || int(p.From) >= c.n {
		return
	}
	if p.Kind == packetPrepare {
		if p.Ballot < c.ballot {
			return
		}
		if p.Ballot > c.ballot {
			c.ballot = p.Ballot
			c.leader = p.From
			c.preparing = false
			c.votes = map[int64]replicaset.Set{}
		}
		c.active = false
		c.promise(p)
		return
	}
	if p.Kind == packetReady && p.Ballot > c.ballot {
		c.ballot = p.Ballot
		c.leader = p.From
		c.preparing = false
		c.active = false
		c.votes = map[int64]replicaset.Set{}
	}
	if p.Ballot != c.ballot {
		return
	}
	switch p.Kind {
	case packetPromise:
		c.promiseReply(p)
	case packetAccept:
		if p.From == c.leader {
			c.accept(p)
		}
	case packetAck:
		c.ack(p)
	case packetCommit:
		if p.From != c.leader {
			return
		}
		for _, r := range p.Records {
			c.install(r)
			c.log[r.Slot].Committed = true
		}
		c.apply()
	case packetReady:
		if p.From != c.leader {
			return
		}
		if p.Floor == 1 && !c.active {
			c.active = true
			c.schedulePending()
		}
		if p.High > c.executed {
			c.requestFetch(time.Now())
		}
	case packetFetch:
		if c.leader == c.id {
			c.sendSuffix(p.From, p.Floor, true)
		}
	case packetForward:
		if c.id == c.leader && c.active {
			for _, r := range p.Requests {
				c.submit(r)
			}
		}
	}
}

func (c *core) install(r Record) {
	if old := c.log[r.Slot]; old != nil && old.Committed {
		if old.Request.ID != r.Request.ID {
			panic(fmt.Sprintf("conflicting decision at %d", r.Slot))
		}
		return
	}
	cp := r
	c.log[r.Slot] = &cp
	c.assigned[r.Request.ID] = r.Slot
	if r.Slot > c.high {
		c.high = r.Slot
	}
}

func (c *core) accept(p *Packet) {
	accepted := []Record{}
	gap := false
	for _, r := range p.Records {
		if r.Slot > 0 && c.log[r.Slot-1] == nil {
			gap = true
			c.gapAccepts++
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
	ack := &Packet{Kind: packetAck, From: c.id, Ballot: c.ballot, Records: accepted}
	c.send(c.leader, ack)
}

func (c *core) ack(p *Packet) {
	if c.preparing || c.id != c.leader {
		return
	}
	committed := []Record{}
	for _, ack := range p.Records {
		if ack.Slot <= c.executed {
			continue
		}
		c.votes[ack.Slot] = c.votes[ack.Slot].With(int(p.From))
		r := c.log[ack.Slot]
		if r == nil || r.Ballot != c.ballot || r.Committed || c.votes[ack.Slot].Size() < c.n/2+1 {
			continue
		}
		r.Committed = true
		committed = append(committed, *r)
	}
	if c.id == c.leader && len(committed) > 0 {
		c.broadcast(&Packet{Kind: packetCommit, From: c.id, Ballot: c.ballot, Records: committed})
	}
	c.apply()
}
