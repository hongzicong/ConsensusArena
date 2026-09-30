package recoverylog

import (
	"fmt"
	"github.com/hongzicong/ConsensusArena/state"
	"math/bits"
	"sort"
	"time"
)

const PageSize = 128
const AdmissionWindow = 8192

type Core struct {
	N                          int
	ID, Leader                 int32
	CURP                       bool
	Classic                    bool // leader-directed classic Paxos phase two
	frozen                     []Packet
	frozenBallot, frozenFloor  int64
	Ballot                     int64
	Active, Preparing          bool
	High, Executed             int64
	Log                        map[int64]*Record
	Values                     map[Key]state.Value
	Assigned                   map[Key]int64
	Pending                    map[Key]Request
	Witness                    map[Key]Request
	recorded                   map[Key]int64
	votes                      map[int64]uint64
	pages                      map[int32]map[int32]Packet
	promises                   map[int32]bool
	queue                      []Key
	queued                     map[Key]bool
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

func New(n int, id, leader int32, curp bool, st *state.State) *Core {
	if n < 3 || n > 63 || n%2 == 0 {
		panic("recovery log requires odd membership of 3..63")
	}
	return &Core{N: n, ID: id, Leader: leader, CURP: curp, Ballot: int64(leader), Active: true,
		High: -1, Executed: -1, recoveryEnd: -1, Log: map[int64]*Record{}, Values: map[Key]state.Value{}, Assigned: map[Key]int64{}, Pending: map[Key]Request{}, Witness: map[Key]Request{}, recorded: map[Key]int64{}, votes: map[int64]uint64{}, pages: map[int32]map[int32]Packet{}, promises: map[int32]bool{}, queued: map[Key]bool{}, State: st, witnessIndex: newConflictIndex(), pendingIndex: newConflictIndex()}
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
	if c.CURP {
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
	} else if c.Classic {
		c.send(c.Leader, &Packet{Kind: Forward, From: c.ID, Ballot: c.Ballot, Requests: []Request{r}})
	}
}
func (c *Core) Begin(now time.Time) {
	c.Ballot = (c.Ballot/int64(c.N)+1)*int64(c.N) + int64(c.ID)
	c.Leader = c.ID
	c.Active = false
	c.Preparing = true
	c.Recoveries++
	c.pages = map[int32]map[int32]Packet{}
	c.promises = map[int32]bool{}
	c.votes = map[int64]uint64{}
	c.lastSend = now
	p := &Packet{Kind: Prepare, From: c.ID, Ballot: c.Ballot, Floor: c.Executed + 1}
	c.broadcast(p)
	c.Handle(p)
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
			p := &Packet{Kind: Prepare, From: c.ID, Ballot: c.Ballot, Floor: c.Executed + 1}
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
			c.broadcast(&Packet{Kind: Ready, From: c.ID, Ballot: c.Ballot, High: c.Executed, Floor: boolInt(c.Active)})
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
func (c *Core) activate() {
	c.Active = true
	c.broadcast(&Packet{Kind: Ready, From: c.ID, Ballot: c.Ballot, High: c.Executed, Floor: 1})
	c.schedulePending()
}
func (c *Core) schedulePending() {
	c.resume = make([]Request, 0, len(c.Pending))
	for _, r := range c.Pending {
		c.resume = append(c.resume, r)
	}
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
		fast := c.CURP
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
		p := &Packet{Kind: Accept, From: c.ID, Ballot: c.Ballot, Records: records}
		c.broadcast(p)
		c.accept(p)
	}
}
func (c *Core) Handle(p *Packet) {
	if p.From < 0 || int(p.From) >= c.N {
		return
	}
	if p.Kind == Prepare {
		if p.Ballot < c.Ballot {
			return
		}
		if p.Ballot > c.Ballot {
			c.Ballot = p.Ballot
			c.Leader = p.From
			c.Preparing = false
			c.votes = map[int64]uint64{}
		}
		c.Active = false
		c.promise(p)
		return
	}
	if p.Kind == Ready && p.Ballot > c.Ballot {
		c.Ballot = p.Ballot
		c.Leader = p.From
		c.Preparing = false
		c.Active = false
		c.votes = map[int64]uint64{}
	}
	if p.Ballot != c.Ballot {
		return
	}
	switch p.Kind {
	case Promise:
		c.promiseReply(p)
	case Accept:
		if p.From == c.Leader {
			c.accept(p)
		}
	case Ack:
		c.ack(p)
	case Commit:
		if p.From != c.Leader {
			return
		}
		for _, r := range p.Records {
			c.install(r)
			c.Log[r.Slot].Committed = true
		}
		c.execute()
	case Ready:
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
	case Fetch:
		if c.Leader == c.ID {
			c.sendSuffix(p.From, p.Floor, true)
		}
	case Forward:
		if c.ID == c.Leader && c.Active {
			for _, r := range p.Requests {
				c.Propose(r)
			}
		}
	}
}
func (c *Core) promise(p *Packet) {
	if c.Classic && len(c.frozen) > 0 && c.frozenBallot == p.Ballot && c.frozenFloor == p.Floor {
		for i := range c.frozen {
			c.send(p.From, &c.frozen[i])
		}
		return
	}
	var snapshot []Packet
	values := []Record{}
	for slot := p.Floor; slot <= c.High; slot++ {
		if r := c.Log[slot]; r != nil {
			values = append(values, *r)
		}
	}
	witness := []Request{}
	if c.CURP {
		for _, r := range c.Witness {
			witness = append(witness, r)
		}
	}
	sort.Slice(witness, func(i, j int) bool {
		a, b := witness[i].ID, witness[j].ID
		if a.Client != b.Client {
			return a.Client < b.Client
		}
		return a.Sequence < b.Sequence
	})
	count := (len(values) + PageSize - 1) / PageSize
	wc := (len(witness) + PageSize - 1) / PageSize
	if wc > count {
		count = wc
	}
	if count == 0 {
		count = 1
	}
	for page := 0; page < count; page++ {
		reply := &Packet{Kind: Promise, From: c.ID, Ballot: c.Ballot, Floor: p.Floor, High: c.High, Page: int32(page), Pages: int32(count)}
		lo := page * PageSize
		hi := lo + PageSize
		if lo < len(values) {
			end := hi
			if end > len(values) {
				end = len(values)
			}
			reply.Records = values[lo:end]
		}
		if lo < len(witness) {
			end := hi
			if end > len(witness) {
				end = len(witness)
			}
			reply.Requests = witness[lo:end]
		}
		snapshot = append(snapshot, *reply)
	}
	if c.Classic {
		c.frozen, c.frozenBallot, c.frozenFloor = snapshot, p.Ballot, p.Floor
	}
	for i := range snapshot {
		c.send(p.From, &snapshot[i])
	}
}
func (c *Core) promiseReply(p *Packet) {
	if !c.Preparing || c.Leader != c.ID || p.Pages <= 0 || p.Page < 0 || p.Page >= p.Pages {
		return
	}
	if c.pages[p.From] == nil {
		c.pages[p.From] = map[int32]Packet{}
	}
	c.pages[p.From][p.Page] = *p
	if len(c.pages[p.From]) != int(p.Pages) {
		return
	}
	for page := int32(0); page < p.Pages; page++ {
		v, ok := c.pages[p.From][page]
		if !ok || v.Pages != p.Pages || v.Floor != c.Executed+1 || v.High != p.High {
			return
		}
	}
	c.promises[p.From] = true
	if len(c.promises) < c.N/2+1 {
		return
	}
	selected := map[int64]Record{}
	witnessVotes := map[Key]uint64{}
	witness := map[Key]Request{}
	high := c.Executed
	for peer := range c.promises {
		for _, page := range c.pages[peer] {
			if page.High > high {
				high = page.High
			}
			for _, r := range page.Records {
				old, ok := selected[r.Slot]
				if !ok || r.Ballot > old.Ballot {
					selected[r.Slot] = r
				}
			}
			for _, r := range page.Requests {
				witness[r.ID] = r
				witnessVotes[r.ID] |= uint64(1) << peer
			}
		}
	}
	c.Preparing = false
	c.High = high
	c.Assigned = map[Key]int64{}
	c.pendingIndex = newConflictIndex()
	for slot, r := range c.Log {
		if slot <= c.Executed {
			c.Assigned[r.Request.ID] = slot
		}
	}
	for slot := c.Executed + 1; slot <= high; slot++ {
		r, ok := selected[slot]
		if !ok {
			r = Record{Slot: slot, Request: Request{ID: Key{Client: -1, Sequence: int32(slot)}, Command: state.Command{Op: state.NONE}}}
		}
		r.Ballot = c.Ballot
		r.Committed = false
		c.Log[slot] = &r
		c.pendingIndex.add(r.Request)
		c.Assigned[r.Request.ID] = slot
	}
	keys := []Key{}
	for id := range witness {
		keys = append(keys, id)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].Client != keys[j].Client {
			return keys[i].Client < keys[j].Client
		}
		return keys[i].Sequence < keys[j].Sequence
	})
	threshold := (c.N/2+1)/2 + 1 // ceil(f/2)+1
	for _, id := range keys {
		if bits.OnesCount64(witnessVotes[id]) < threshold {
			continue
		}
		if _, ok := c.Assigned[id]; ok {
			continue
		}
		if _, ok := c.Values[id]; ok {
			continue
		}
		c.High++
		r := Record{Slot: c.High, Ballot: c.Ballot, Request: witness[id]}
		c.Log[c.High] = &r
		c.pendingIndex.add(r.Request)
		c.Assigned[id] = c.High
	}
	c.recoveryEnd = c.High
	c.votes = map[int64]uint64{}
	c.sendSuffix(c.ID, c.Executed+1, false)
	if c.Executed >= c.recoveryEnd {
		c.activate()
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
	ack := &Packet{Kind: Ack, From: c.ID, Ballot: c.Ballot, Records: accepted}
	if c.Classic {
		c.send(c.Leader, ack)
		return
	}
	// N2Paxos and CURP retain all-to-all phase two.
	c.broadcast(ack)
	c.ack(ack)
}

func (c *Core) requestFetch(now time.Time) {
	if c.ID == c.Leader {
		return
	}
	// Gap notifications and leader heartbeats share a retry budget. A burst
	// of later accepts must not enqueue the same suffix for every packet.
	if now.Sub(c.lastFetch) < 100*time.Millisecond {
		c.FetchSuppressed++
		return
	}
	c.lastFetch = now
	c.FetchRequests++
	c.send(c.Leader, &Packet{Kind: Fetch, From: c.ID, Ballot: c.Ballot, Floor: c.Executed + 1})
}

func (c *Core) ack(p *Packet) {
	if c.Preparing || c.Classic && c.ID != c.Leader {
		return
	}
	committed := []Record{}
	for _, ack := range p.Records {
		if ack.Slot <= c.Executed {
			continue
		}
		c.votes[ack.Slot] |= uint64(1) << p.From
		r := c.Log[ack.Slot]
		if r == nil || r.Ballot != c.Ballot || r.Committed || bits.OnesCount64(c.votes[ack.Slot]) < c.N/2+1 {
			continue
		}
		r.Committed = true
		committed = append(committed, *r)
	}
	if (c.CURP || c.Classic) && c.ID == c.Leader && len(committed) > 0 {
		c.broadcast(&Packet{Kind: Commit, From: c.ID, Ballot: c.Ballot, Records: committed})
	}
	c.execute()
}
func (c *Core) execute() {
	for {
		r := c.Log[c.Executed+1]
		if r == nil || !r.Committed {
			return
		}
		if _, ok := c.Values[r.Request.ID]; !ok && r.Request.Command.Op != state.NONE {
			v := r.Request.Command.Execute(c.State)
			c.Values[r.Request.ID] = append(state.Value(nil), v...)
			c.Reply(r.Request, v, false)
		}
		delete(c.Pending, r.Request.ID)
		if w, ok := c.Witness[r.Request.ID]; ok {
			c.witnessIndex.remove(w)
		}
		delete(c.Witness, r.Request.ID)
		c.pendingIndex.remove(r.Request)
		delete(c.recorded, r.Request.ID)
		delete(c.votes, r.Slot)
		c.Executed++
	}
}
func (c *Core) sendSuffix(to int32, start int64, fetch bool) {
	end := c.High
	if limit := start + 2048 - 1; end > limit {
		end = limit
	}
	for lo := start; lo <= end; lo += PageSize {
		recs := []Record{}
		for slot := lo; slot < lo+PageSize && slot <= end; slot++ {
			if r := c.Log[slot]; r != nil {
				cp := *r
				cp.Ballot = c.Ballot
				recs = append(recs, cp)
			}
		}
		if len(recs) == 0 {
			continue
		}
		if fetch && to != c.ID {
			c.Send(to, &Packet{Kind: Accept, From: c.ID, Ballot: c.Ballot, Records: recs})
			decided := []Record{}
			for _, r := range recs {
				if r.Committed {
					decided = append(decided, r)
				}
			}
			if len(decided) > 0 {
				c.Send(to, &Packet{Kind: Commit, From: c.ID, Ballot: c.Ballot, Records: decided})
			}
		} else {
			for i := range recs {
				recs[i].Committed = false
			}
			p := &Packet{Kind: Accept, From: c.ID, Ballot: c.Ballot, Records: recs}
			c.broadcast(p)
			c.accept(p)
		}
	}
}
