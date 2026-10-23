package curp

// Protocol recovery transitions and recovery-specific helpers.
import (
	"math/bits"
	"sort"
	"time"

	"github.com/hongzicong/ConsensusArena/replica/defs"
	"github.com/hongzicong/ConsensusArena/replicaset"
	"github.com/hongzicong/ConsensusArena/state"
)

func (c *core) Begin(now time.Time) {
	c.ballot = (c.ballot/int64(c.n)+1)*int64(c.n) + int64(c.id)
	c.leader = c.id
	c.active = false
	c.preparing = true
	c.recoveries++
	c.pages = map[int32]map[int32]Packet{}
	c.promises = map[int32]bool{}
	c.votes = map[int64]replicaset.Set{}
	c.lastSend = now
	p := &Packet{Kind: packetPrepare, From: c.id, Ballot: c.ballot, Floor: c.executed + 1}
	c.broadcast(p)
	c.receive(p)
}

func (c *core) activate() {
	c.active = true
	c.broadcast(&Packet{Kind: packetReady, From: c.id, Ballot: c.ballot, High: c.executed, Floor: 1})
	c.schedulePending()
}

func (c *core) schedulePending() {
	c.resume = make([]Request, 0, len(c.pending))
	for _, r := range c.pending {
		c.resume = append(c.resume, r)
	}
}

func (c *core) promise(p *Packet) {

	var snapshot []Packet
	values := []Record{}
	for slot := p.Floor; slot <= c.high; slot++ {
		if r := c.log[slot]; r != nil {
			values = append(values, *r)
		}
	}
	witness := []Request{}
	{
		for _, r := range c.witness {
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
		reply := &Packet{Kind: packetPromise, From: c.id, Ballot: c.ballot, Floor: p.Floor, High: c.high, Page: int32(page), Pages: int32(count)}
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

	for i := range snapshot {
		c.send(p.From, &snapshot[i])
	}
}

func (c *core) promiseReply(p *Packet) {
	if !c.preparing || c.leader != c.id || p.Pages <= 0 || p.Page < 0 || p.Page >= p.Pages {
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
		if !ok || v.Pages != p.Pages || v.Floor != c.executed+1 || v.High != p.High {
			return
		}
	}
	c.promises[p.From] = true
	if len(c.promises) < c.n/2+1 {
		return
	}
	selected := map[int64]Record{}
	witnessVotes := map[defs.RequestID]uint64{}
	witness := map[defs.RequestID]Request{}
	high := c.executed
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
	c.preparing = false
	c.high = high
	c.assigned = map[defs.RequestID]int64{}
	c.pendingIndex = newConflictIndex()
	for slot, r := range c.log {
		if slot <= c.executed {
			c.assigned[r.Request.ID] = slot
		}
	}
	for slot := c.executed + 1; slot <= high; slot++ {
		r, ok := selected[slot]
		if !ok {
			r = Record{Slot: slot, Request: Request{ID: defs.RequestID{Client: -1, Sequence: int32(slot)}, Command: state.Command{Op: state.NONE}}}
		}
		r.Ballot = c.ballot
		r.Committed = false
		c.log[slot] = &r
		c.pendingIndex.add(r.Request)
		c.assigned[r.Request.ID] = slot
	}
	keys := []defs.RequestID{}
	for id := range witness {
		keys = append(keys, id)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].Client != keys[j].Client {
			return keys[i].Client < keys[j].Client
		}
		return keys[i].Sequence < keys[j].Sequence
	})
	threshold := (c.n/2+1)/2 + 1 // ceil(f/2)+1
	for _, id := range keys {
		if bits.OnesCount64(witnessVotes[id]) < threshold {
			continue
		}
		if _, ok := c.assigned[id]; ok {
			continue
		}
		if _, ok := c.values[id]; ok {
			continue
		}
		c.high++
		r := Record{Slot: c.high, Ballot: c.ballot, Request: witness[id]}
		c.log[c.high] = &r
		c.pendingIndex.add(r.Request)
		c.assigned[id] = c.high
	}
	c.recoveryEnd = c.high
	c.votes = map[int64]replicaset.Set{}
	c.sendSuffix(c.id, c.executed+1, false)
	if c.executed >= c.recoveryEnd {
		c.activate()
	}
}

func (c *core) requestFetch(now time.Time) {
	if c.id == c.leader {
		return
	}
	// Gap notifications and leader heartbeats share a retry budget. A burst
	// of later accepts must not enqueue the same suffix for every packet.
	if now.Sub(c.lastFetch) < 100*time.Millisecond {
		c.fetchSuppressed++
		return
	}
	c.lastFetch = now
	c.fetchRequests++
	c.send(c.leader, &Packet{Kind: packetFetch, From: c.id, Ballot: c.ballot, Floor: c.executed + 1})
}

func (c *core) sendSuffix(to int32, start int64, fetch bool) {
	end := c.high
	if limit := start + 2048 - 1; end > limit {
		end = limit
	}
	for lo := start; lo <= end; lo += PageSize {
		recs := []Record{}
		for slot := lo; slot < lo+PageSize && slot <= end; slot++ {
			if r := c.log[slot]; r != nil {
				cp := *r
				cp.Ballot = c.ballot
				recs = append(recs, cp)
			}
		}
		if len(recs) == 0 {
			continue
		}
		if fetch && to != c.id {
			c.send(to, &Packet{Kind: packetAccept, From: c.id, Ballot: c.ballot, Records: recs})
			decided := []Record{}
			for _, r := range recs {
				if r.Committed {
					decided = append(decided, r)
				}
			}
			if len(decided) > 0 {
				c.send(to, &Packet{Kind: packetCommit, From: c.id, Ballot: c.ballot, Records: decided})
			}
		} else {
			for i := range recs {
				recs[i].Committed = false
			}
			p := &Packet{Kind: packetAccept, From: c.id, Ballot: c.ballot, Records: recs}
			c.broadcast(p)
			c.accept(p)
		}
	}
}
