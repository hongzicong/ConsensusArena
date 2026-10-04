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

func (c *Core) Begin(now time.Time) {
	c.Ballot = (c.Ballot/int64(c.N)+1)*int64(c.N) + int64(c.ID)
	c.Leader = c.ID
	c.Active = false
	c.Preparing = true
	c.Recoveries++
	c.pages = map[int32]map[int32]Packet{}
	c.promises = map[int32]bool{}
	c.votes = map[int64]replicaset.Set{}
	c.lastSend = now
	p := &Packet{Kind: packetPrepare, From: c.ID, Ballot: c.Ballot, Floor: c.Executed + 1}
	c.SendToAll(p)
	c.Handle(p)
}

func (c *Core) activate() {
	c.Active = true
	c.SendToAll(&Packet{Kind: packetReady, From: c.ID, Ballot: c.Ballot, High: c.Executed, Floor: 1})
	c.schedulePending()
}

func (c *Core) schedulePending() {
	c.resume = make([]Request, 0, len(c.Pending))
	for _, r := range c.Pending {
		c.resume = append(c.resume, r)
	}
}

func (c *Core) promise(p *Packet) {

	var snapshot []Packet
	values := []Record{}
	for slot := p.Floor; slot <= c.High; slot++ {
		if r := c.Log[slot]; r != nil {
			values = append(values, *r)
		}
	}
	witness := []Request{}
	{
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
		reply := &Packet{Kind: packetPromise, From: c.ID, Ballot: c.Ballot, Floor: p.Floor, High: c.High, Page: int32(page), Pages: int32(count)}
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
		c.Send(p.From, &snapshot[i])
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
	witnessVotes := map[defs.RequestID]uint64{}
	witness := map[defs.RequestID]Request{}
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
	c.Assigned = map[defs.RequestID]int64{}
	c.pendingIndex = newConflictIndex()
	for slot, r := range c.Log {
		if slot <= c.Executed {
			c.Assigned[r.Request.ID] = slot
		}
	}
	for slot := c.Executed + 1; slot <= high; slot++ {
		r, ok := selected[slot]
		if !ok {
			r = Record{Slot: slot, Request: Request{ID: defs.RequestID{Client: -1, Sequence: int32(slot)}, Command: state.Command{Op: state.NONE}}}
		}
		r.Ballot = c.Ballot
		r.Committed = false
		c.Log[slot] = &r
		c.pendingIndex.add(r.Request)
		c.Assigned[r.Request.ID] = slot
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
	c.votes = map[int64]replicaset.Set{}
	c.sendSuffix(c.ID, c.Executed+1, false)
	if c.Executed >= c.recoveryEnd {
		c.activate()
	}
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
	c.Send(c.Leader, &Packet{Kind: packetFetch, From: c.ID, Ballot: c.Ballot, Floor: c.Executed + 1})
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
			c.Send(to, &Packet{Kind: packetAccept, From: c.ID, Ballot: c.Ballot, Records: recs})
			decided := []Record{}
			for _, r := range recs {
				if r.Committed {
					decided = append(decided, r)
				}
			}
			if len(decided) > 0 {
				c.Send(to, &Packet{Kind: packetCommit, From: c.ID, Ballot: c.Ballot, Records: decided})
			}
		} else {
			for i := range recs {
				recs[i].Committed = false
			}
			p := &Packet{Kind: packetAccept, From: c.ID, Ballot: c.Ballot, Records: recs}
			c.SendToAll(p)
			c.accept(p)
		}
	}
}
