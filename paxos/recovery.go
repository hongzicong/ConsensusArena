package paxos

// Protocol recovery transitions and recovery-specific helpers.
import (
	"time"

	"github.com/hongzicong/ConsensusArena/replica/defs"
	"github.com/hongzicong/ConsensusArena/replicaset"
	"github.com/hongzicong/ConsensusArena/state"
)

func (c *core) Begin(now time.Time) {
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
	c.broadcast(p)
	c.receive(p)
}

func (c *core) activate() {
	c.Active = true
	c.broadcast(&Packet{Kind: packetReady, From: c.ID, Ballot: c.Ballot, High: c.Executed, Floor: 1})
	c.schedulePending()
}

func (c *core) schedulePending() {
	c.resume = make([]Request, 0, len(c.Pending))
	for _, r := range c.Pending {
		c.resume = append(c.resume, r)
	}
}

func (c *core) promise(p *Packet) {
	if len(c.frozen) > 0 && c.frozenBallot == p.Ballot && c.frozenFloor == p.Floor {
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
	count := (len(values) + PageSize - 1) / PageSize
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
		snapshot = append(snapshot, *reply)
	}
	{
		c.frozen, c.frozenBallot, c.frozenFloor = snapshot, p.Ballot, p.Floor
	}
	for i := range snapshot {
		c.send(p.From, &snapshot[i])
	}
}

func (c *core) promiseReply(p *Packet) {
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
		}
	}
	c.Preparing = false
	c.High = high
	c.Assigned = map[defs.RequestID]int64{}
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
		c.Assigned[r.Request.ID] = slot
	}
	c.recoveryEnd = c.High
	c.votes = map[int64]replicaset.Set{}
	c.sendSuffix(c.ID, c.Executed+1, false)
	if c.Executed >= c.recoveryEnd {
		c.activate()
	}
}

func (c *core) requestFetch(now time.Time) {
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
	c.send(c.Leader, &Packet{Kind: packetFetch, From: c.ID, Ballot: c.Ballot, Floor: c.Executed + 1})
}

func (c *core) sendSuffix(to int32, start int64, fetch bool) {
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
			c.send(to, &Packet{Kind: packetAccept, From: c.ID, Ballot: c.Ballot, Records: recs})
			decided := []Record{}
			for _, r := range recs {
				if r.Committed {
					decided = append(decided, r)
				}
			}
			if len(decided) > 0 {
				c.send(to, &Packet{Kind: packetCommit, From: c.ID, Ballot: c.Ballot, Records: decided})
			}
		} else {
			for i := range recs {
				recs[i].Committed = false
			}
			p := &Packet{Kind: packetAccept, From: c.ID, Ballot: c.Ballot, Records: recs}
			c.broadcast(p)
			c.accept(p)
		}
	}
}
