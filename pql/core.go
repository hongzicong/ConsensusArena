package pql

// Protocol state and normal-case transitions.
import (
	"fmt"
	"time"

	"github.com/hongzicong/ConsensusArena/replica/defs"
	"github.com/hongzicong/ConsensusArena/replicaset"
	"github.com/hongzicong/ConsensusArena/state"
)

// core is owned exclusively by replica.EventLoop. PQL changes the read path
// and phase-two destination set; ordering and execution remain serialized.
type core struct {
	n                                       int
	id, leader                              int32
	policy                                  *leasePolicy
	lease                                   *leaseState
	control                                 bool
	configLog                               *core
	policies                                map[int64]*leasePolicy
	latestEpoch                             int64
	configSequence                          int32
	readStats                               map[state.Key][]uint64
	nextConfiguration                       time.Time
	unresponsive                            []time.Time
	suspected                               uint64
	lastConfigProposal                      time.Time
	clock                                   time.Time
	now                                     func() time.Time
	leaderHeard                             time.Time
	onChange                                func(int64, *LeaseChange)
	localPending                            []defs.RequestID
	localQueued                             map[defs.RequestID]bool
	frozen                                  []Packet
	frozenBallot, frozenFloor               int64
	ballot                                  int64
	active, preparing                       bool
	high, executed                          int64
	log                                     map[int64]*Record
	values                                  map[defs.RequestID]state.Value
	forwardResults                          map[defs.RequestID]state.Value
	assigned                                map[defs.RequestID]int64
	pending                                 map[defs.RequestID]Request
	votes                                   map[int64]replicaset.Set
	pages                                   map[int32]map[int32]Packet
	promises                                map[int32]bool
	queue                                   []defs.RequestID
	queued                                  map[defs.RequestID]bool
	recoveryEnd                             int64
	catchupTarget                           int64
	lastSend, lastFetch                     time.Time
	updating                                map[state.Key]int
	execute                                 func(state.Command) state.Value
	send                                    func(int32, *Packet)
	broadcast                               func(*Packet)
	sendMask                                func(uint64, *Packet)
	reply                                   func(Request, state.Value)
	recoveries, replayReplies, gapAccepts   uint64
	fetchRequests, fetchSuppressed          uint64
	localReads, forwardedReads, loggedReads uint64
	observeLocalRead                        func(defs.RequestID)
	leaseRejects, quorumSends               uint64
	resume                                  []Request
}

func newLogCore(n int, id, leader int32, policy *leasePolicy, execute func(state.Command) state.Value) *core {
	if n < 3 || n > 63 || n%2 == 0 {
		panic("PQL requires odd membership of 3..63 replicas")
	}
	if policy == nil || policy.n != n {
		panic("PQL requires a lease policy for the full membership")
	}
	return &core{n: n, id: id, leader: leader, policy: policy, lease: newLeaseState(n, id),
		ballot: int64(leader), active: true, high: -1, executed: -1, recoveryEnd: -1, catchupTarget: -1,
		log: map[int64]*Record{}, values: map[defs.RequestID]state.Value{}, forwardResults: map[defs.RequestID]state.Value{},
		assigned: map[defs.RequestID]int64{}, pending: map[defs.RequestID]Request{},
		votes: map[int64]replicaset.Set{}, pages: map[int32]map[int32]Packet{},
		promises: map[int32]bool{}, queued: map[defs.RequestID]bool{},
		updating: map[state.Key]int{}, execute: execute,
		localQueued: map[defs.RequestID]bool{}}
}

func newCore(n int, id, leader int32, policy *leasePolicy, execute func(state.Command) state.Value) *core {
	c := newLogCore(n, id, leader, policy, execute)
	c.policies = map[int64]*leasePolicy{genesisLeaseEpoch: policy}
	c.latestEpoch = genesisLeaseEpoch
	c.readStats = map[state.Key][]uint64{}
	c.unresponsive = make([]time.Time, n)
	c.configLog = newLogCore(n, id, leader, policy, nil)
	c.configLog.control = true
	c.configLog.onChange = c.commitConfiguration
	c.configLog.send = func(id int32, p *Packet) {
		cp := *p
		cp.Control = true
		c.send(id, &cp)
	}
	c.configLog.broadcast = func(p *Packet) {
		cp := *p
		cp.Control = true
		c.broadcast(&cp)
	}
	c.configLog.reply = func(Request, state.Value) {}
	return c
}

// Network event timestamps and ticker timestamps may be stale after a pause.
// The runtime supplies a fresh monotonic clock; deterministic drivers supply
// event times instead. Neither path may move protocol time backwards.
func (c *core) observeTime(now time.Time) time.Time {
	if c.now != nil {
		now = c.now()
	}
	if now.Before(c.clock) {
		return c.clock
	}
	c.clock = now
	return now
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

func (c *core) submit(r Request, now time.Time) {
	now = c.observeTime(now)
	if v, ok := c.values[r.ID]; ok {
		c.replayReplies++
		c.finish(r, v)
		return
	}
	if v, ok := c.forwardResults[r.ID]; ok {
		c.reply(r, v)
		return
	}
	if !c.control && c.id == c.leader && r.Command.Op == state.GET && r.Origin >= 0 && int(r.Origin) < c.n {
		if _, already := c.pending[r.ID]; !already {
			if c.readStats[r.Command.K] == nil {
				c.readStats[r.Command.K] = make([]uint64, c.n)
			}
			c.readStats[r.Command.K][r.Origin]++
		}
	}
	// A holder reads only after a majority of promises, catch-up to their
	// frontiers, and execution of every accepted write to this key.
	_, logged := c.assigned[r.ID]
	if !c.control && !logged && r.Command.Op == state.GET {
		readPolicy := c.policies[c.lease.promisedTo]
		if readPolicy == nil {
			readPolicy = c.policy
		}
		// Match the authors' handlePropose/reader: a configured holder waits
		// for lease validity, catch-up, and interfering writes to execute.
		// An excluded live ingress may retain an older incoming lease epoch
		// forever. Route its reads as a non-holder rather than waiting for
		// promises that the committed configuration will never grant it.
		excluded := c.policies[c.latestEpoch].excluded&(uint64(1)<<uint(c.id)) != 0
		isHolder := !excluded && readPolicy.holds(c.id, r.Command.K)
		if isHolder || c.id == c.leader {
			if isHolder && c.updating[r.Command.K] == 0 && c.lease.canRead(now, c.executed) {
				v := c.execute(r.Command)
				c.values[r.ID] = append(state.Value(nil), v...)
				delete(c.pending, r.ID)
				c.localReads++
				if r.Origin == c.id && c.observeLocalRead != nil {
					c.observeLocalRead(r.ID)
				}
				c.finish(r, v)
			} else {
				c.pending[r.ID] = r
				if !c.localQueued[r.ID] {
					c.localQueued[r.ID] = true
					c.localPending = append(c.localPending, r.ID)
				}
			}
			return
		}
	}
	c.pending[r.ID] = r
	if !c.active {
		return
	}
	if c.leader == c.id {
		if r.Command.Op == state.GET {
			c.loggedReads++
		}
		c.enqueue(r)
		return
	}
	if r.Command.Op == state.GET {
		c.forwardedReads++
	}
	c.send(c.leader, &Packet{Kind: packetForward, From: c.id, Ballot: c.ballot, Requests: []Request{r}})
}

func (c *core) finish(r Request, v state.Value) {
	if r.Origin == c.id {
		c.reply(r, v)
		return
	}
	// Every replica executes committed entries, but only the current leader
	// returns an ordered result to a forwarding origin.
	if c.id != c.leader {
		return
	}
	c.send(r.Origin, &Packet{Kind: packetForwardReply, From: c.id, Ballot: c.ballot,
		Requests: []Request{r}, Result: append(state.Value(nil), v...)})
}

func (c *core) tick(now time.Time, alive []bool) {
	now = c.observeTime(now)
	if c.leaderHeard.IsZero() {
		c.leaderHeard = now
	}
	if !c.control {
		c.maintainConfigurations(now, alive)
	}
	// TCP connectivity alone is not evidence that a proposer is responsive.
	// Use the normal command-log heartbeats, independently of lease traffic,
	// so losing lease messages does not disrupt a working Paxos leader.
	if !c.control && c.id != c.leader &&
		(!alive[c.leader] || now.Sub(c.leaderHeard) >= failureGrace) {
		c.suspected |= uint64(1) << uint(c.leader)
		candidate := int32(-1)
		for id, up := range alive {
			if up && int32(id) != c.leader &&
				c.policy.excluded&(uint64(1)<<uint(id)) == 0 &&
				(int32(id) == c.id || c.suspected&(uint64(1)<<uint(id)) == 0) {
				candidate = int32(id)
				break
			}
		}
		if candidate == c.id && !c.preparing {
			c.Begin(now)
		}
	}
	if !c.control {
		if len(c.localPending) > 0 {
			if c.id == c.leader {
				if c.lease.catchup > c.catchupTarget {
					c.catchupTarget = c.lease.catchup
				}
			} else {
				c.requestFetch(now)
			}
		}
		count := len(c.localPending)
		if count > PageSize {
			count = PageSize
		}
		for i := 0; i < count; i++ {
			id := c.localPending[0]
			c.localPending = c.localPending[1:]
			delete(c.localQueued, id)
			if r, ok := c.pending[id]; ok {
				c.submit(r, now)
			}
		}
		c.configLog.tick(now, alive)
	}
	if c.active {
		for i := 0; i < PageSize && len(c.resume) > 0; i++ {
			r := c.resume[0]
			c.resume[0] = Request{}
			c.resume = c.resume[1:]
			if _, ok := c.pending[r.ID]; ok {
				c.submit(r, now)
			}
		}
	}
	if c.preparing {
		if now.Sub(c.lastSend) >= time.Second {
			c.lastSend = now
			p := &Packet{Kind: packetPrepare, From: c.id, Ballot: c.ballot, Floor: c.executed + 1}
			c.broadcast(p)
			c.receive(p, now)
		}
		return
	}
	if c.leader == c.id {
		c.fillCatchup()
		if !c.active && c.executed >= c.recoveryEnd {
			c.activate()
		}
		if c.active {
			c.flush(now)
		}
		if now.Sub(c.lastSend) >= 200*time.Millisecond {
			c.lastSend = now
			c.broadcast(&Packet{Kind: packetReady, From: c.id, Ballot: c.ballot, High: c.executed, Floor: boolInt(c.active)})
			c.sendSuffix(c.id, c.executed+1, false, now)
		}
	}
}

func boolInt(v bool) int64 {
	if v {
		return 1
	}
	return 0
}

// Commands sharing a holder set may be batched. Different lease
// configurations use distinct Accept packets, as in the authors' code.
func (c *core) flush(now time.Time) {
	if c.high-c.executed >= AdmissionWindow {
		return
	}
	groups := make(map[uint64][]Record)
	order := make([]uint64, 0)
	count := 0
	for len(c.queue) > 0 && count < PageSize && c.high-c.executed < AdmissionWindow {
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
		c.install(rec)
		mask := c.policy.writeMask(r.Command)
		if c.control {
			mask = 0
		}
		if _, ok := groups[mask]; !ok {
			order = append(order, mask)
		}
		groups[mask] = append(groups[mask], rec)
		count++
	}
	for _, mask := range order {
		epoch := c.lease.promisedBy
		records := groups[mask]
		for i := range records {
			records[i].LeaseEpoch = epoch
			if !c.control && records[i].Request.Command.Op == state.PUT {
				records[i].NotifyMask = c.policy.holderMask(records[i].Request.Command.K)
			}
			c.log[records[i].Slot].LeaseEpoch = epoch
			c.log[records[i].Slot].NotifyMask = records[i].NotifyMask
		}
		p := &Packet{Kind: packetAccept, From: c.id, Ballot: c.ballot, LeaseEpoch: epoch, Records: records}
		c.quorumSends++
		if c.control {
			c.broadcast(p)
			c.accept(p, now)
		} else {
			c.sendMask(mask, p)
		}
	}
}

func (c *core) receive(p *Packet, now time.Time) {
	now = c.observeTime(now)
	if p.From < 0 || int(p.From) >= c.n {
		return
	}
	if p.Control && !c.control {
		c.configLog.receive(p, now)
		return
	}
	if !c.control {
		if p.Kind == packetSuspect {
			// A peer's network view is not this replica's failure evidence.
			// Retain decoding compatibility, but use local observations for
			// exclusions, as in the authors' leader-side failure detector.
			return
		}
		if p.Kind >= packetGuard && p.Kind <= packetLeaseReply {
			if p.Kind == packetLeasePromise && c.policies[p.LeaseEpoch] == nil {
				c.configLog.requestFetch(now)
				return
			}
			if p.Kind == packetGuardAck && (c.latestEpoch > c.lease.promisedBy || c.hasUnexcludedSuspect()) {
				return
			}
			c.lease.receive(p, now, c.high, c.send)
			return
		}
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
		c.leaderHeard = now
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
	if p.From == c.leader && (p.Kind == packetReady || p.Kind == packetAccept || p.Kind == packetCommit) {
		c.leaderHeard = now
	}
	switch p.Kind {
	case packetPromise:
		c.promiseReply(p, now)
	case packetAccept:
		if p.From == c.leader {
			c.accept(p, now)
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
			c.requestFetch(now)
		}
	case packetFetch:
		if c.leader == c.id {
			if p.High > c.catchupTarget {
				c.catchupTarget = p.High
			}
			// Never send new-ballot accepts before phase one completes.
			if !c.preparing {
				c.fillCatchup()
				c.sendSuffix(p.From, p.Floor, true, now)
			}
		}
	case packetForward:
		if c.id == c.leader && c.active {
			for _, r := range p.Requests {
				c.submit(r, now)
			}
		}
	case packetForwardReply:
		if p.From != c.leader || len(p.Requests) != 1 {
			return
		}
		r := p.Requests[0]
		if r.Origin != c.id {
			return
		}
		c.forwardResults[r.ID] = append(state.Value(nil), p.Result...)
		delete(c.pending, r.ID)
		c.reply(r, p.Result)
	}
}

func (c *core) install(r Record) {
	if old := c.log[r.Slot]; old != nil {
		if old.Committed {
			if old.Request.ID != r.Request.ID || recoveryNoop(old.Request) != recoveryNoop(r.Request) {
				panic(fmt.Sprintf("conflicting decision at %d", r.Slot))
			}
			return
		}
		if old.Request.ID == r.Request.ID && recoveryNoop(old.Request) == recoveryNoop(r.Request) {
			cp := r
			c.log[r.Slot] = &cp
			return
		}
		if old.Request.Command.Op == state.PUT {
			c.removeUpdating(old.Request.Command.K)
		}
		if !recoveryNoop(old.Request) {
			delete(c.assigned, old.Request.ID)
		}
	}
	cp := r
	c.log[r.Slot] = &cp
	if !recoveryNoop(r.Request) {
		c.assigned[r.Request.ID] = r.Slot
	}
	if r.Slot > c.executed && r.Request.Command.Op == state.PUT {
		c.updating[r.Request.Command.K]++
	}
	if r.Slot > c.high {
		c.high = r.Slot
	}
}

func (c *core) removeUpdating(key state.Key) {
	if c.updating[key] <= 1 {
		delete(c.updating, key)
	} else {
		c.updating[key]--
	}
}

func (c *core) accept(p *Packet, now time.Time) {
	if !c.control && (c.policies[p.LeaseEpoch] == nil ||
		(p.LeaseEpoch != c.lease.promisedBy && !c.lease.canWriteOutside(now))) {
		c.leaseRejects++
		if c.policies[p.LeaseEpoch] == nil {
			c.configLog.requestFetch(now)
		}
		return
	}
	accepted := []Record{}
	gap := false
	for _, r := range p.Records {
		if r.Slot > 0 && c.log[r.Slot-1] == nil {
			gap = true
			c.gapAccepts++
		}
		r.Ballot = p.Ballot
		r.LeaseEpoch = p.LeaseEpoch
		c.install(r)
		accepted = append(accepted, Record{Slot: r.Slot})
	}
	if gap {
		c.requestFetch(now)
	}
	if len(accepted) > 0 {
		c.send(c.leader, &Packet{Kind: packetAck, From: c.id, Ballot: c.ballot, LeaseEpoch: p.LeaseEpoch, Records: accepted})
	}
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
		r := c.log[ack.Slot]
		if r == nil || (!c.control && r.LeaseEpoch != p.LeaseEpoch) {
			continue
		}
		c.votes[ack.Slot] = c.votes[ack.Slot].With(int(p.From))
		if r == nil || r.Ballot != c.ballot || r.Committed || c.votes[ack.Slot].Size() < c.n/2+1 {
			continue
		}
		if uint64(c.votes[ack.Slot])&r.NotifyMask != r.NotifyMask {
			continue
		}
		r.Committed = true
		committed = append(committed, *r)
	}
	if len(committed) > 0 {
		c.broadcast(&Packet{Kind: packetCommit, From: c.id, Ballot: c.ballot, Records: committed})
	}
	c.apply()
}
