package kcensus

import (
	"bytes"
	"encoding/gob"
	"sort"
	"time"

	"github.com/hongzicong/ConsensusArena/replica/defs"
	"github.com/hongzicong/ConsensusArena/state"
)

// Recovery contract (crash-stop, authenticated lossless FIFO, no restart):
// A donor exports only its executed prefix, never an accepted/undecided suffix.
// A checkpoint contains the latest PUT per application key, that shard's PUT
// completion IDs, the executed slot and last_v. Installing it is one event-loop
// transition; slots above the cut and losing pending inputs survive. The KV
// overlay is sufficient because this package supports only PUT/GET, no delete.
// Donor and receiver start from identical preloaded state. Only this shard's
// completion IDs cross the cut; a global client watermark could skip requests
// on other shards. A correct non-Byzantine donor's executed state is sufficient:
// installing it learns a chosen prefix and does not certify new decisions.
// No normal message, quorum, decision rule or executor selection is changed.
// Unlike a complete recovery log, overwritten values and slot objects are freed.
// Sparse deduplication ranges can still grow with out-of-order request IDs.
type shardCheckpoint struct {
	values    map[state.Key]state.Value
	completed map[int32]*completedSequences
}

type checkpointCompletion struct {
	Client int32
	Floor  int32
	Ranges [][2]int32
}

type checkpointImage struct {
	Version   uint8
	Slot, UID uint64
	Values    []state.Command
	Completed []checkpointCompletion
}

type snapshotInput struct {
	from     int
	sequence uint64
	cut      uint64
	count    uint64
	parts    [][]byte
}

const checkpointChunkBytes = 60 << 10

func (c *core) checkpointWrite(k state.Key, r Record) {
	if c.checkpoints == nil {
		c.checkpoints = make(map[state.Key]*shardCheckpoint)
	}
	p := c.checkpoints[k]
	if p == nil {
		p = &shardCheckpoint{values: make(map[state.Key]state.Value), completed: make(map[int32]*completedSequences)}
		c.checkpoints[k] = p
	}
	// Protocol/application values are immutable after admission. Retain just the
	// newest slice, sharing its bytes with KV, instead of copying every history.
	p.values[r.Command.K] = r.Command.V
	s := p.completed[r.ID.Client]
	if s == nil {
		s = &completedSequences{floor: -1}
		p.completed[r.ID.Client] = s
	}
	s.add(r.ID.Sequence)
}

func (c *core) checkpointHas(k state.Key, id defs.RequestID) bool {
	p := c.checkpoints[k]
	return p != nil && p.completed[id.Client] != nil && p.completed[id.Client].contains(id.Sequence)
}

// Only recovery traffic uses these limits. Each sender needs one chunk credit
// from its receiver before producing the next wire frame. The image budget may
// admit one oversized shard image so a large checkpoint cannot block forever.
const checkpointWindow = 32
const checkpointImageBudget = 4 << 20
const checkpointPendingSends = 128

type checkpointRequest struct {
	key                       state.Key
	target, request, executed uint64
	donor, preferred          int
	tried                     uint64
	probe                     bool
	at, due, backoff          time.Duration
}

type checkpointDonor struct {
	key   state.Key
	peer  int
	valid bool
}

type snapshotTarget struct {
	peer int
	key  state.Key
}
type snapshotOutput struct {
	target             snapshotTarget
	request, next, cut uint64
	data               []byte
	part, parts        int
	waiting            bool
	at                 time.Duration
}

func (c *core) checkpointTimeout() time.Duration { return max(c.failure, 4*c.retry) }

// Calls from GET fences, slot retries and result lookups coalesce here. target
// is a wanted execution position, never evidence that a value was decided.
func (c *core) requestCheckpoint(k state.Key, next uint64, probe bool) {
	if c.checkpointRequests == nil {
		c.checkpointRequests = make(map[state.Key]*checkpointRequest)
	}
	q := c.checkpointRequests[k]
	if q == nil {
		q = &checkpointRequest{key: k, target: next, executed: c.executed(k), donor: -1, preferred: -1, probe: probe, backoff: c.checkpointTimeout()}
		if d := c.checkpointDonors[uint64(k)%64]; d.valid && d.key == k {
			q.preferred = d.peer
		}
		c.checkpointRequests[k] = q
		c.checkpointQueue = append(c.checkpointQueue, q)
	} else {
		q.target = max(q.target, next)
		q.probe = q.probe && probe
		c.stats.CheckpointMerged++
	}
	c.driveCheckpoint(q)
}

func (c *core) releaseCheckpoint(q *checkpointRequest, cancel bool) {
	if q.donor >= 0 {
		if cancel {
			c.send(q.donor, message{Kind: checkpointAck, Shard: q.key, Ballot: q.request})
		}
		c.checkpointActive--
	}
	q.donor = -1
	delete(c.snapshotInputs, q.key)
}

func (c *core) checkpointAdvanced(k state.Key) {
	q := c.checkpointRequests[k]
	if q == nil {
		return
	}
	executed := c.executed(k)
	if executed >= q.target && len(c.resultByShard[k]) == 0 {
		c.releaseCheckpoint(q, true)
		delete(c.checkpointRequests, k)
		return
	}
	if executed > q.executed {
		q.executed, q.at, q.backoff = executed, c.now, c.checkpointTimeout()
	}
}

func (c *core) switchCheckpoint(q *checkpointRequest) {
	if q.donor >= 0 {
		q.tried |= uint64(1) << q.donor
	}
	c.releaseCheckpoint(q, true)
	c.stats.CheckpointSwitches++
	q.due = c.now + min(q.backoff/4, c.checkpointTimeout())
	q.backoff = min(2*q.backoff, 32*c.checkpointTimeout())
}

func (c *core) driveCheckpoint(q *checkpointRequest) {
	c.checkpointAdvanced(q.key)
	if c.checkpointRequests[q.key] != q {
		return
	}
	if q.donor >= 0 {
		if !c.peerFailed(q.donor) && c.now-q.at < q.backoff {
			return
		}
		c.switchCheckpoint(q)
	}
	if c.now < q.due || c.checkpointActive >= checkpointWindow {
		return
	}
	eligible := func(id int) bool {
		return id >= 0 && id < c.n && id != c.id && !c.peerFailed(id) && q.tried&(uint64(1)<<id) == 0
	}
	donor := q.preferred
	if !eligible(donor) {
		donor = -1
		// Spread shards across voters instead of using the global census hub.
		first := (uint64(q.key)*11400714819323198485 + uint64(c.id)) % uint64(c.n)
		for i := 0; i < c.n; i++ {
			id := int((first + uint64(i)) % uint64(c.n))
			if eligible(id) {
				donor = id
				break
			}
		}
	}
	if donor < 0 {
		q.tried = 0
		q.due = c.now + q.backoff
		return
	}
	c.snapshotSequence++
	if c.snapshotSequence == 0 {
		panic("checkpoint request sequence exhausted")
	}
	q.donor, q.request, q.at = donor, c.snapshotSequence, c.now
	c.checkpointActive++
	c.stats.CheckpointRequests++
	c.send(donor, message{Kind: fetch, Shard: q.key, Slot: c.executed(q.key) + 1, Ballot: q.request, Explicit: q.probe})
}

// Retired Prepare/Accept messages advertise progress, never eagerly serialize
// a snapshot. The requester chooses one donor through the same catch-up state.
func (c *core) hintCheckpoint(to int, k state.Key) {
	if to < 0 || to >= c.n || to == c.id || c.peerFailed(to) {
		return
	}
	c.send(to, message{Kind: checkpointStatus, Shard: k, Slot: c.executed(k)})
	c.stats.CheckpointHints++
}

func (c *core) receiveCheckpointStatus(m message) {
	if m.From < 0 || m.From >= c.n || m.From == c.id || c.peerFailed(m.From) {
		return
	}
	if m.Ballot == 0 {
		if m.Slot <= c.executed(m.Shard) {
			return
		}
		if q := c.checkpointRequests[m.Shard]; q == nil {
			c.checkpointDonors[uint64(m.Shard)%64] = checkpointDonor{key: m.Shard, peer: m.From, valid: true}
		} else if q.donor < 0 && q.tried&(uint64(1)<<m.From) == 0 {
			q.preferred = m.From
		}
		c.requestCheckpoint(m.Shard, m.Slot, true)
		return
	}
	q := c.checkpointRequests[m.Shard]
	if q == nil || q.donor != m.From || q.request != m.Ballot {
		return
	}
	// A behind/busy donor cannot supply this attempt. This is not a failure
	// declaration and does not change quorum membership or leader eligibility.
	c.switchCheckpoint(q)
}

func (c *core) queueSnapshot(m message) {
	key := snapshotTarget{m.From, m.Shard}
	if c.snapshotOutputs == nil {
		c.snapshotOutputs = make(map[snapshotTarget]*snapshotOutput)
	}
	if old := c.snapshotOutputs[key]; old != nil {
		if old.request >= m.Ballot {
			c.stats.CheckpointSuppressed++
			return
		}
		c.dropSnapshot(old)
	}
	if len(c.snapshotOutputs) >= checkpointPendingSends {
		c.send(m.From, message{Kind: checkpointStatus, Shard: m.Shard, Slot: c.executed(m.Shard), Ballot: m.Ballot, Explicit: true})
		return
	}
	s := &snapshotOutput{target: key, request: m.Ballot, next: m.Slot, at: c.now}
	c.snapshotOutputs[key] = s
	c.snapshotQueue = append(c.snapshotQueue, s)
}

func (c *core) dropSnapshot(s *snapshotOutput) {
	if c.snapshotOutputs[s.target] != s {
		return
	}
	delete(c.snapshotOutputs, s.target)
	if s.data != nil {
		c.snapshotActive--
		c.snapshotBytes -= len(s.data)
		s.data = nil
	}
}

func (c *core) receiveCheckpointAck(m message) {
	s := c.snapshotOutputs[snapshotTarget{m.From, m.Shard}]
	if s == nil || s.request != m.Ballot {
		return
	}
	if m.High == 0 {
		c.dropSnapshot(s)
		return
	}
	if !s.waiting || m.Slot != s.cut || m.High != uint64(s.part+1) {
		return
	}
	s.part++
	s.waiting = false
	s.at = c.now
	if s.part == s.parts {
		c.dropSnapshot(s)
	}
}

func (c *core) encodeSnapshot(s *snapshotOutput) {
	k := s.target.key
	p := c.checkpoints[k]
	if p == nil || c.executed(k) < s.next {
		c.dropSnapshot(s)
		return
	}
	uid, ok := c.lastValue(k)
	if !ok {
		panic("executed shard has no predecessor")
	}
	image := checkpointImage{Version: 1, Slot: c.executed(k), UID: uid}
	for key, value := range p.values {
		image.Values = append(image.Values, state.Command{Op: state.PUT, K: key, V: value})
	}
	sort.Slice(image.Values, func(i, j int) bool { return image.Values[i].K < image.Values[j].K })
	for client, done := range p.completed {
		entry := checkpointCompletion{Client: client, Floor: done.floor}
		for _, r := range done.ranges {
			entry.Ranges = append(entry.Ranges, [2]int32{r.first, r.last})
		}
		image.Completed = append(image.Completed, entry)
	}
	sort.Slice(image.Completed, func(i, j int) bool { return image.Completed[i].Client < image.Completed[j].Client })
	var body bytes.Buffer
	if err := gob.NewEncoder(&body).Encode(image); err != nil {
		panic(err)
	}
	s.data, s.cut = body.Bytes(), image.Slot
	s.parts = (len(s.data) + checkpointChunkBytes - 1) / checkpointChunkBytes
	c.snapshotActive++
	c.snapshotBytes += len(s.data)
	c.stats.CheckpointTransfers++
}

func (c *core) tickCheckpoints() {
	for left := min(256, len(c.checkpointQueue)-c.checkpointHead); left > 0; left-- {
		q := c.checkpointQueue[c.checkpointHead]
		c.checkpointQueue[c.checkpointHead] = nil
		c.checkpointHead++
		if c.checkpointRequests[q.key] != q {
			continue
		}
		c.driveCheckpoint(q)
		if c.checkpointRequests[q.key] == q {
			c.checkpointQueue = append(c.checkpointQueue, q)
		}
	}
	if c.checkpointHead > 256 && c.checkpointHead*2 >= len(c.checkpointQueue) {
		n := copy(c.checkpointQueue, c.checkpointQueue[c.checkpointHead:])
		clear(c.checkpointQueue[n:])
		c.checkpointQueue = c.checkpointQueue[:n]
		c.checkpointHead = 0
	}
	encodes := 1
	for left := min(checkpointPendingSends, len(c.snapshotQueue)-c.snapshotHead); left > 0; left-- {
		s := c.snapshotQueue[c.snapshotHead]
		c.snapshotQueue[c.snapshotHead] = nil
		c.snapshotHead++
		if c.snapshotOutputs[s.target] != s {
			continue
		}
		if c.peerFailed(s.target.peer) || c.now-s.at >= 64*c.checkpointTimeout() {
			c.dropSnapshot(s)
			continue
		}
		c.snapshotQueue = append(c.snapshotQueue, s)
		if s.data == nil {
			if encodes == 0 || c.snapshotActive >= checkpointWindow || c.snapshotBytes >= checkpointImageBudget {
				continue
			}
			encodes--
			c.encodeSnapshot(s)
			if c.snapshotOutputs[s.target] != s {
				continue
			}
		}
		if s.waiting {
			continue
		}
		first := s.part * checkpointChunkBytes
		end := min(first+checkpointChunkBytes, len(s.data))
		c.send(s.target.peer, message{Kind: snapshotPart, Shard: s.target.key, Slot: s.cut, Ballot: s.request, High: uint64(s.parts), GraphTime: int64(s.part), Digest: c.plan.Digest, Result: s.data[first:end]})
		s.waiting = true
		s.at = c.now
	}
	if c.snapshotHead > checkpointPendingSends && c.snapshotHead*2 >= len(c.snapshotQueue) {
		n := copy(c.snapshotQueue, c.snapshotQueue[c.snapshotHead:])
		clear(c.snapshotQueue[n:])
		c.snapshotQueue = c.snapshotQueue[:n]
		c.snapshotHead = 0
	}
}

func (c *core) receiveSnapshot(m message) {
	if m.From < 0 || m.From >= c.n || m.From == c.id || c.peerFailed(m.From) ||
		m.Digest != c.plan.Digest || m.Slot <= c.executed(m.Shard) || m.Ballot == 0 ||
		m.High == 0 || m.GraphTime < 0 || uint64(m.GraphTime) >= m.High ||
		len(m.Result) == 0 || len(m.Result) > checkpointChunkBytes ||
		c.checkpointRequests[m.Shard] == nil ||
		c.checkpointRequests[m.Shard].donor != m.From || c.checkpointRequests[m.Shard].request != m.Ballot {
		return
	}
	if c.snapshotInputs == nil {
		c.snapshotInputs = make(map[state.Key]*snapshotInput)
	}
	a := c.snapshotInputs[m.Shard]
	if a != nil && (c.peerFailed(a.from) || a.cut <= c.executed(m.Shard)) {
		delete(c.snapshotInputs, m.Shard)
		a = nil
	}
	if a == nil {
		// FIFO puts part zero first. Append on receipt rather than allocating
		// from the sender's count. Other transfers cannot replace a live one.
		if m.GraphTime != 0 {
			return
		}
		a = &snapshotInput{from: m.From, sequence: m.Ballot, cut: m.Slot, count: m.High}
		c.snapshotInputs[m.Shard] = a
	}
	if a.from != m.From || a.sequence != m.Ballot || a.cut != m.Slot || a.count != m.High ||
		uint64(len(a.parts)) != uint64(m.GraphTime) {
		return
	}
	a.parts = append(a.parts, m.Result)
	q := c.checkpointRequests[m.Shard]
	q.at = c.now // actual chunk progress extends this donor attempt
	c.send(m.From, message{Kind: checkpointAck, Shard: m.Shard, Slot: m.Slot, Ballot: m.Ballot, High: uint64(len(a.parts))})
	c.stats.CheckpointChunks++
	if uint64(len(a.parts)) != a.count {
		return
	}
	delete(c.snapshotInputs, m.Shard)
	var image checkpointImage
	if err := gob.NewDecoder(bytes.NewReader(bytes.Join(a.parts, nil))).Decode(&image); err != nil {
		panic(err)
	}
	if image.Version != 1 || image.Slot != a.cut {
		panic("checkpoint cut/version mismatch")
	}
	c.checkpointDonors[uint64(m.Shard)%64] = checkpointDonor{key: m.Shard, peer: m.From, valid: true}
	c.releaseCheckpoint(q, false)
	q.preferred, q.tried, q.due, q.backoff = m.From, 0, c.now, c.checkpointTimeout()
	c.installCheckpoint(m.Shard, image)
}

func (c *core) installCheckpoint(k state.Key, image checkpointImage) {
	if image.Slot <= c.executed(k) {
		return
	}
	p := &shardCheckpoint{values: make(map[state.Key]state.Value), completed: make(map[int32]*completedSequences)}
	for _, cmd := range image.Values {
		if cmd.Op != state.PUT || c.shardFor(cmd.K) != k || len(cmd.V) > 65535 {
			panic("checkpoint value outside shard")
		}
		if _, exists := p.values[cmd.K]; exists {
			panic("duplicate checkpoint key")
		}
		p.values[cmd.K] = cmd.V
	}
	for _, entry := range image.Completed {
		if entry.Client < 0 || int(entry.Client) >= c.m || entry.Floor < -1 || p.completed[entry.Client] != nil {
			panic("invalid checkpoint completion owner")
		}
		s := &completedSequences{floor: -1}
		if entry.Floor >= 0 {
			s.addRange(0, entry.Floor)
		}
		previous := int64(entry.Floor)
		for _, r := range entry.Ranges {
			if int64(r[0]) <= previous || r[0] < 0 || r[1] < r[0] {
				panic("invalid checkpoint completion range")
			}
			s.addRange(r[0], r[1])
			previous = int64(r[1])
		}
		p.completed[entry.Client] = s
	}
	// Nothing above the cut is retired. In particular, accepted suffix ballots
	// and pending losing Singles are not overwritten by an application snapshot.
	s := c.shard(k)
	for i, x := range s.slots {
		if i <= image.Slot {
			if x.phase != 0 && x.decided == nil {
				c.activeCensuses--
			}
			delete(c.active, slotID{k, i})
			delete(s.slots, i)
			c.retiredSlots = append(c.retiredSlots, x)
		}
	}
	for _, cmd := range image.Values {
		if c.execute != nil {
			c.execute(cmd)
		}
	}
	if c.checkpoints == nil {
		c.checkpoints = make(map[state.Key]*shardCheckpoint)
	}
	c.checkpoints[k] = p
	s.executed, s.lastUID, s.hasLast = image.Slot, image.UID, true
	if s.high < s.executed {
		s.high = s.executed
	}
	for client, done := range p.completed {
		// The ordinary executor itself may be the lagging node. Return newly
		// imported PUTs even if their original payload never arrived here.
		if int(client) >= c.n && c.plan.Leaders[client] == c.id && !c.peerFailed(int(client)) {
			replyRange := func(first, last int32) {
				for seq := int64(first); seq <= int64(last); seq++ {
					id := defs.RequestID{Client: client, Sequence: int32(seq)}
					if !c.requestCompleted(id) {
						c.send(int(client), message{Kind: executedResult, Shard: k, Slot: image.Slot, ID: id, Result: state.NIL()})
					}
				}
			}
			if done.floor >= 0 {
				replyRange(0, done.floor)
			}
			for _, r := range done.ranges {
				replyRange(r.first, r.last)
			}
		}
		dst := c.completions(client)
		if done.floor >= 0 {
			dst.addRange(0, done.floor)
		}
		for _, r := range done.ranges {
			dst.addRange(r.first, r.last)
		}
	}
	for id := range s.queued {
		if c.checkpointHas(k, id) {
			if r, ok := c.known[id]; ok {
				c.complete(id, state.NIL())
				c.forgetRequest(k, r)
			}
		}
	}
	c.purgeCommitted(k, s)
	delete(c.snapshotInputs, k)
	c.stats.CheckpointInstalls++
	for id := range c.resultByShard[k] {
		c.finishResultWaiter(id, k, s.executed)
	}
	// Resume reads, suffix delivery and original inputs through existing rules.
	c.apply(k)
}
