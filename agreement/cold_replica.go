package agreement

import (
	"encoding/binary"
	"encoding/json"
	"github.com/hongzicong/ConsensusArena/replica/defs"
	"github.com/hongzicong/ConsensusArena/state"
	"math/bits"
	"time"
)

func (c *coldReplica) proposeEnvelope(p *defs.GPropose, now time.Time) bool {
	if p.Command.Op != coldPUT && p.Command.Op != coldProof {
		return false
	}
	if !c.enabled {
		return true
	}
	if p.Command.Op == coldPUT {
		if len(p.Command.V) < 20 {
			return true
		}
		g := coldGeneration{B: binary.LittleEndian.Uint64(p.Command.V[:8]), E: binary.LittleEndian.Uint64(p.Command.V[8:16])}
		origin := int(binary.LittleEndian.Uint32(p.Command.V[16:20]))
		if origin < 0 || origin >= c.r.N {
			return true
		}
		req := request{Proposal: *p.Propose, Origin: origin}
		req.Proposal.Command.Op = state.PUT
		req.Proposal.Command.V = p.Command.V[20:]
		if c.isCold(req.Proposal.Command.K) {
			c.intent(req, g, now)
		}
	} else {
		if len(p.Command.V) < 4 {
			return true
		}
		n := int(binary.LittleEndian.Uint32(p.Command.V[:4]))
		if n < 1 || n > len(p.Command.V)-4 {
			return true
		}
		var rec coldRecord
		if json.Unmarshal(p.Command.V[4:4+n], &rec) != nil || rec.ID != p.RequestID() || rec.Key != p.Command.K {
			return true
		}
		req := request{Proposal: *p.Propose, Origin: rec.Origin}
		req.Proposal.Command.Op = state.PUT
		req.Proposal.Command.V = p.Command.V[4+n:]
		if commandDigest(req.Proposal.Command) == rec.Hash {
			c.retain(req)
			if !c.migrated && !c.hasBridge() {
				c.chosen(rec, now)
			}
		}
	}

	return true
}
func (c *coldReplica) intercept(req request, now time.Time) bool {
	if c.migrated {
		return false
	}
	if !c.enabled || req.Stage != 0 {
		return false
	}
	cmd := req.Proposal.Command
	if cmd.Op == state.SCAN {
		return c.submitScan(req, now)
	}
	if !c.isCold(cmd.K) {
		return false
	}
	switch cmd.Op {
	case state.PUT:
		c.intent(req, c.installed, now)
		return true
	case state.GET:
		c.read(req, now)
		return true
	case cancelRead:
		delete(c.held, req.id())
		delete(c.reads, req.id())
		return true
	}
	return false
}
func (c *coldReplica) retain(req request) [32]byte {
	h := commandDigest(req.Proposal.Command)
	if _, ok := c.inputs[h]; !ok {
		c.inputs[h] = req
	}
	return h
}
func (c *coldReplica) intent(req request, hint coldGeneration, now time.Time) {
	if c.migrated {
		c.r.engine.submit(req, now)
		return
	}
	c.stats.Arrivals++
	h := c.retain(req)
	id := req.id()
	if old, ok := c.done[id]; ok {
		if old == h {
			c.replyDone(req, h)
		}
		c.stats.Repeated++
		return
	}
	s := c.key(req.Proposal.Command.K)
	s.Pending[id] = req
	if _, queued := c.replayPending[id]; !queued {
		c.replayQueue = append(c.replayQueue, id)
	}
	c.replayPending[id] = req
	if !c.ready || c.frozen || c.installed != c.promised || !c.r.engine.active() {
		return
	}
	if c.q&bit(int(c.r.ID)) == 0 && s.Fast {
		if s.Since.IsZero() {
			s.Since = now
		}
		if hint != c.installed && c.r.engine.id == c.r.engine.current.Leader {
			for i := 0; i < c.r.N; i++ {
				if c.q&bit(i) != 0 {
					c.send(i, coldMessage{Type: "intent", G: c.installed, Request: req})
				}
			}
		}
		return
	}
	if hint != c.installed || !s.Fast {
		if c.r.engine.id == c.r.engine.current.Leader {
			c.ensureKeyPrepare(req.Proposal.Command.K, false, now)
		} else {
			c.send(c.r.engine.current.Leader, coldMessage{Type: "intent", G: c.installed, Request: req})
		}
		return
	}
	v := s.Prefix + 1
	if old, ok := s.Accepted[v]; ok && old.Round == s.Promise {
		if old.ID == id && old.Hash == h {
			c.replyVote(old)
			return
		}
		c.stats.Collisions++
		c.send(c.r.engine.current.Leader, coldMessage{Type: "collision", G: c.installed, Request: req, Key: req.Proposal.Command.K})
		return
	}
	rec := coldRecord{ColdVoteIdentity: ColdVoteIdentity{Round: s.Promise, Key: req.Proposal.Command.K, Version: v, Q: c.q, ID: id, Origin: req.Origin, Timestamp: req.Proposal.Timestamp, Hash: h}, Fast: true}
	s.Accepted[v] = rec
	s.Since = now
	c.stats.Fresh++
	c.stats.FastVotes++
	c.replyVote(rec)
}
func (c *coldReplica) input(rec coldRecord) (request, bool) {
	req, ok := c.inputs[rec.Hash]
	req.Proposal.ClientId = rec.ID.Client
	req.Proposal.CommandId = rec.ID.Sequence
	req.Proposal.Timestamp = rec.Timestamp
	req.Origin = rec.Origin
	return req, ok
}
func (c *coldReplica) replyVote(rec coldRecord) {
	req, ok := c.input(rec)
	if !ok {
		return
	}
	b, _ := json.Marshal(rec.ColdVoteIdentity)
	c.replyTyped(req, coldVoteOK, b)
}
func (c *coldReplica) replyTyped(req request, ok uint8, value []byte) {
	reply := &defs.ProposeReplyTS{OK: ok, CommandId: req.Proposal.CommandId, Value: value, Timestamp: req.Proposal.Timestamp}
	for _, p := range c.r.learnerStreams[req.Proposal.ClientId] {
		if err := c.r.ReplyProposal(p, reply, 8192); err != nil {
			c.stats.ClientErrors++
		} else {
			c.stats.ClientFrames++
			c.stats.ClientBytes += uint64(len(value) + 17)
		}
	}
}
func (c *coldReplica) replyDone(req request, h [32]byte) {
	b, _ := json.Marshal(coldID{req.id(), h, req.Proposal.Command.K})
	c.stats.ColdDone++
	c.replyTyped(req, coldDoneOK, b)
}
func (c *coldReplica) validChoice(rec coldRecord) bool {
	if !c.isCold(rec.Key) || rec.Version == 0 || rec.Q == 0 || rec.Q>>uint(c.r.N) != 0 || bits.OnesCount64(rec.Q) != c.r.N/2+1 {
		return false
	}
	if rec.Fast {
		return rec.Voters&rec.Q == rec.Q
	}
	return bits.OnesCount64(rec.Voters) >= c.r.N/2+1 && rec.Voters&rec.Q == rec.Q
}
func (c *coldReplica) chosen(rec coldRecord, now time.Time) {
	if !c.validChoice(rec) {
		return
	}
	s := c.key(rec.Key)
	if old, ok := s.Choices[rec.Version]; ok && (old.ID != rec.ID || old.Hash != rec.Hash) {
		panic("cold: conflicting chosen version")
	}
	s.Choices[rec.Version] = rec
	if _, ok := c.inputs[rec.Hash]; !ok {
		c.needData(rec.Hash)
		return
	}
	c.applyKey(rec.Key, now)
}
func (c *coldReplica) applyKey(k state.Key, now time.Time) {
	s := c.key(k)
	for {
		rec, ok := s.Choices[s.Prefix+1]
		if !ok {
			for v := range s.Choices {
				if v > s.Prefix+1 {
					c.needProof(k, s.Prefix+1)
					break
				}
			}
			break
		}
		req, ok := c.input(rec)
		if !ok {
			c.needData(rec.Hash)
			break
		}
		if old, exists := c.done[rec.ID]; exists {
			if old != rec.Hash {
				panic("cold: chosen request identity changed")
			}
		} else {
			req.Proposal.Command.Execute(c.r.State)
			c.done[rec.ID] = rec.Hash
		}
		if !s.Since.IsZero() {
			if rec.Fast {
				c.stats.FastWindowSamples++
				c.stats.FastWindowNanos += uint64(now.Sub(s.Since))
			} else {
				c.stats.ClassicWindowSamples++
				c.stats.ClassicWindowNanos += uint64(now.Sub(s.Since))
			}
		}
		s.Prefix++
		s.Last = rec
		if rec.Round == s.Promise {
			s.Ready = true
		}
		s.Done[rec.ID] = rec.Hash
		delete(s.Pending, rec.ID)
		delete(c.replayPending, rec.ID)
		delete(c.held, rec.ID)
		if rec.Fast {
			c.stats.FastChoices++
		} else {
			c.stats.ClassicChoices++
		}
		c.replyDone(req, rec.Hash)
	}
	if p := c.keyPrep[k]; p != nil && p.Prepared {
		c.driveKey(k, now)
	}
	c.releaseReads(now)
}
func (c *coldReplica) keyValid(s *coldKey) bool {
	if !s.Ready {
		return false
	}
	for v, rec := range s.Accepted {
		if v > s.Prefix && rec.Round == s.Promise {
			return false
		}
	}
	return true
}
func (c *coldReplica) readAuthority(now time.Time) bool {
	e := c.r.engine
	return c.ready && !c.frozen && c.installed == c.promised &&
		c.installed.B == e.current.Ballot && c.q&bit(e.id) != 0 && e.localReadAuthority(now)
}

func (c *coldReplica) read(req request, now time.Time) {
	e := c.r.engine
	if c.readAuthority(now) && c.keyValid(c.key(req.Proposal.Command.K)) {
		value := req.Proposal.Command.Execute(c.r.State)
		c.stats.ColdReads++
		if req.Origin == e.id && e.observeLocalRead != nil {
			e.observeLocalRead(req.id())
		}
		if start, ok := c.readSince[req.id()]; ok {
			c.stats.HeldReadSamples++
			c.stats.HeldReadNanos += uint64(now.Sub(start))
			delete(c.readSince, req.id())
		}
		e.finish(req, value)
		delete(c.reads, req.id())
		return
	}
	if _, ok := c.reads[req.id()]; !ok {
		c.stats.HeldReads++
		c.readSince[req.id()] = now
	}
	c.reads[req.id()] = req
	if c.q != 0 && c.installed == c.promised && e.active() && c.q&bit(e.id) == 0 {
		target := c.nearestColdRole(e.id)
		if target >= 0 {
			c.stats.ForwardedReadAttempts++
			c.send(target, coldMessage{Type: "read", G: c.installed, Request: req})
		}
	}
}
func (c *coldReplica) nearestColdRole(_ int) int {
	for i := 0; i < c.r.N; i++ {
		if c.q&bit(i) != 0 && c.r.engine.peerHealthy(i, time.Now()) {
			return i
		}
	}
	return -1
}
func (c *coldReplica) releaseReads(now time.Time) {
	if !c.ready || c.frozen {
		return
	}
	for id, req := range c.reads {
		if c.readAuthority(now) && c.keyValid(c.key(req.Proposal.Command.K)) {
			value := req.Proposal.Command.Execute(c.r.State)
			c.stats.ColdReads++
			if req.Origin == c.r.engine.id && c.r.engine.observeLocalRead != nil {
				c.r.engine.observeLocalRead(req.id())
			}
			if start, ok := c.readSince[req.id()]; ok {
				c.stats.HeldReadSamples++
				c.stats.HeldReadNanos += uint64(now.Sub(start))
				delete(c.readSince, req.id())
			}
			c.r.engine.finish(req, value)
			delete(c.reads, id)
		}
	}
}
func (c *coldReplica) tick(now time.Time) {
	if !c.enabled {
		return
	}
	e := c.r.engine
	if c.migrated || c.hasBridge() {
		c.tickBridge(now)
		return
	}
	if c.installed.B != e.current.Ballot || !e.active() {
		c.ready = false
	}
	if e.id == e.current.Leader && e.active() && e.prepared && e.stable(now) && (c.prep == nil || c.prep.G.B != e.current.Ballot) {
		c.startGlobal(coldGeneration{B: e.current.Ballot}, c.installed.B != 0 && c.installed.B != e.current.Ballot && c.scan == nil, now)
	}
	c.driveGlobal(now)
	c.drivePendingManifests(now)
	c.driveScan(now)
	c.proposeBridge(now)
	for key, p := range c.keyPrep {
		if now.Sub(p.Last) >= 300*time.Millisecond {
			c.retryKey(key, now)
		}
	}
	if now.Sub(c.lastRetry) >= 400*time.Millisecond {
		c.lastRetry = now
		c.releaseReads(now)
		for _, req := range c.reads {
			if c.q&bit(e.id) == 0 {
				c.read(req, now)
			}
		}
		// Pause replay while fencing/catching up; retain requests for fair bounded replay.
		if e.active() && c.installed == c.promised && c.installed.B == e.current.Ballot && !c.frozen {
			budget := 256
			limit := len(c.replayQueue) - c.replayHead
			for limit > 0 && budget > 0 {
				id := c.replayQueue[c.replayHead]
				c.replayHead++
				limit--
				req, exists := c.replayPending[id]
				if !exists {
					continue
				}
				if _, done := c.done[id]; done {
					delete(c.replayPending, id)
					continue
				}
				c.replayQueue = append(c.replayQueue, id)
				budget--
				s := c.key(req.Proposal.Command.K)
				if e.id == e.current.Leader && c.coordinatorReady() {
					if s.Fast && now.Sub(s.Since) <= 800*time.Millisecond && c.q&bit(e.id) == 0 {
						for i := 0; i < e.n; i++ {
							if c.q&bit(i) != 0 {
								c.send(i, coldMessage{Type: "intent", G: c.installed, Request: req})
							}
						}
					} else if !s.Fast || now.Sub(s.Since) > 800*time.Millisecond {
						c.ensureKeyPrepare(req.Proposal.Command.K, false, now)
					}
				} else {
					c.send(e.current.Leader, coldMessage{Type: "intent", G: c.installed, Request: req})
				}
			}
			if c.replayHead > 4096 && c.replayHead*2 > len(c.replayQueue) {
				c.replayQueue = append([]defs.RequestID(nil), c.replayQueue[c.replayHead:]...)
				c.replayHead = 0
			}
		}

		for _, s := range c.keys {
			for v, rec := range s.Choices {
				if v > s.Prefix {
					if _, ok := s.Choices[s.Prefix+1]; !ok {
						c.needProof(rec.Key, s.Prefix+1)
					}
					if _, ok := c.inputs[rec.Hash]; !ok {
						c.needData(rec.Hash)
					}
				}
			}
		}
		for _, req := range c.scanRefs {
			c.fetchScan(req)
		}
	}
	c.flushOutgoing()
}
func (c *coldReplica) receive(from int, data []byte, now time.Time) {
	if !c.enabled {
		return
	}
	var m coldMessage
	if json.Unmarshal(data, &m) != nil {
		return
	}
	if from < 0 || from >= c.r.N {
		return
	}
	// Actual decoded peer traffic is failure-detector evidence, never a lease grant.
	c.r.engine.refreshPeer(from, now)
	c.stats.PeerHealthRefreshes++
	if (c.migrated || c.hasBridge()) && m.Type != "payload_need" && m.Type != "payload_bodies" && m.Type != "data" && m.Type != "need" {
		if m.Type == "intent" && m.Request.Proposal.Command.Op == state.PUT {
			c.intent(m.Request, m.G, now)
		} else if m.Type == "read" && c.migrated {
			c.r.engine.submit(m.Request, now)
		} else {
			c.stats.BridgeIgnored++
		}
		return
	}
	switch m.Type {
	case "intent":
		if m.Request.Proposal.Command.Op == state.PUT && c.isCold(m.Request.Proposal.Command.K) {
			c.intent(m.Request, m.G, now)
		}
	case "collision":
		c.retain(m.Request)
		c.key(m.Key).Pending[m.Request.id()] = m.Request
		if c.r.engine.id == c.r.engine.current.Leader && c.ready && !c.frozen {
			c.ensureKeyPrepare(m.Key, false, now)
		}
	case "chosen":
		if m.Request.Proposal.Command.Op == state.PUT && commandDigest(m.Request.Proposal.Command) == m.Record.Hash {
			c.retain(m.Request)
		}
		c.chosen(m.Record, now)
	case "read":
		if m.Request.Proposal.Command.Op == state.GET && c.isCold(m.Request.Proposal.Command.K) {
			c.read(m.Request, now)
		}
	case "prepare", "promise", "bulk_accept", "bulk_vote", "bulk_commit", "bulk_applied", "enable", "ready":
		c.receiveGlobal(from, m, now)
	case "key_prepare", "key_promise", "classic", "accepted", "key_enable", "key_ready":
		c.receiveKey(from, m, now)
	case "payload_need", "payload_bodies":
		c.receiveCatalogPayloads(from, m, now)
	case "need", "data", "proof_need":
		c.receiveData(from, m, now)
	case "scan_data", "scan_held", "scan_need":
		c.receiveScanData(from, m, now)
	}
}

func (c *coldReplica) coordinatorReady() bool {
	return c.prep != nil && c.prep.G == c.installed && c.prep.Enabled
}
