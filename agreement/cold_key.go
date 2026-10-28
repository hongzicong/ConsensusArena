package agreement

import (
	"crypto/sha256"
	"encoding/json"
	"github.com/hongzicong/ConsensusArena/replica/defs"
	"github.com/hongzicong/ConsensusArena/state"
	"math/bits"
	"sort"
	"time"
)

func (c *coldReplica) keySnapshot(k state.Key) coldKeySnapshot {
	s := c.key(k)
	out := coldKeySnapshot{Key: k, Prefix: s.Prefix, Last: s.Last, Promise: s.Promise}
	for v, rec := range s.Accepted {
		if v > s.Prefix {
			out.Accepted = append(out.Accepted, rec)
		}
	}
	for id, h := range s.Done {
		out.Done = append(out.Done, coldID{id, h, k})
	}
	return out
}

// The actual phase-one set intersects the ONE old fast quorum. A possible
// old fast choice must be reported by every member of that intersection.
func safeColdTail(snaps map[int]coldKeySnapshot, prefix uint64) (coldRecord, bool) {
	var round coldRound
	found := false
	for _, s := range snaps {
		for _, rec := range s.Accepted {
			if rec.Version == prefix+1 && (!found || round.less(rec.Round)) {
				round = rec.Round
				found = true
			}
		}
	}
	if !found {
		return coldRecord{}, false
	}
	candidates := map[ColdVoteIdentity]coldRecord{}
	for _, s := range snaps {
		for _, rec := range s.Accepted {
			if rec.Version == prefix+1 && rec.Round == round {
				candidates[rec.ColdVoteIdentity] = rec
			}
		}
	}
	for _, rec := range candidates {
		if !rec.Fast {
			return rec, true
		}
		possible, intersects := true, false
		for from, s := range snaps {
			if rec.Q&bit(from) == 0 {
				continue
			}
			intersects = true
			match := false
			for _, r := range s.Accepted {
				if r.Version == prefix+1 && r.Round == round && r.ID == rec.ID && r.Hash == rec.Hash {
					match = true
					break
				}
			}
			if !match {
				possible = false
				break
			}
		}
		if possible && intersects {
			return rec, true
		}
	}
	// No previous fast choice is possible. Select a retained original input,
	// deterministically; this does not invent a value or discard losing requests.
	list := make([]coldRecord, 0, len(candidates))
	for _, r := range candidates {
		list = append(list, r)
	}
	sort.Slice(list, func(i, j int) bool {
		a, b := list[i].ID, list[j].ID
		return a.Client < b.Client || a.Client == b.Client && a.Sequence < b.Sequence
	})
	return list[0], true
}
func (c *coldReplica) installKeySnapshot(snap coldKeySnapshot) bool {
	s := c.key(snap.Key)
	for _, d := range snap.Done {
		if old, ok := c.done[d.ID]; ok && old != d.Hash {
			panic("cold: checkpoint dedup conflict")
		}
		c.done[d.ID] = d.Hash
		s.Done[d.ID] = d.Hash
		delete(s.Pending, d.ID)
	}
	if snap.Prefix <= s.Prefix {
		return true
	}
	rec := snap.Last
	if rec.Version != snap.Prefix || !c.validChoice(rec) {
		return false
	}
	req, ok := c.input(rec)
	if !ok {
		c.needData(rec.Hash)
		return false
	}
	req.Proposal.Command.Execute(c.r.State)
	s.Prefix = snap.Prefix
	s.Last = rec
	s.Choices[rec.Version] = rec
	c.done[rec.ID] = rec.Hash
	s.Done[rec.ID] = rec.Hash
	return true
}
func (c *coldReplica) ensureKeyPrepare(k state.Key, resume bool, now time.Time) {
	if !resume && !c.coordinatorReady() {
		return
	}
	if c.keyPrep[k] != nil {
		return
	}
	s := c.key(k)
	round := coldRound{G: c.promised, K: s.Promise.K + 1}
	if s.Promise.G != round.G {
		round.K = 1
	}
	p := &coldKeyPrepare{Since: now, Round: round, Promises: map[int]coldKeySnapshot{}, Sources: map[int]*coldParts{}, Votes: map[uint64]uint64{}, Resume: resume, Last: now}
	c.keyPrep[k] = p
	c.stats.KeyPrepares++
	if resume {
		c.stats.ResumePrepares++
	}
	c.retryKey(k, now)
}
func (c *coldReplica) retryKey(k state.Key, now time.Time) {
	p := c.keyPrep[k]
	if p == nil {
		return
	}
	p.Last = now
	if !p.Prepared {
		c.broadcast(coldMessage{Type: "key_prepare", Round: p.Round, G: p.Round.G, Key: k, Q: c.q})
		return
	}
	if p.Resume && len(p.Selected) == 0 {
		snap := c.keySnapshot(k)
		c.broadcast(coldMessage{Type: "key_enable", Round: p.Round, G: p.Round.G, Key: k, Q: c.q, Keys: []coldKeySnapshot{snap}})
		return
	}
	c.driveKey(k, now)
}
func (c *coldReplica) prepareKeyChoices(k state.Key, now time.Time) {
	p := c.keyPrep[k]
	if p == nil || p.Prepared || len(p.Promises) < c.r.N/2+1 {
		return
	}
	var latest coldKeySnapshot
	latest.Key = k
	for _, s := range p.Promises {
		if s.Prefix > latest.Prefix {
			latest = s
		}
	}
	if !c.installKeySnapshot(latest) {
		return
	}
	s := c.key(k)
	tail, has := safeColdTail(p.Promises, s.Prefix)
	if has {
		tail.Round = p.Round
		tail.Q = c.q
		tail.Fast = false
		tail.Voters = 0
		p.Selected = append(p.Selected, tail)
	}
	p.Prepared = true
	if p.Resume && has {
		p.Resume = false
	}
	c.driveKey(k, now)
}
func (c *coldReplica) driveKey(k state.Key, now time.Time) {
	p := c.keyPrep[k]
	if p == nil || !p.Prepared {
		return
	}
	s := c.key(k)
	// Retire already chosen work before proposing a next version.
	for len(p.Selected) > 0 && p.Selected[0].Version <= s.Prefix {
		p.Selected = p.Selected[1:]
	}
	if len(p.Selected) == 0 {
		if p.Resume {
			snap := c.keySnapshot(k)
			for i := 0; i < c.r.N; i++ {
				if c.q&bit(i) != 0 {
					c.send(i, coldMessage{Type: "key_enable", Round: p.Round, G: p.Round.G, Key: k, Q: c.q, Keys: []coldKeySnapshot{snap}})
				}
			}
			return
		}
		ids := make([]defs.RequestID, 0, len(s.Pending))
		if c.ready && !c.frozen {
			for id := range s.Pending {
				if _, ok := c.done[id]; !ok {
					ids = append(ids, id)
				}
			}
		}
		sort.Slice(ids, func(i, j int) bool {
			return ids[i].Client < ids[j].Client || ids[i].Client == ids[j].Client && ids[i].Sequence < ids[j].Sequence
		})
		if len(ids) == 0 {
			delete(c.keyPrep, k)
			if c.prep == nil || c.prep.Merged {
				c.ensureKeyPrepare(k, true, now)
			}
			return
		}
		req := s.Pending[ids[0]]
		h := c.retain(req)
		p.Selected = []coldRecord{{ColdVoteIdentity: ColdVoteIdentity{Round: p.Round, Key: k, Version: s.Prefix + 1, Q: c.q, ID: req.id(), Origin: req.Origin, Timestamp: req.Proposal.Timestamp, Hash: h}}}
	}
	rec := p.Selected[0]
	req, ok := c.input(rec)
	if !ok {
		c.needData(rec.Hash)
		return
	}
	for i := 0; i < c.r.N; i++ {
		c.send(i, coldMessage{Type: "classic", Round: p.Round, G: p.Round.G, Key: k, Q: c.q, Record: rec, Request: req, Keys: []coldKeySnapshot{c.keySnapshot(k)}})
	}
}
func (c *coldReplica) receiveKey(from int, m coldMessage, now time.Time) {
	e := c.r.engine
	if m.Round.G != c.promised || m.Round.G.B != e.current.Ballot || from < 0 || from >= e.n {
		return
	}
	switch m.Type {
	case "key_prepare":
		if from != e.current.Leader || bits.OnesCount64(m.Q) != e.majority || m.Q>>uint(e.n) != 0 || (c.installed == c.promised && m.Q != c.q) {
			return
		}
		s := c.key(m.Key)
		if m.Round.less(s.Promise) {
			return
		}
		s.Promise = m.Round
		s.Q = m.Q
		s.Fast = false
		s.Ready = false
		snap := c.keySnapshot(m.Key)
		data, _ := json.Marshal(snap)
		hash := sha256.Sum256(data)
		n := (len(data) + coldPartBytes - 1) / coldPartBytes
		if n == 0 {
			n = 1
		}
		for i := 0; i < n; i++ {
			a, b := i*coldPartBytes, min((i+1)*coldPartBytes, len(data))
			c.send(from, coldMessage{Type: "key_promise", Round: m.Round, G: m.Round.G, Key: m.Key, Part: i, Parts: n, Hash: hash, Data: data[a:b]})
		}
	case "key_promise":
		p := c.keyPrep[m.Key]
		if p == nil || p.Round != m.Round || p.Prepared {
			return
		}
		part := p.Sources[from]
		if part == nil {
			part = &coldParts{}
			p.Sources[from] = part
		}
		data, ok := addColdPart(part, m)
		if !ok {
			return
		}
		var snap coldKeySnapshot
		if json.Unmarshal(data, &snap) != nil || snap.Key != m.Key {
			return
		}
		p.Promises[from] = snap
		c.prepareKeyChoices(m.Key, now)
	case "classic":
		if from != e.current.Leader || m.Record.Fast || m.Record.Round != m.Round || m.Record.Q != m.Q || bits.OnesCount64(m.Q) != e.majority {
			return
		}
		s := c.key(m.Key)
		if m.Round != s.Promise {
			return
		}
		if len(m.Keys) != 1 || !c.installKeySnapshot(m.Keys[0]) {
			return
		}
		rec := m.Record
		if rec.Version <= s.Prefix {
			c.send(from, coldMessage{Type: "chosen", G: m.G, Record: s.Last})
			return
		}
		if rec.Version != s.Prefix+1 || m.Request.id() != rec.ID || commandDigest(m.Request.Proposal.Command) != rec.Hash {
			return
		}
		if old, ok := s.Accepted[rec.Version]; ok && old.Round == rec.Round && (old.ID != rec.ID || old.Hash != rec.Hash) {
			panic("cold: same-round classic conflict")
		}
		c.retain(m.Request)
		s.Accepted[rec.Version] = rec
		s.Since = now
		s.Fast = false
		s.Ready = false
		s.Q = m.Q
		c.send(from, coldMessage{Type: "accepted", Round: m.Round, G: m.G, Key: m.Key, Record: rec})
	case "accepted":
		p := c.keyPrep[m.Key]
		if p == nil || p.Round != m.Round || len(p.Selected) == 0 || p.Selected[0].ColdVoteIdentity != m.Record.ColdVoteIdentity {
			return
		}
		rec := p.Selected[0]
		p.Votes[rec.Version] |= bit(from)
		mask := p.Votes[rec.Version]
		if bits.OnesCount64(mask) < e.majority || mask&rec.Q != rec.Q {
			return
		}
		rec.Voters = mask
		req, _ := c.input(rec)
		// Install locally before dissemination; remove selected before callbacks.
		p.Selected = p.Selected[1:]
		c.chosen(rec, now)
		c.broadcast(coldMessage{Type: "chosen", G: m.G, Record: rec, Request: req})
	case "key_enable":
		if from != e.current.Leader || bits.OnesCount64(m.Q) != e.majority || len(m.Keys) != 1 {
			return
		}
		s := c.key(m.Key)
		if m.Round != s.Promise || m.Q != s.Q || !c.installKeySnapshot(m.Keys[0]) {
			return
		}
		s.Fast = true
		s.Ready = true
		s.Q = m.Q
		c.send(from, coldMessage{Type: "key_ready", Round: m.Round, G: m.G, Key: m.Key, Q: m.Q})
	case "key_ready":
		p := c.keyPrep[m.Key]
		if p == nil || !p.Resume || p.Round != m.Round || m.Q != c.q {
			return
		}
		p.Ready |= bit(from)
		if p.Ready&c.q == c.q {
			if !p.Since.IsZero() {
				c.stats.KeyRecoverySamples++
				c.stats.KeyRecoveryNanos += uint64(now.Sub(p.Since))
			}
			delete(c.keyPrep, m.Key)
			s := c.key(m.Key)
			for _, req := range s.Pending {
				for i := 0; i < e.n; i++ {
					if c.q&bit(i) != 0 {
						c.send(i, coldMessage{Type: "intent", G: c.installed, Request: req})
					}
				}
			}
		}
	}
}
