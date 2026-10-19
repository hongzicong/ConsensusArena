package xpaxos

import (
	"crypto/sha256"
	"math/bits"
	"sort"
	"time"

	"github.com/hongzicong/ConsensusArena/state"
)

type coldBundle struct {
	G      coldGeneration
	Q      uint64
	Frozen bool
	Keys   []coldKeySnapshot
	Tails  []coldRecord
	Inputs []request
}

func (c *coldReplica) snapshot() coldSnapshot {
	s := coldSnapshot{}
	hashes := map[[32]byte]bool{}
	for k, key := range c.keys {
		if key.Prefix == 0 && len(key.Accepted) == 0 && len(key.Done) == 0 {
			continue
		}
		snap := c.keySnapshot(k)
		s.Keys = append(s.Keys, snap)
		if snap.Prefix > 0 {
			hashes[snap.Last.Hash] = true
		}
		for _, rec := range snap.Accepted {
			hashes[rec.Hash] = true
		}
	}
	for h := range hashes {
		if req, ok := c.inputs[h]; ok {
			s.Inputs = append(s.Inputs, req)
		}
	}
	return s
}

func (c *coldReplica) startGlobal(g coldGeneration, frozen bool, now time.Time) {
	if c.prep != nil && c.prep.G == g {
		return
	}
	c.prep = &coldPrepareState{PayloadHolders: map[[32]byte]uint64{}, G: g, Frozen: frozen, Sources: map[int]*coldParts{}, Snapshots: map[int]coldSnapshot{}, Tails: map[state.Key]coldRecord{}, Recovered: map[state.Key]coldKeySnapshot{}, Since: now, Ready: map[int]bool{}}
	c.keyPrep = map[state.Key]*coldKeyPrepare{}
	c.ready = false
	c.stats.GlobalPrepares++
	c.broadcast(coldMessage{Type: "prepare", G: g, Frozen: frozen})
}

func (c *coldReplica) retainBodies(inputs []request) bool {
	for _, req := range inputs {
		if req.Proposal.Command.Op != state.PUT || !c.isCold(req.Proposal.Command.K) || req.Origin < 0 || req.Origin >= c.r.N {
			return false
		}
		c.retain(req)
	}
	return true
}

func (c *coldReplica) snapshotComplete(s coldSnapshot) bool {
	for _, key := range s.Keys {
		if key.Prefix > 0 {
			if key.Last.Key != key.Key || key.Last.Version != key.Prefix || !c.validChoice(key.Last) {
				return false
			}
			if _, ok := c.inputs[key.Last.Hash]; !ok {
				return false
			}
		}
		for _, rec := range key.Accepted {
			if rec.Key != key.Key {
				return false
			}
			if _, ok := c.inputs[rec.Hash]; !ok {
				return false
			}
		}
	}
	return true
}

func (c *coldReplica) receiveGlobal(from int, m coldMessage, now time.Time) {
	e := c.r.engine
	if from < 0 || from >= e.n || m.G.B != e.current.Ballot || !e.active() {
		return
	}
	switch m.Type {
	case "prepare":
		if from != e.current.Leader || m.G.less(c.promised) {
			return
		}
		if c.promised.less(m.G) {
			c.promised = m.G
			c.ready = false
			c.frozen = m.Frozen
			c.keyPrep = map[state.Key]*coldKeyPrepare{}
			for _, s := range c.keys {
				s.Promise = coldRound{G: m.G}
				s.Fast = false
				s.Ready = false
			}
		}
		data, ok := c.snapshots[m.G]
		if !ok {
			start := time.Now()
			snap := c.snapshot()
			if !c.snapshotComplete(snap) {
				c.stats.BulkRejected++
				return
			}
			c.stats.SourceReferencedBodies += uint64(len(snap.Inputs))
			snap.Inputs = nil
			var err error
			data, err = c.encodeRecovery(snap)
			if err != nil {
				c.stats.BulkRejected++
				return
			}
			c.stats.CatalogSourceBytes += uint64(len(data))
			c.snapshots[m.G] = data
			c.stats.BulkEncodeNanos += uint64(time.Since(start))
			c.stats.BulkSourceBytes += uint64(len(data))
		}
		c.sendParts(from, "promise", m.G, sha256.Sum256(data), data, m.Frozen, 0)
	case "promise":
		p := c.prep
		if e.id != e.current.Leader || p == nil || p.G != m.G || p.Merged {
			return
		}
		if _, ok := p.Snapshots[from]; ok {
			return
		}
		part := p.Sources[from]
		if part == nil {
			part = &coldParts{}
			p.Sources[from] = part
		}
		data, ok := addColdPart(part, m)
		c.stats.SnapshotParts++
		if !ok {
			return
		}
		start := time.Now()
		var s coldSnapshot
		err := c.decodeRecovery(data, &s)
		c.stats.BulkDecodeNanos += uint64(time.Since(start))
		if err != nil || len(s.Inputs) != 0 || !c.snapshotMetadataComplete(s) {
			c.stats.BulkRejected++
			return
		}
		p.Snapshots[from] = s
		c.rememberPayloadHolders(p, from, s)
		c.mergeGlobal(now)
	case "bulk_accept":
		c.receiveBulkAccept(from, m, now)
	case "bulk_vote":
		p := c.prep
		if p == nil || !p.Merged || p.G != m.G || p.Q != m.Q || p.BundleHash != m.Hash {
			return
		}
		p.BulkVotes |= bit(from)
		if p.ChoiceMask == 0 && bits.OnesCount64(p.BulkVotes) >= e.majority && p.BulkVotes&p.Q == p.Q {
			p.ChoiceMask = p.BulkVotes
			c.stats.BulkVotes += uint64(bits.OnesCount64(p.ChoiceMask))
			p.Last = time.Time{}
			c.driveGlobal(now)
		}
	case "bulk_commit":
		c.receiveBulkCommit(from, m, now)
	case "bulk_applied":
		p := c.prep
		if p == nil || p.ChoiceMask == 0 || p.G != m.G || p.Q != m.Q || p.BundleHash != m.Hash {
			return
		}
		p.BulkApplied |= bit(from)
		if p.BulkApplied&p.Q == p.Q && p.BulkApplied&bit(e.id) != 0 {
			c.sendGlobalEnable(p)
		}
	case "enable":
		if from != e.current.Leader || m.G != c.installed || m.G != c.promised || m.Q != c.q || m.Q&bit(e.id) == 0 || !c.bulkApplied[m.Hash] {
			return
		}
		if !c.ready {
			for _, s := range c.keys {
				s.Ready = true
				s.Fast = !m.Frozen
				s.Q = m.Q
			}
			c.ready = true
			c.frozen = m.Frozen
			c.stats.ReadyGenerations++
		}
		c.send(from, coldMessage{Type: "ready", G: m.G, Q: m.Q, Hash: m.Hash, Frozen: m.Frozen})
	case "ready":
		p := c.prep
		if p == nil || p.G != m.G || p.Q != m.Q || p.BundleHash != m.Hash || p.BulkApplied&p.Q != p.Q || p.BulkApplied&bit(e.id) == 0 {
			return
		}
		p.Ready[from] = true
		mask := uint64(0)
		for id := range p.Ready {
			mask |= bit(id)
		}
		if mask&p.Q == p.Q {
			if !p.Enabled {
				p.Enabled = true
				c.stats.GlobalRecoverySamples++
				c.stats.GlobalRecoveryNanos += uint64(now.Sub(p.Since))
				c.ready = true
				c.frozen = p.Frozen
				c.installed = p.G
				c.q = p.Q
				for _, s := range c.keys {
					s.Ready = true
					s.Fast = !p.Frozen
					s.Q = p.Q
				}
			}
			c.releaseReads(now)
			c.proposeBridge(now)
		}
	}
}

func (c *coldReplica) mergeGlobal(now time.Time) {
	p := c.prep
	if p == nil || p.Merged || len(p.Snapshots) < c.r.N/2+1 {
		return
	}
	participants := uint64(0)
	for id := range p.Snapshots {
		participants |= bit(id)
	}
	q := uint64(0)
	if p.G.B == c.installed.B && c.q != 0 {
		q = c.q
	} else if c.installed.B == 0 && participants&c.plannedQ == c.plannedQ {
		q = c.plannedQ
	} else if c.installed.B == 0 && now.Sub(p.Since) < time.Second {
		return
	}
	if q == 0 {
		ids := make([]int, 0, len(p.Snapshots))
		for id := range p.Snapshots {
			ids = append(ids, id)
		}
		sort.Ints(ids)
		for _, id := range ids {
			q |= bit(id)
			if bits.OnesCount64(q) == c.r.N/2+1 {
				break
			}
		}
	}
	p.Q = q
	if c.q != 0 && c.q != q {
		c.stats.QChanges++
	}
	c.q = q
	groups := map[state.Key]map[int]coldKeySnapshot{}
	for id, snap := range p.Snapshots {
		for _, key := range snap.Keys {
			if groups[key.Key] == nil {
				groups[key.Key] = map[int]coldKeySnapshot{}
			}
			groups[key.Key][id] = key
		}
	}
	for key, group := range groups {
		for id := range p.Snapshots {
			if _, ok := group[id]; !ok {
				group[id] = coldKeySnapshot{Key: key}
			}
		}
		latest := coldKeySnapshot{Key: key}
		for _, snap := range group {
			if snap.Prefix > latest.Prefix {
				latest = snap
			}
		}
		p.Recovered[key] = latest
		if tail, ok := safeColdTail(group, latest.Prefix); ok {
			tail.Round = coldRound{G: p.G}
			tail.Q = p.Q
			tail.Fast = false
			tail.Voters = 0
			p.Tails[key] = tail
		}
	}
	p.Merged = true
	c.driveGlobal(now)
}

func (c *coldReplica) buildBundle(p *coldPrepareState) bool {
	if p.CatalogDraft == nil {
		b := coldBundle{G: p.G, Q: p.Q, Frozen: p.Frozen}
		hashes := map[[32]byte]bool{}
		keys := make([]state.Key, 0, len(p.Recovered))
		for key := range p.Recovered {
			keys = append(keys, key)
		}
		sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
		for _, key := range keys {
			s := p.Recovered[key]
			s.Accepted = nil
			b.Keys = append(b.Keys, s)
			if s.Prefix > 0 {
				hashes[s.Last.Hash] = true
			}
			if tail, ok := p.Tails[key]; ok {
				b.Tails = append(b.Tails, tail)
				hashes[tail.Hash] = true
			}
		}
		p.CatalogMissing = map[[32]byte]bool{}
		for h := range hashes {
			if _, ok := c.inputs[h]; !ok {
				p.CatalogMissing[h] = true
			}
		}
		p.CatalogDraft = &b
	}
	now := time.Now()
	if len(p.CatalogMissing) > 0 && now.Sub(p.CatalogLast) < 100*time.Millisecond {
		return false
	}
	p.CatalogLast = now
	missing := make([][32]byte, 0, len(p.CatalogMissing))
	for h := range p.CatalogMissing {
		if _, ok := c.inputs[h]; ok {
			delete(p.CatalogMissing, h)
		} else {
			missing = append(missing, h)
		}
	}
	if len(missing) > 0 {
		c.requestCatalogBodies(p.G, missing, p.PayloadHolders, now)
		return false
	}
	b := *p.CatalogDraft
	// All bodies are retained locally, but their hashes already bind manifest records.
	b.Inputs = nil
	start := time.Now()
	data, err := c.encodeRecovery(b)
	c.stats.BulkEncodeNanos += uint64(time.Since(start))
	if err != nil {
		return false
	}
	p.Bundle = data
	p.BundleHash = sha256.Sum256(data)
	p.Last = time.Time{}
	c.stats.BulkBodyBytes += uint64(len(data))
	c.stats.CatalogBundleBytes += uint64(len(data))
	c.stats.BulkTailKeys += uint64(len(b.Tails))
	return true
}

func (c *coldReplica) driveGlobal(now time.Time) {
	if c.migrated || c.hasBridge() {
		return
	}
	p := c.prep
	e := c.r.engine
	if p == nil || p.G.B != e.current.Ballot || e.id != e.current.Leader {
		return
	}
	if !p.Merged {
		c.mergeGlobal(now)
		if now.Sub(p.Last) >= 400*time.Millisecond {
			p.Last = now
			c.broadcast(coldMessage{Type: "prepare", G: p.G, Frozen: p.Frozen})
		}
		return
	}
	if len(p.Bundle) == 0 && !c.buildBundle(p) {
		return
	}
	if now.Sub(p.Last) < 400*time.Millisecond {
		return
	}
	p.Last = now
	if p.ChoiceMask == 0 {
		for i := 0; i < e.n; i++ {
			if p.BulkVotes&bit(i) == 0 {
				c.sendParts(i, "bulk_accept", p.G, p.BundleHash, p.Bundle, p.Frozen, p.Q)
			}
		}
		return
	}
	for i := 0; i < e.n; i++ {
		if p.BulkApplied&bit(i) == 0 {
			c.send(i, coldMessage{Type: "bulk_commit", G: p.G, Q: p.Q, Hash: p.BundleHash, Record: coldRecord{Voters: p.ChoiceMask}, Frozen: p.Frozen})
		}
	}
	if p.BulkApplied&p.Q == p.Q && p.BulkApplied&bit(e.id) != 0 {
		c.sendGlobalEnable(p)
	}
}

func (c *coldReplica) sendGlobalEnable(p *coldPrepareState) {
	for i := 0; i < c.r.N; i++ {
		if p.Q&bit(i) != 0 {
			c.send(i, coldMessage{Type: "enable", G: p.G, Q: p.Q, Hash: p.BundleHash, Frozen: p.Frozen})
		}
	}
}

func (c *coldReplica) receiveBulkAccept(from int, m coldMessage, now time.Time) {
	e := c.r.engine
	if from != e.current.Leader || m.G != c.promised || bits.OnesCount64(m.Q) != e.majority || m.Q>>uint(e.n) != 0 {
		return
	}
	if hash, ok := c.bulkAccepted[m.G]; ok {
		if hash != m.Hash {
			c.stats.BulkRejected++
			return
		}
		c.stats.BulkDuplicates++
		c.send(from, coldMessage{Type: "bulk_vote", G: m.G, Q: m.Q, Hash: m.Hash})
		return
	}
	if pending := c.manifestPending[m.Hash]; pending != nil {
		c.stats.ManifestPendingDuplicates++
		c.tryPendingManifest(m.Hash, now)
		return
	}
	if hash, ok := c.manifestGeneration[m.G]; ok && hash != m.Hash {
		c.stats.BulkRejected++
		return
	}
	parts := c.chunks[m.Hash]
	if parts == nil {
		parts = &coldParts{}
		c.chunks[m.Hash] = parts
	}
	data, ok := addColdPart(parts, m)
	if !ok {
		return
	}
	start := time.Now()
	var b coldBundle
	err := c.decodeRecovery(data, &b)
	c.stats.BulkDecodeNanos += uint64(time.Since(start))
	c.stats.ManifestDecodeSamples++
	if err != nil || b.G != m.G || b.Q != m.Q || b.Frozen != m.Frozen || len(b.Inputs) != 0 {
		c.stats.BulkRejected++
		return
	}
	refs, valid := c.manifestReferences(b)
	if !valid {
		c.stats.BulkRejected++
		return
	}
	missing := map[[32]byte]bool{}
	for h, key := range refs {
		req, ok := c.inputs[h]
		c.stats.ManifestReferences++
		if ok && req.Proposal.Command.Op == state.PUT && req.Proposal.Command.K == key && commandDigest(req.Proposal.Command) == h {
			c.stats.ManifestCacheHits++
		} else {
			missing[h] = true
			c.stats.ManifestCacheMisses++
		}
	}
	c.manifestGeneration[m.G] = m.Hash
	c.manifestPending[m.Hash] = &coldPendingManifest{From: from, Message: m, Bundle: b, Missing: missing, Since: now}
	c.tryPendingManifest(m.Hash, now)
}

func (c *coldReplica) acceptCompleteManifest(from int, m coldMessage, b coldBundle, now time.Time) {
	e := c.r.engine
	if from != e.current.Leader || m.G != c.promised || b.G != m.G || b.Q != m.Q {
		return
	}
	refs, ok := c.manifestReferences(b)
	if !ok {
		c.stats.BulkRejected++
		return
	}
	for h, key := range refs {
		req, retained := c.inputs[h]
		if !retained || req.Proposal.Command.Op != state.PUT || req.Proposal.Command.K != key || commandDigest(req.Proposal.Command) != h {
			c.stats.BulkRejected++
			return
		}
	}
	for _, snap := range b.Keys {
		if !c.isRecoveryKey(snap.Key) || len(snap.Accepted) > 0 || !c.installKeySnapshot(snap) {
			c.stats.BulkRejected++
			return
		}
	}
	seen := map[state.Key]bool{}
	for _, rec := range b.Tails {
		s := c.key(rec.Key)
		if seen[rec.Key] || rec.Fast || rec.Voters != 0 || rec.Round != (coldRound{G: m.G}) || rec.Q != m.Q || s.Promise != rec.Round || rec.Version != s.Prefix+1 {
			// A late original proof may already have chosen this exact tail.
			if !seen[rec.Key] && !rec.Fast && rec.Round == (coldRound{G: m.G}) && rec.Q == m.Q && rec.Voters == 0 && rec.Version <= s.Prefix && s.Done[rec.ID] == rec.Hash {
				seen[rec.Key] = true
				continue
			}
			c.stats.BulkRejected++
			return
		}
		if _, ok := c.inputs[rec.Hash]; !ok {
			c.stats.BulkRejected++
			return
		}
		if old, ok := s.Accepted[rec.Version]; ok && old.Round == rec.Round && (old.ID != rec.ID || old.Hash != rec.Hash) {
			c.stats.BulkRejected++
			return
		}
		seen[rec.Key] = true
		s.Accepted[rec.Version] = rec
		s.Fast = false
		s.Ready = false
		s.Q = m.Q
	}
	c.bulkBodies[m.Hash] = b
	c.bulkAccepted[m.G] = m.Hash
	c.stats.BulkAccepts++
	c.send(from, coldMessage{Type: "bulk_vote", G: m.G, Q: m.Q, Hash: m.Hash})
}

func (c *coldReplica) receiveBulkCommit(from int, m coldMessage, now time.Time) {
	e := c.r.engine
	if from != e.current.Leader || m.G != c.promised || m.Q>>uint(e.n) != 0 || bits.OnesCount64(m.Q) != e.majority || bits.OnesCount64(m.Record.Voters) < e.majority || m.Record.Voters&m.Q != m.Q || m.Record.Voters>>uint(e.n) != 0 {
		return
	}
	b, ok := c.bulkBodies[m.Hash]
	if !ok || b.G != m.G || b.Q != m.Q || c.bulkAccepted[m.G] != m.Hash {
		return
	}
	if !c.bulkApplied[m.Hash] {
		for _, rec := range b.Tails {
			rec.Voters = m.Record.Voters
			c.chosen(rec, now)
			s := c.key(rec.Key)
			if s.Prefix < rec.Version || s.Done[rec.ID] != rec.Hash {
				c.stats.BulkRejected++
				return
			}
		}
		if c.installed.less(m.G) {
			c.installed = m.G
			c.q = m.Q
			c.ready = false
			c.frozen = m.Frozen
			c.stats.CheckpointInstalls++
			for _, s := range c.keys {
				s.Q = m.Q
				s.Fast = false
				s.Ready = false
			}
		}
		c.bulkApplied[m.Hash] = true
		c.stats.BulkApplications++
		c.stats.BulkCommits++
	}
	c.send(from, coldMessage{Type: "bulk_applied", G: m.G, Q: m.Q, Hash: m.Hash, Frozen: m.Frozen})
}
