package agreement

import (
	"encoding/json"
	"github.com/hongzicong/ConsensusArena/state"
	"math/bits"
	"sort"
	"time"
)

const catalogBatch = 64
const catalogRequestBudget = 1024

type coldPayloadTarget struct {
	To   int
	Hash [32]byte
}
type coldPendingManifest struct {
	From        int
	Message     coldMessage
	Bundle      coldBundle
	Missing     map[[32]byte]bool
	Since, Last time.Time
}

// A phase-one report binds retained records, not a new acceptance or read grant.
func (c *coldReplica) snapshotMetadataComplete(s coldSnapshot) bool {
	for _, key := range s.Keys {
		if !c.isRecoveryKey(key.Key) {
			return false
		}
		if key.Prefix > 0 && (key.Last.Key != key.Key || key.Last.Version != key.Prefix || !c.validChoice(key.Last)) {
			return false
		}
		for _, rec := range key.Accepted {
			if rec.Key != key.Key || rec.Origin < 0 || rec.Origin >= c.r.N {
				return false
			}
		}
	}
	return true
}

func (c *coldReplica) rememberPayloadHolders(p *coldPrepareState, from int, s coldSnapshot) {
	for _, key := range s.Keys {
		if key.Prefix > 0 {
			p.PayloadHolders[key.Last.Hash] |= bit(from) | key.Last.Voters
		}
		for _, rec := range key.Accepted {
			p.PayloadHolders[rec.Hash] |= bit(from)
		}
	}
}

func (c *coldReplica) manifestReferences(b coldBundle) (map[[32]byte]state.Key, bool) {
	refs := map[[32]byte]state.Key{}
	add := func(rec coldRecord) bool {
		if !c.isRecoveryKey(rec.Key) || rec.Origin < 0 || rec.Origin >= c.r.N {
			return false
		}
		if key, exists := refs[rec.Hash]; exists && key != rec.Key {
			return false
		}
		refs[rec.Hash] = rec.Key
		return true
	}
	for _, s := range b.Keys {
		if !c.isRecoveryKey(s.Key) || len(s.Accepted) != 0 {
			return nil, false
		}
		if s.Prefix > 0 && (s.Last.Key != s.Key || s.Last.Version != s.Prefix || !c.validChoice(s.Last) || !add(s.Last)) {
			return nil, false
		}
	}
	for _, rec := range b.Tails {
		if !add(rec) {
			return nil, false
		}
	}
	return refs, true
}

// This transports body availability only. The counted vote remains in cold_bulk.go.
func (c *coldReplica) requestCatalogBodies(g coldGeneration, hashes [][32]byte, holders map[[32]byte]uint64, now time.Time) {
	e := c.r.engine
	if !e.active() || g.B != e.current.Ballot {
		return
	}
	healthy := bit(e.id)
	for i := 0; i < e.n; i++ {
		if e.peerHealthy(i, now) {
			healthy |= bit(i)
		}
	}
	groups := map[int][][32]byte{}
	budget := catalogRequestBudget
	// Stable ordering makes the bounded request rotation auditable; received bodies
	// leave the wanted set and retry clocks let later hashes proceed in the meantime.
	sort.Slice(hashes, func(i, j int) bool { return string(hashes[i][:]) < string(hashes[j][:]) })
	for _, h := range hashes {
		if _, exists := c.inputs[h]; exists {
			continue
		}
		if budget == 0 {
			break
		}
		to := e.current.Leader
		if e.id == e.current.Leader {
			mask := holders[h] & healthy &^ bit(e.id)
			if mask == 0 {
				// Never replace a selected value within this generation because a holder
				// disappeared. A higher parent ballot repeats physical phase-one selection.
				if holders[h] != 0 && holders[h]&healthy == 0 && bits.OnesCount64(healthy) >= e.majority && e.pending.Ballot == 0 {
					c.stats.MissingHolderCampaigns++
					e.proposeFilteredRoster(e.id, healthy, now)
					return
				}
				mask = healthy &^ bit(e.id)
			}
			if mask == 0 {
				continue
			}
			to = bits.TrailingZeros64(mask)
		}
		if to == e.id {
			continue
		}
		target := coldPayloadTarget{to, h}
		if last := c.payloadRetry[target]; !last.IsZero() && now.Sub(last) < 400*time.Millisecond {
			continue
		}
		c.payloadRetry[target] = now
		groups[to] = append(groups[to], h)
		budget--
	}
	for to, hs := range groups {
		for len(hs) > 0 {
			count := min(catalogBatch, len(hs))
			if c.send(to, coldMessage{Type: "payload_need", G: g, Hashes: hs[:count]}) {
				c.stats.PayloadNeedFrames++
				c.stats.PayloadRequestedBodies += uint64(count)
			}
			hs = hs[count:]
		}
	}
}

func (c *coldReplica) receiveCatalogPayloads(from int, m coldMessage, now time.Time) {
	if from < 0 || from >= c.r.N {
		return
	}
	switch m.Type {
	case "payload_need":
		if len(m.Hashes) > catalogBatch {
			return
		}
		batch := []request{}
		bytes := 0
		flush := func() {
			if len(batch) > 0 && c.send(from, coldMessage{Type: "payload_bodies", G: m.G, Payloads: batch}) {
				c.stats.PayloadReplyFrames++
				c.stats.PayloadReplyBodies += uint64(len(batch))
				c.stats.PayloadBodyBytes += uint64(bytes)
			}
			batch = nil
			bytes = 0
		}
		for _, h := range m.Hashes {
			req, ok := c.inputs[h]
			if !ok {
				continue
			}
			data, err := json.Marshal(req)
			if err != nil {
				continue
			}
			if len(data) > coldPartBytes {
				flush()
				c.sendParts(from, "data", m.G, h, data, false, 0)
				continue
			}
			if bytes+len(data) > coldPartBytes {
				flush()
			}
			batch = append(batch, req)
			bytes += len(data)
		}
		flush()
	case "payload_bodies":
		if len(m.Payloads) > catalogBatch {
			return
		}
		for _, req := range m.Payloads {
			if req.Proposal.Command.Op != state.PUT || !c.isCold(req.Proposal.Command.K) || req.Origin < 0 || req.Origin >= c.r.N {
				c.stats.PayloadRejectedBodies++
				continue
			}
			h := c.retain(req)
			c.stats.PayloadReceivedBodies++
			for _, p := range c.manifestPending {
				delete(p.Missing, h)
			}
		}
		c.drivePendingManifests(now)
		c.driveGlobal(now)
		if c.hasBridge() || c.migrated {
			c.r.engine.apply()
		}
	}
}

func (c *coldReplica) tryPendingManifest(hash [32]byte, now time.Time) {
	p := c.manifestPending[hash]
	if p == nil {
		return
	}
	e := c.r.engine
	if !e.active() || p.Message.G != c.promised || p.Message.G.B != e.current.Ballot || p.From != e.current.Leader {
		return
	}
	if now.Sub(p.Last) < 100*time.Millisecond && len(p.Missing) > 0 {
		return
	}
	p.Last = now
	missing := make([][32]byte, 0, len(p.Missing))
	for h := range p.Missing {
		if _, ok := c.inputs[h]; ok {
			delete(p.Missing, h)
		} else {
			missing = append(missing, h)
		}
	}
	if len(missing) > 0 {
		c.requestCatalogBodies(p.Message.G, missing, nil, now)
		return
	}
	c.acceptCompleteManifest(p.From, p.Message, p.Bundle, now)
	if c.bulkAccepted[p.Message.G] == hash {
		c.stats.ManifestWaitSamples++
		c.stats.ManifestWaitNanos += uint64(now.Sub(p.Since))
		delete(c.manifestPending, hash)
	}
}

func (c *coldReplica) drivePendingManifests(now time.Time) {
	if c.migrated || c.hasBridge() {
		return
	}
	for hash := range c.manifestPending {
		c.tryPendingManifest(hash, now)
	}
}
