package agreement

import (
	"crypto/sha256"
	"encoding/json"
	"github.com/hongzicong/ConsensusArena/replica/defs"
	"github.com/hongzicong/ConsensusArena/state"
	"math/bits"
	"time"
)

const bridgeOp = state.Operation(247)

type bridgeValue struct {
	Data   []byte
	Hash   [32]byte
	Voters uint64
}
type bridgePending struct {
	Value   bridgeValue
	Bundle  coldBundle
	Missing map[[32]byte]bool
	Since   time.Time
	Applied bool
}

func (c *coldReplica) advertisedQ() uint64 {
	if c.migrated {
		return 0
	}
	return c.q
}
func (c *coldReplica) hasBridge() bool {
	if !c.enabled || c.migrated {
		return c.migrated
	}
	return c.r.engine.bridgeSlot != 0
}
func (c *coldReplica) proposeBridge(now time.Time) {
	e := c.r.engine
	p := c.prep
	if !c.enabled || c.migrated || c.hasBridge() || c.scan != nil || p == nil || !p.Frozen || !p.Enabled || p.G.B != e.current.Ballot || e.id != e.current.Leader || !e.active() || !e.prepared {
		return
	}
	if c.bridgeProposed && c.bridgeBallot == e.current.Ballot {
		return
	}
	value := bridgeValue{Data: p.Bundle, Hash: p.BundleHash, Voters: p.ChoiceMask}
	data, err := json.Marshal(value)
	if err != nil || len(data) > maxFrame-4096 {
		return
	}
	req := request{Origin: -1, Proposal: defs.Propose{ClientId: -2147483647, CommandId: int32(e.current.Ballot), Command: state.Command{Op: bridgeOp, V: data}}}
	c.bridgeProposed = true
	c.bridgeBallot = e.current.Ballot
	c.bridgeSince = now
	c.stats.BridgeProposals++
	c.stats.BridgeBytes += uint64(len(data))
	e.flushBatch(now)
	e.proposeBatch([]request{req}, now)
}
func (c *coldReplica) parseBridge(req request) *bridgePending {
	h := sha256.Sum256(req.Proposal.Command.V)
	if p := c.bridgeRoots[h]; p != nil {
		return p
	}
	var v bridgeValue
	var b coldBundle
	if json.Unmarshal(req.Proposal.Command.V, &v) != nil || sha256.Sum256(v.Data) != v.Hash || c.decodeRecovery(v.Data, &b) != nil || !b.Frozen || b.G.B == 0 || bits.OnesCount64(b.Q) != c.r.N/2+1 || b.Q>>uint(c.r.N) != 0 || bits.OnesCount64(v.Voters) < c.r.N/2+1 || v.Voters&b.Q != b.Q || v.Voters>>uint(c.r.N) != 0 {
		return nil
	}
	refs, ok := c.manifestReferences(b)
	if !ok {
		return nil
	}
	p := &bridgePending{Value: v, Bundle: b, Missing: map[[32]byte]bool{}, Since: time.Now()}
	for hash, key := range refs {
		body, exists := c.inputs[hash]
		if !exists || body.Proposal.Command.K != key || commandDigest(body.Proposal.Command) != hash {
			p.Missing[hash] = true
		}
	}
	c.bridgeRoots[h] = p
	c.stats.BridgeMissingBodies += uint64(len(p.Missing))
	return p
}
func (c *coldReplica) beforeBridgeApply(v entry) bool {
	for _, req := range v.requests() {
		if req.Proposal.Command.Op != bridgeOp || c.migrated {
			continue
		}
		p := c.parseBridge(req)
		if p == nil {
			return false
		}
		missing := make([][32]byte, 0, len(p.Missing))
		holders := map[[32]byte]uint64{}
		for h := range p.Missing {
			if _, ok := c.inputs[h]; ok {
				delete(p.Missing, h)
			} else {
				missing = append(missing, h)
				holders[h] = p.Value.Voters
			}
		}
		if len(missing) > 0 {
			c.stats.BridgeWaits++
			c.requestCatalogBodies(coldGeneration{B: c.r.engine.current.Ballot}, missing, holders, time.Now())
			return false
		}
	}
	return true
}
func (c *coldReplica) applyBridgeOrScan(req request, slot uint64) bool {
	if req.Proposal.Command.Op != bridgeOp {
		if c.migrated {
			return false
		}
		return c.applyScan(req, slot)
	}
	if c.migrated {
		return true
	}
	p := c.parseBridge(req)
	if p == nil || len(p.Missing) > 0 {
		panic("bridge prefix advanced before full foundation")
	}
	e := c.r.engine
	for _, key := range p.Bundle.Keys {
		if key.Prefix > 0 {
			body := c.inputs[key.Last.Hash]
			body.Proposal.Command.Execute(c.r.State)
		}
		for _, d := range key.Done {
			e.completed[d.ID] = nil
			c.done[d.ID] = d.Hash
		}
	}
	for _, rec := range p.Bundle.Tails {
		body := c.inputs[rec.Hash]
		body.Proposal.Command.Execute(c.r.State)
		e.completed[rec.ID] = nil
		c.done[rec.ID] = rec.Hash
	}
	p.Applied = true
	c.migrated = true
	c.ready = false
	c.frozen = true
	e.strictPrefix = true
	c.stats.BridgeInstallations++
	c.stats.BridgeWaitNanos += uint64(time.Since(p.Since))
	c.outgoing = map[int][][]byte{}
	c.outgoingBytes = map[int]int{}
	c.stats.QueuedBytes = 0
	c.keyPrep = map[state.Key]*coldKeyPrepare{}
	c.prep = nil
	// Retain body/evidence caches for another member's ordinary phase-one replay.
	return true
}
func (c *coldReplica) finishMigrated(req request) {
	e := c.r.engine
	if !c.migrated || e.id != e.current.Leader || req.Proposal.Command.Op != state.PUT || !c.isCold(req.Proposal.Command.K) {
		return
	}
	if _, done := e.completed[req.id()]; done {
		c.replyDone(req, commandDigest(req.Proposal.Command))
	}
}
func (c *coldReplica) tickBridge(now time.Time) {
	e := c.r.engine
	c.ready = false
	c.frozen = true
	e.apply()
	if c.migrated {
		budget := 256
		for id, req := range c.reads {
			if budget == 0 {
				break
			}
			delete(c.reads, id)
			delete(c.readSince, id)
			e.submit(req, now)
			budget--
			c.stats.BridgeReplay++
		}
		for id, req := range c.replayPending {
			if budget == 0 {
				break
			}
			delete(c.replayPending, id)
			e.submit(req, now)
			budget--
			c.stats.BridgeReplay++
		}
	}
	// Availability transport remains live, but old cold authority transitions are quarantined.
	c.flushOutgoing()
}
