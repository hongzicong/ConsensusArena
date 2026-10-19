package xpaxos

import (
	"encoding/json"
	"github.com/hongzicong/ConsensusArena/state"
	"time"
)

func (c *coldReplica) needData(h [32]byte) {
	now := time.Now()
	if now.Sub(c.dataRetry[h]) < 400*time.Millisecond {
		return
	}
	c.dataRetry[h] = now
	c.stats.DataNeeds++
	c.broadcast(coldMessage{Type: "need", G: c.promised, Hash: h})
}
func (c *coldReplica) receiveData(from int, m coldMessage, now time.Time) {
	if from < 0 || from >= c.r.N {
		return
	}
	if m.Type == "proof_need" {
		s := c.keys[m.Key]
		if s != nil {
			if rec, ok := s.Choices[m.Record.Version]; ok {
				if req, ok := c.input(rec); ok {
					c.send(from, coldMessage{Type: "chosen", G: rec.Round.G, Record: rec, Request: req})
				}
			}
		}
		return
	}
	if m.Type == "need" {
		if req, ok := c.inputs[m.Hash]; ok {
			data, _ := json.Marshal(req)
			c.sendParts(from, "data", m.G, m.Hash, data, false, 0)
		}
		return
	}
	if _, retained := c.inputs[m.Hash]; retained {
		return
	}
	chunks := c.chunks[m.Hash]
	if chunks == nil {
		chunks = &coldParts{}
		c.chunks[m.Hash] = chunks
	}
	data, ok := addColdPart(chunks, m)
	c.stats.DataParts++
	if !ok {
		return
	}
	var req request
	if json.Unmarshal(data, &req) != nil || req.Proposal.Command.Op != state.PUT || commandDigest(req.Proposal.Command) != m.Hash {
		return
	}
	c.retain(req)
	if c.migrated || c.hasBridge() {
		c.r.engine.apply()
		return
	}
	for k, s := range c.keys {
		if _, ok := s.Choices[s.Prefix+1]; ok {
			c.applyKey(k, now)
		}
	}
	for k, p := range c.keyPrep {
		if !p.Prepared {
			c.prepareKeyChoices(k, now)
		} else {
			c.driveKey(k, now)
		}
	}
	c.driveGlobal(now)
}

func (c *coldReplica) needProof(key state.Key, version uint64) {
	c.stats.ProofGaps++
	c.broadcast(coldMessage{Type: "proof_need", G: c.promised, Key: key, Record: coldRecord{ColdVoteIdentity: ColdVoteIdentity{Version: version}}})
}
