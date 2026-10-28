package agreement

import (
	"crypto/sha256"
	"math/bits"
	"time"
)

type coldScanState struct {
	Request        request
	Begin          uint64
	Ref            coldScanRef
	ResultChosen   bool
	ResultProposed bool
	Body           []byte
	Computed       bool
	Voters         uint64
	Last           time.Time
}

func (c *coldReplica) submitScan(req request, now time.Time) bool {
	e := c.r.engine
	if e.id != e.current.Leader {
		return false
	}
	if _, ok := e.completed[req.id()]; ok {
		return false
	}
	if _, ok := c.scanRefs[req.id()]; ok {
		c.fetchScan(c.scanRefs[req.id()])
		return true
	}
	if !e.active() || !e.prepared {
		e.enqueue(req)
		return true
	}
	if c.scan != nil {
		if c.scan.Request.id() != req.id() {
			e.enqueue(req)
		}
		return true
	}
	e.flushBatch(now)
	e.admissionPaused = true
	req.Stage = 1
	c.scan = &coldScanState{Request: req}
	e.proposeBatch([]request{req}, now)
	return true
}
func (c *coldReplica) applyScan(req request, slot uint64) bool {
	if req.Stage == 0 {
		return false
	}
	e := c.r.engine
	switch req.Stage {
	case 1:
		e.admissionPaused = true
		if c.scan == nil || c.scan.Request.id() != req.id() {
			c.scan = &coldScanState{Request: req, Begin: slot}
		} else {
			c.scan.Begin = slot
		}
		c.stats.ScanBegins++
	case 2:
		c.scanRefs[req.id()] = req
		c.stats.ScanResults++
		if c.scan == nil {
			c.scan = &coldScanState{Request: req, Begin: req.Scan.Begin}
			c.scan.Request.Stage = 1
		}
		if c.scan.Request.id() == req.id() {
			c.scan.ResultChosen = true
			c.scan.Ref = req.Scan
			e.admissionPaused = true
		}
		c.fetchScan(req)
	}
	return true
}
func (c *coldReplica) globalReady(g coldGeneration) bool {
	p := c.prep
	if p == nil || p.G != g {
		return false
	}
	mask := uint64(0)
	for id := range p.Ready {
		mask |= bit(id)
	}
	return mask&p.Q == p.Q && p.Q != 0
}
func (c *coldReplica) driveScan(now time.Time) {
	s := c.scan
	e := c.r.engine
	if s == nil || s.Begin == 0 || e.id != e.current.Leader || !e.active() || !e.prepared || !e.stable(now) {
		return
	}
	if s.ResultChosen {
		g := coldGeneration{B: e.current.Ballot, E: 2*s.Begin + 1}
		if c.promised.less(g) {
			c.startGlobal(g, false, now)
		}
		if c.globalReady(g) {
			e.admissionPaused = false
			c.scan = nil
		}
		return
	}
	g := coldGeneration{B: e.current.Ballot, E: 2 * s.Begin}
	if c.promised.less(g) {
		c.startGlobal(g, true, now)
	}
	if !c.globalReady(g) || !c.frozen || e.prefix < s.Begin {
		return
	}
	if !s.Computed {
		s.Body = s.Request.Proposal.Command.Execute(c.r.State)
		s.Ref = coldScanRef{Begin: s.Begin, Hash: sha256.Sum256(s.Body), Length: uint64(len(s.Body))}
		s.Computed = true
		c.bodies[s.Ref.Hash] = s.Body
		s.Voters |= bit(e.id)
	}
	if bits.OnesCount64(s.Voters) >= e.majority {
		if !s.ResultProposed {
			req := s.Request
			req.Stage = 2
			req.Scan = s.Ref
			req.Scan.Voters = s.Voters
			s.ResultProposed = true
			e.proposeBatch([]request{req}, now)
		}
		return
	}
	if now.Sub(s.Last) >= 400*time.Millisecond {
		s.Last = now
		n := (len(s.Body) + coldPartBytes - 1) / coldPartBytes
		if n == 0 {
			n = 1
		}
		for to := 0; to < e.n; to++ {
			if s.Voters&bit(to) != 0 {
				continue
			}
			for i := 0; i < n; i++ {
				a, b := i*coldPartBytes, min((i+1)*coldPartBytes, len(s.Body))
				c.send(to, coldMessage{Type: "scan_data", G: g, Ref: s.Ref, Hash: s.Ref.Hash, Part: i, Parts: n, Data: s.Body[a:b]})
			}
		}
	}
}
func (c *coldReplica) fetchScan(req request) {
	ref := req.Scan
	if bits.OnesCount64(ref.Voters) < c.r.N/2+1 {
		return
	}
	body, ok := c.bodies[ref.Hash]
	if ok && uint64(len(body)) == ref.Length && sha256.Sum256(body) == ref.Hash {
		e := c.r.engine
		e.completed[req.id()] = body
		if e.id == e.current.Leader || req.Origin == e.id {
			e.finish(req, body)
		}
		delete(c.scanRefs, req.id())
		return
	}
	c.broadcast(coldMessage{Type: "scan_need", G: c.promised, Ref: ref, Hash: ref.Hash})
}
func (c *coldReplica) receiveScanData(from int, m coldMessage, now time.Time) {
	if from < 0 || from >= c.r.N {
		return
	}
	switch m.Type {
	case "scan_need":
		body, ok := c.bodies[m.Hash]
		if !ok || uint64(len(body)) != m.Ref.Length {
			return
		}
		n := (len(body) + coldPartBytes - 1) / coldPartBytes
		if n == 0 {
			n = 1
		}
		for i := 0; i < n; i++ {
			a, b := i*coldPartBytes, min((i+1)*coldPartBytes, len(body))
			c.send(from, coldMessage{Type: "scan_data", G: m.G, Ref: m.Ref, Hash: m.Hash, Part: i, Parts: n, Data: body[a:b]})
		}
	case "scan_data":
		if m.Hash != m.Ref.Hash {
			return
		}
		p := c.chunks[m.Hash]
		if p == nil {
			p = &coldParts{}
			c.chunks[m.Hash] = p
		}
		body, ok := addColdPart(p, m)
		if !ok || uint64(len(body)) != m.Ref.Length {
			return
		}
		c.bodies[m.Hash] = body
		c.stats.ScanDataHeld++
		c.send(from, coldMessage{Type: "scan_held", G: m.G, Ref: m.Ref, Hash: m.Hash})
		for _, req := range c.scanRefs {
			if req.Scan.Hash == m.Hash {
				c.fetchScan(req)
			}
		}
	case "scan_held":
		s := c.scan
		if s == nil || !s.Computed || s.Ref != m.Ref || m.Hash != s.Ref.Hash {
			return
		}
		s.Voters |= bit(from)
		c.driveScan(now)
	}
}
