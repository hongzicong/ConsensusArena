package pql

import (
	"testing"
	"time"

	"github.com/hongzicong/ConsensusArena/replica/defs"
	"github.com/hongzicong/ConsensusArena/state"
)

func TestSourceCoherenceTemporaryPartitionMustNotPoisonHealthyMajority(t *testing.T) {
	s := cluster(t, 5)
	s.advance(time.Second)
	s.drop = func(p *Packet, to int) bool { return p.From == 1 || to == 1 }
	s.advance(11 * time.Second)
	s.drop = nil
	s.advance(10 * time.Second)
	w := s.request(0, state.PUT, 7, "after-heal")
	s.advance(5 * time.Second)
	if _, ok := s.replies[w]; !ok {
		t.Fatalf("healthy cluster cannot write after partition healed: leader=%d excludes=%05b suspects=%05b mask=%05b",
			s.cores[0].leader, s.cores[0].policy.excluded, s.cores[0].suspected,
			s.cores[0].policy.writeMask(state.Command{Op: state.PUT, K: 7}))
	}
}

func TestSourceCoherenceRecoveryMustResolveActivationFrontier(t *testing.T) {
	s := cluster(t, 5)
	s.advance(time.Second)
	// Slot zero reaches only the old leader and holder 2, so it is unchosen.
	s.drop = func(p *Packet, to int) bool { return !p.Control && p.Kind == packetAccept && to == 1 }
	w := s.request(0, state.PUT, 7, "unchosen")
	s.advance(100 * time.Millisecond)
	if _, ok := s.replies[w]; ok || s.cores[2].high != 0 || s.cores[1].high != -1 {
		t.Fatal("setup did not leave a minority-accepted write")
	}
	s.alive[0] = false
	// A valid recovery quorum is {1,3,4}; it does not report slot zero.
	s.drop = func(p *Packet, to int) bool {
		return !p.Control && p.Kind == packetPromise && p.From == 2 && to == 1
	}
	s.advance(12 * time.Second)
	if s.cores[1].leader != 1 || !s.cores[1].active {
		t.Fatal("setup: replacement leader did not recover")
	}
	r := s.request(1, state.GET, 8, "")
	s.advance(5 * time.Second)
	if _, ok := s.replies[r]; !ok {
		c := s.cores[1]
		t.Fatalf("read of unrelated key never completes: log=%d/%d catchup=%d live_lease=%v local_pending=%d",
			c.executed, c.high, c.lease.catchup, c.lease.canRead(s.now, c.executed), len(c.localPending))
	}
}

func TestSourceCoherenceOnePeersSuspicionMustNotDisableHealthyMajority(t *testing.T) {
	s := cluster(t, 5)
	s.advance(time.Second)
	// Only lease exchanges involving replica 1 are lost. Normal Paxos
	// heartbeats and its Suspect messages still arrive at the healthy leader.
	s.drop = func(p *Packet, to int) bool {
		return (p.From == 1 || to == 1) && p.Kind >= packetGuard && p.Kind <= packetLeaseReply
	}
	s.advance(10 * time.Second)
	w := s.request(0, state.PUT, 7, "healthy-majority")
	s.advance(5 * time.Second)
	if _, ok := s.replies[w]; !ok {
		c := s.cores[0]
		t.Fatalf("one peer's accusations disabled a healthy majority: excludes=%05b suspects=%05b write_mask=%05b",
			c.policy.excluded, c.suspected, c.policy.writeMask(state.Command{Op: state.PUT, K: 7}))
	}
}

// This asserts fidelity to qlease/src/paxos/paxos.go:handlePropose/reader,
// rather than a stronger liveness property of the abstract paper protocol.
func TestSourceCoherenceHolderWithoutLeaseWaitsLikeOriginalSource(t *testing.T) {
	s := cluster(t, 5)
	s.drop = func(p *Packet, _ int) bool { return p.Kind >= packetGuard && p.Kind <= packetLeaseReply }
	s.advance(time.Second)
	r := s.request(0, state.GET, 7, "")
	s.advance(time.Second)
	if _, replied := s.replies[r]; replied || s.cores[0].loggedReads != 0 {
		t.Fatalf("source fidelity: author reader waits for its lease; port replied=%v logged_reads=%d", replied, s.cores[0].loggedReads)
	}
}

func TestSourceCoherenceCatchupDoesNotAcceptBeforePhaseOne(t *testing.T) {
	now := time.Unix(400, 0)
	c := testCore(1, func(state.Command) state.Value { return nil })
	c.install(Record{Slot: 0, Request: Request{ID: defs.RequestID{Client: 7, Sequence: 1},
		Command: state.Command{Op: state.PUT, K: 7, V: state.Value("minority")}}})
	accepts := 0
	c.send = func(_ int32, p *Packet) {
		if p.Kind == packetAccept || p.Kind == packetCommit {
			accepts++
		}
	}
	c.sendMask = func(_ uint64, _ *Packet) { accepts++ }
	c.Begin(now)
	c.receive(&Packet{Kind: packetFetch, From: 2, Ballot: c.ballot, Floor: 0, High: 4}, now)
	if accepts != 0 || !c.preparing || c.high != 0 {
		t.Fatal("catch-up sent phase-two messages before phase one completed")
	}
	// This quorum excludes the local minority acceptance. Its promises fence
	// the suffix, making no-ops safe in all five previously unchosen slots.
	for _, from := range []int32{2, 3, 4} {
		c.receive(&Packet{Kind: packetPromise, From: from, Ballot: c.ballot, Floor: 0, High: -1, Pages: 1}, now)
	}
	if c.preparing || c.high != 4 || accepts == 0 {
		t.Fatal("deferred frontier was not resolved after phase one")
	}
	for _, rec := range c.log {
		if rec.Request.Command.Op != state.NONE || rec.Committed {
			t.Fatal("catch-up did not propose ordinary, initially uncommitted no-ops")
		}
	}
}

func TestSourceCoherenceFrontierRecoveryPreservesChosenWrite(t *testing.T) {
	s := cluster(t, 5)
	s.advance(time.Second)
	w := s.request(0, state.PUT, 7, "chosen")
	s.advance(time.Second)
	s.expectReply(w, "chosen")
	s.drop = func(p *Packet, to int) bool { return !p.Control && p.Kind == packetAccept && to == 1 }
	unchosen := s.request(0, state.PUT, 7, "minority")
	s.advance(100 * time.Millisecond)
	if _, ok := s.replies[unchosen]; ok || s.cores[2].high != 1 || s.cores[1].high != 0 {
		t.Fatal("setup: second write was not minority accepted")
	}
	s.alive[0] = false
	s.drop = func(p *Packet, to int) bool {
		return !p.Control && p.Kind == packetPromise && p.From == 2 && to == 1
	}
	s.advance(12 * time.Second)
	// Both the leader's activation barrier and the minority acceptor's local
	// pending write must resolve, without replacing the already chosen prefix.
	r1 := s.request(1, state.GET, 7, "")
	r2 := s.request(2, state.GET, 7, "")
	s.advance(5 * time.Second)
	s.expectReply(r1, "chosen")
	s.expectReply(r2, "chosen")
	for id := 1; id < 5; id++ {
		if string(s.stores[id][7]) != "chosen" || s.cores[id].executed < 1 {
			t.Fatalf("replica %d did not preserve the chosen prefix while resolving slot 1", id)
		}
	}
}

func TestSourceCoherenceNewLeaderWaitsForHolderConfiguration(t *testing.T) {
	s := cluster(t, 5)
	// The next leader (replica 1) is not an initial holder of this key.
	s.cores[0].policy.holders[7] = 0b11001
	s.advance(time.Second)
	w := s.request(0, state.PUT, 7, "preserved")
	s.advance(time.Second)
	s.expectReply(w, "preserved")
	s.alive[0] = false
	s.advance(11 * time.Second)
	if s.cores[1].policy.holds(1, 7) {
		t.Fatal("setup: new leader unexpectedly already holds this key")
	}
	r := s.request(1, state.GET, 7, "")
	s.advance(100 * time.Millisecond)
	if _, ok := s.replies[r]; ok || s.cores[1].loggedReads != 0 {
		t.Fatal("new leader bypassed holder configuration by logging or locally reading the key")
	}
	// The ordinary read-frequency configuration update adds the new leader.
	s.advance(13 * time.Second)
	s.expectReply(r, "preserved")
	if s.cores[1].loggedReads != 0 || s.cores[1].localReads == 0 {
		t.Fatal("new leader did not use its newly activated read lease")
	}
}
