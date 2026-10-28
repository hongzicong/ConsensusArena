package pql

import (
	"container/heap"
	"testing"
	"time"

	"github.com/hongzicong/ConsensusArena/replica/defs"
	"github.com/hongzicong/ConsensusArena/state"
)

func regressionAdvanceWithPausedReplica(s *testCluster, paused int, duration time.Duration) {
	end := s.now.Add(duration)
	for s.now.Before(end) {
		s.now = s.now.Add(10 * time.Millisecond)
		for s.queue.Len() > 0 && !s.queue[0].at.After(s.now) {
			e := heap.Pop(&s.queue).(delivery)
			if s.alive[e.to] && e.to != paused {
				s.cores[e.to].receive(e.packet, s.now)
			}
		}
		for id, c := range s.cores {
			if s.alive[id] && id != paused {
				c.tick(s.now, append([]bool(nil), s.alive...))
			}
		}
	}
}

// Regression coverage for the five paper/source coherence review findings.
// The original reproduction tests remain under report/analysis/pql-review.
func TestRegressionStaleMaintenanceMustNotServeExpiredLease(t *testing.T) {
	s := cluster(t, 5)
	s.advance(time.Second)
	w := s.request(0, state.PUT, 7, "old")
	s.advance(time.Second)
	s.expectReply(w, "old")
	queuedTick := s.now.Add(2 * time.Millisecond)
	if !s.cores[1].lease.canRead(queuedTick, s.cores[1].executed) {
		t.Fatal("setup: holder has no active lease")
	}
	s.drop = func(p *Packet, to int) bool { return p.From == 1 || to == 1 }
	regressionAdvanceWithPausedReplica(s, 1, 11*time.Second)
	w = s.request(0, state.PUT, 7, "new")
	regressionAdvanceWithPausedReplica(s, 1, time.Second)
	s.expectReply(w, "new")
	read := s.request(1, state.GET, 7, "")
	if _, ok := s.replies[read]; ok {
		t.Fatal("setup: fresh-time submission unexpectedly answered")
	}
	// EventLoop.Run passes the buffered ticker timestamp to Machine.Tick.
	// A delayed timestamp may be processed after a fresh-time Propose event.
	s.cores[1].tick(queuedTick, append([]bool(nil), s.alive...))
	if got, ok := s.replies[read]; ok {
		t.Fatalf("expired isolated holder answered %q after completed write new; delayed tick=%v actual=%v", got, queuedTick, s.now)
	}
}

func TestRegressionUnresponsiveConnectedLeaderMustBeReplaced(t *testing.T) {
	s := cluster(t, 5)
	s.advance(time.Second)
	// Silent message loss without a connection-close notification; four peers
	// remain mutually connected and continue executing their event loops.
	s.drop = func(p *Packet, to int) bool { return p.From == 0 || to == 0 }
	s.advance(15 * time.Second)
	w := s.request(1, state.PUT, 8, "available")
	s.advance(5 * time.Second)
	if _, ok := s.replies[w]; !ok {
		t.Fatalf("responsive majority made no progress: leader=%d preparing=%v suspected=%b transport_alive=%v", s.cores[1].leader, s.cores[1].preparing, s.cores[1].suspected, s.alive)
	}
}

func TestRegressionReadWaitsForLeaseLikeOriginalSource(t *testing.T) {
	s := cluster(t, 5)
	s.drop = func(p *Packet, to int) bool {
		return p.Kind >= packetGuard && p.Kind <= packetLeaseReply
	}
	w := s.request(0, state.PUT, 7, "available")
	s.advance(time.Second)
	s.expectReply(w, "available")
	read := s.request(0, state.GET, 7, "")
	s.advance(time.Second)
	if _, ok := s.replies[read]; ok || s.cores[0].loggedReads != 0 {
		t.Fatal("holder read bypassed lease wait through command consensus")
	}
	s.drop = nil
	s.advance(time.Second)
	s.expectReply(read, "available")
}

func TestRegressionLocalNoopMustComplete(t *testing.T) {
	s := cluster(t, 5)
	s.advance(time.Second)
	r := s.request(0, state.NONE, 0, "")
	s.advance(time.Second)
	if _, ok := s.replies[r]; !ok {
		t.Fatalf("accepted no-op never completed: executed=%d", s.cores[0].executed)
	}
}

func TestRegressionRenewalAckMustNotExtendOriginalGrantBound(t *testing.T) {
	now := time.Unix(200, 0)
	l := newLeaseState(5, 0)
	// A previous promise has been acknowledged. Its ACK may guard a renewal.
	l.sentSequence[1], l.ackSequence[1] = 1, 1
	l.lastAckTime[1] = now.Add(-100 * time.Millisecond)
	l.sendPromise(now, 1, -1, false, func(int32, *Packet) {})
	bound := l.latestReplies[1]
	l.receive(&Packet{Kind: packetLeaseReply, From: 1, LeaseEpoch: genesisLeaseEpoch,
		Timestamp: 2}, now.Add(3500*time.Millisecond), -1, func(int32, *Packet) {})
	if l.latestReplies[1].After(bound) {
		t.Fatalf("late renewal ACK extended grant deadline from %v to %v; Figure 5 requires min(existing deadline, now+lease)", bound, l.latestReplies[1])
	}
}

func TestRuntimeClockRejectsExpiredLeaseDespiteOldTick(t *testing.T) {
	now := time.Unix(300, 0)
	c := testCore(1, func(state.Command) state.Value {
		t.Fatal("expired holder executed a local read")
		return nil
	})
	c.now = func() time.Time { return now }
	c.lease.promisedTo = genesisLeaseEpoch
	c.lease.latestPromises[0], c.lease.latestPromises[2] = now.Add(time.Second), now.Add(time.Second)
	c.lease.recomputeReadUntil(now)
	c.updating[7] = 1
	r := Request{ID: defs.RequestID{Client: 1, Sequence: 1}, Origin: 1, Command: state.Command{Op: state.GET, K: 7}}
	c.submit(r, now)
	if len(c.localPending) != 1 {
		t.Fatal("setup: read did not wait behind accepted write")
	}
	oldTick := now
	now = now.Add(3 * time.Second)
	delete(c.updating, 7)
	forwards := 0
	c.send = func(_ int32, p *Packet) {
		if p.Kind == packetForward {
			forwards++
		}
	}
	c.tick(oldTick, []bool{true, true, true, true, true})
	if !c.clock.Equal(now) || c.localReads != 0 || forwards != 0 || len(c.localPending) != 1 {
		t.Fatalf("stale tick bypassed runtime clock: clock=%v reads=%d forwards=%d", c.clock, c.localReads, forwards)
	}
}

func TestEventClockCannotMoveBackwards(t *testing.T) {
	now := time.Unix(310, 0)
	c := testCore(1, func(state.Command) state.Value { return nil })
	c.receive(&Packet{From: 0, Kind: packetReady, Ballot: c.ballot}, now)
	c.tick(now.Add(-time.Second), []bool{true, true, true, true, true})
	if !c.clock.Equal(now) {
		t.Fatalf("protocol clock moved backwards: %v", c.clock)
	}
}

func TestForwardedReadWaitsForLeaderLease(t *testing.T) {
	s := cluster(t, 5)
	s.drop = func(p *Packet, _ int) bool { return p.Kind >= packetGuard && p.Kind <= packetLeaseReply }
	w := s.request(0, state.PUT, 7, "ordered")
	s.advance(time.Second)
	s.expectReply(w, "ordered")
	r := s.request(4, state.GET, 7, "")
	s.advance(time.Second)
	if _, ok := s.replies[r]; ok || s.cores[0].loggedReads != 0 {
		t.Fatal("forwarded read bypassed the leader's lease wait")
	}
	s.drop = nil
	s.advance(time.Second)
	s.expectReply(r, "ordered")
	if s.cores[0].localReads != 1 || s.cores[4].forwardedReads != 1 || s.cores[4].localReads != 0 {
		t.Fatal("read did not complete via the leader's reader path")
	}
}

func TestLeaseMessageLossDoesNotPreemptResponsiveLeader(t *testing.T) {
	s := cluster(t, 5)
	s.drop = func(p *Packet, _ int) bool { return p.Kind >= packetGuard && p.Kind <= packetLeaseReply }
	s.advance(7 * time.Second)
	for id, c := range s.cores {
		if c.leader != 0 || c.recoveries != 0 {
			t.Fatalf("replica %d preempted responsive leader after lease-only loss", id)
		}
	}
}

func TestForwardedNoopCachesCompletionWithoutExecutingState(t *testing.T) {
	s := cluster(t, 5)
	s.advance(time.Second)
	for _, c := range s.cores {
		c.execute = func(state.Command) state.Value {
			t.Fatal("no-op called application state machine")
			return nil
		}
	}
	id := s.request(1, state.NONE, 0, "")
	s.advance(time.Second)
	s.expectReply(id, "")
	high := s.cores[0].high
	delete(s.replies, id)
	s.cores[1].submit(Request{ID: id, Origin: 1, Command: state.Command{Op: state.NONE}}, s.now)
	s.expectReply(id, "")
	if s.cores[0].high != high || s.cores[1].replayReplies != 1 {
		t.Fatal("no-op retry was not served from its executed result")
	}
}
