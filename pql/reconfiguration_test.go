package pql

import (
	"bytes"
	"container/heap"
	"fmt"
	"testing"
	"time"

	"github.com/hongzicong/ConsensusArena/replica/defs"
	"github.com/hongzicong/ConsensusArena/state"
)

type delivery struct {
	at     time.Time
	serial int
	to     int
	packet *Packet
}
type deliveries []delivery

func (q deliveries) Len() int { return len(q) }
func (q deliveries) Less(i, j int) bool {
	if q[i].at.Equal(q[j].at) {
		return q[i].serial < q[j].serial
	}
	return q[i].at.Before(q[j].at)
}
func (q deliveries) Swap(i, j int) { q[i], q[j] = q[j], q[i] }
func (q *deliveries) Push(v any)   { *q = append(*q, v.(delivery)) }
func (q *deliveries) Pop() any     { old := *q; v := old[len(old)-1]; *q = old[:len(old)-1]; return v }

// Deterministic wire-level event driver: network delivery, both consensus
// logs, lease timers, crashes and client completion share one simulated clock.
type testCluster struct {
	t        *testing.T
	cores    []*core
	stores   []map[state.Key]state.Value
	alive    []bool
	now      time.Time
	queue    deliveries
	serial   int
	sequence int32
	replies  map[defs.RequestID]state.Value
	drop     func(*Packet, int) bool
}

func cluster(t *testing.T, n int) *testCluster {
	s := &testCluster{t: t, cores: make([]*core, n), stores: make([]map[state.Key]state.Value, n), alive: make([]bool, n), now: time.Unix(100, 0), replies: map[defs.RequestID]state.Value{}}
	mask := uint64(1)<<uint(n/2+1) - 1
	policy := &leasePolicy{n: n, leader: 0, holders: map[state.Key]uint64{7: mask}, defaultMask: mask}
	for id := 0; id < n; id++ {
		i := id
		s.alive[id] = true
		s.stores[id] = map[state.Key]state.Value{}
		c := newCore(n, int32(id), 0, policy, func(cmd state.Command) state.Value {
			if cmd.Op == state.PUT {
				s.stores[i][cmd.K] = append(state.Value(nil), cmd.V...)
			}
			return append(state.Value(nil), s.stores[i][cmd.K]...)
		})
		s.cores[id] = c
		c.reply = func(r Request, v state.Value) { s.replies[r.ID] = append(state.Value(nil), v...) }
		c.send = func(to int32, p *Packet) { s.send(int(to), p) }
		c.broadcast = func(p *Packet) {
			for to := range s.cores {
				if to != i {
					s.send(to, p)
				}
			}
		}
		c.sendMask = func(mask uint64, p *Packet) {
			for to := range s.cores {
				if mask&(uint64(1)<<uint(to)) != 0 {
					s.send(to, p)
				}
			}
		}
	}
	return s
}

func (s *testCluster) send(to int, p *Packet) {
	if to == int(p.From) {
		s.cores[to].receive(p, s.now)
		return
	}
	if s.drop != nil && s.drop(p, to) {
		return
	}
	data, err := p.MarshalProtoFrame(0, false)
	if err != nil {
		s.t.Fatal(err)
	}
	var cp Packet
	if err = cp.Unmarshal(bytes.NewReader(data)); err != nil {
		s.t.Fatal(err)
	}
	if err = validatePacket(&cp); err != nil {
		s.t.Fatal(err)
	}
	s.serial++
	heap.Push(&s.queue, delivery{at: s.now.Add(10 * time.Millisecond), serial: s.serial, to: to, packet: &cp})
}

func (s *testCluster) advance(duration time.Duration) {
	end := s.now.Add(duration)
	for s.now.Before(end) {
		s.now = s.now.Add(10 * time.Millisecond)
		for s.queue.Len() > 0 && !s.queue[0].at.After(s.now) {
			e := heap.Pop(&s.queue).(delivery)
			if s.alive[e.to] {
				s.cores[e.to].receive(e.packet, s.now)
			}
		}
		for id, c := range s.cores {
			if s.alive[id] {
				c.tick(s.now, append([]bool(nil), s.alive...))
			}
		}
	}
}

func (s *testCluster) request(to int, op state.Operation, key state.Key, value string) defs.RequestID {
	s.sequence++
	id := defs.RequestID{Client: 20, Sequence: s.sequence}
	s.cores[to].submit(Request{ID: id, Origin: int32(to), Command: state.Command{Op: op, K: key, V: state.Value(value)}}, s.now)
	return id
}

func (s *testCluster) expectReply(id defs.RequestID, want string) {
	s.t.Helper()
	value, ok := s.replies[id]
	if !ok || string(value) != want {
		for i, c := range s.cores {
			if s.alive[i] {
				s.t.Logf("replica=%d leader=%d active=%v data=%d/%d configs=%d/%d epoch=%d/%d/%d excludes=%x suspects=%x lease_until=%v", i, c.leader, c.active, c.executed, c.high, c.configLog.executed, c.configLog.high, c.latestEpoch, c.lease.promisedBy, c.lease.promisedTo, c.policy.excluded, c.suspected, c.lease.readUntil)
			}
		}
		s.t.Fatalf("reply for %v = %q (present %v), want %q", id, value, ok, want)
	}
}

func TestLeasesResumeAfterLeaderAndMaximumCrashes(t *testing.T) {
	for _, n := range []int{5, 9, 13} {
		t.Run(fmt.Sprint(n), func(t *testing.T) {
			s := cluster(t, n)
			s.advance(time.Second)
			first := s.request(0, state.PUT, 7, "before")
			s.advance(time.Second)
			s.expectReply(first, "before")
			s.alive[0] = false
			s.advance(11 * time.Second)
			second := s.request(1, state.PUT, 7, "after-leader")
			s.advance(time.Second)
			s.expectReply(second, "after-leader")
			read := s.request(2, state.GET, 7, "")
			s.advance(time.Second)
			s.expectReply(read, "after-leader")
			if s.cores[2].localReads == 0 {
				t.Fatal("surviving holder did not resume local reads after leader loss")
			}
			for id := 1; id < n/2; id++ {
				s.alive[id] = false
			}
			s.advance(11 * time.Second)
			leader := n / 2
			third := s.request(leader, state.PUT, 7, "after-maximum")
			s.advance(time.Second)
			s.expectReply(third, "after-maximum")
			// A read at a previously non-holding survivor is forwarded, counted,
			// and moves it into a quorum through a normal periodic configuration.
			reader := n - 1
			read = s.request(reader, state.GET, 7, "")
			s.advance(time.Second)
			s.expectReply(read, "after-maximum")
			s.advance(14 * time.Second)
			before := s.cores[reader].localReads
			read = s.request(reader, state.GET, 7, "")
			s.advance(time.Second)
			s.expectReply(read, "after-maximum")
			if s.cores[reader].localReads <= before {
				t.Fatal("local reads never resumed under replacement leases")
			}
			for id, c := range s.cores {
				if s.alive[id] && string(s.stores[id][7]) != "after-maximum" {
					t.Fatalf("survivor %d has stale state", id)
				}
				if s.alive[id] && c.configLog.executed < 0 {
					t.Fatalf("survivor %d never learned Lease-Paxos changes", id)
				}
			}
		})
	}
}

func TestHolderFailureWithoutLeaderFailure(t *testing.T) {
	s := cluster(t, 5)
	s.advance(time.Second)
	s.alive[1] = false
	write := s.request(0, state.PUT, 7, "waiting")
	s.advance(time.Second)
	if _, ok := s.replies[write]; ok {
		t.Fatal("write committed without notifying a binding holder")
	}
	s.advance(10 * time.Second)
	s.expectReply(write, "waiting")
	read := s.request(2, state.GET, 7, "")
	s.advance(time.Second)
	s.expectReply(read, "waiting")
	if s.cores[2].localReads == 0 {
		t.Fatal("leases were lost permanently after non-leader holder failure")
	}
}

func TestLostConfigurationCommitIsFetchedBeforeActivation(t *testing.T) {
	s := cluster(t, 5)
	s.advance(time.Second)
	s.drop = func(p *Packet, to int) bool { return p.Control && p.Kind == packetCommit && to == 2 }
	s.cores[0].proposeConfiguration(&LeaseChange{Leader: 0, Updates: []HolderUpdate{{Key: 8, Holders: 0b10101}}}, s.now)
	s.advance(time.Second)
	if s.cores[2].latestEpoch != genesisLeaseEpoch {
		t.Fatal("dropped configuration was treated as committed")
	}
	s.drop = nil
	s.advance(6 * time.Second)
	if s.cores[2].latestEpoch <= genesisLeaseEpoch || s.cores[2].lease.promisedTo <= genesisLeaseEpoch {
		t.Fatal("configuration catch-up failed to restore lease activation")
	}
}

func TestConfigurationLogRecoversAcceptedChangeAfterLeaderCrash(t *testing.T) {
	s := cluster(t, 5)
	s.advance(time.Second)
	s.drop = func(p *Packet, to int) bool { return p.Control && p.Kind == packetAck && to == 0 }
	s.cores[0].proposeConfiguration(&LeaseChange{Leader: 0, Updates: []HolderUpdate{{Key: 8, Holders: 0b11001}}}, s.now)
	s.advance(100 * time.Millisecond)
	if s.cores[0].latestEpoch != genesisLeaseEpoch {
		t.Fatal("configuration committed without a majority")
	}
	s.alive[0] = false
	s.drop = nil
	s.advance(12 * time.Second)
	for id := 1; id < 5; id++ {
		c := s.cores[id]
		if c.policies[c.latestEpoch].holderMask(8) != 0b11000 {
			t.Fatalf("replica %d lost recovered configuration: holders=%05b", id, c.policies[c.latestEpoch].holderMask(8))
		}
	}
	write := s.request(1, state.PUT, 8, "recovered")
	s.advance(time.Second)
	s.expectReply(write, "recovered")
	before := s.cores[3].localReads
	read := s.request(3, state.GET, 8, "")
	s.advance(time.Second)
	s.expectReply(read, "recovered")
	if s.cores[3].localReads <= before {
		t.Fatal("recovered holder was not allowed to read locally")
	}
}

func TestIsolatedHolderCannotReadStaleAfterExclusion(t *testing.T) {
	s := cluster(t, 5)
	s.advance(time.Second)
	write := s.request(0, state.PUT, 7, "old")
	s.advance(time.Second)
	s.expectReply(write, "old")
	// Keep the holder process alive while dropping its peer traffic. Failure
	// must be inferred from missing promise acknowledgements, not a crash flag.
	s.drop = func(p *Packet, to int) bool { return p.From == 1 || to == 1 }
	s.advance(11 * time.Second)
	write = s.request(0, state.PUT, 7, "new")
	s.advance(time.Second)
	s.expectReply(write, "new")
	read := s.request(1, state.GET, 7, "")
	s.advance(time.Second)
	if _, ok := s.replies[read]; ok {
		t.Fatal("isolated holder answered a stale read after its old lease expired")
	}
	if string(s.stores[1][7]) != "old" {
		t.Fatal("test did not leave the isolated holder stale")
	}
}
