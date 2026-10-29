package pqlfrontier

import (
	"bytes"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/hongzicong/ConsensusArena/replica/defs"
	"github.com/hongzicong/ConsensusArena/state"
)

type delivery struct {
	to  int
	at  time.Time
	msg *Packet
}

type operation struct {
	id         defs.RequestID
	write      bool
	value      string
	start, end time.Time
}

type clientDelivery struct {
	at    time.Time
	id    defs.RequestID
	value string
}

type simulation struct {
	t       *testing.T
	now     time.Time
	cores   []*core
	alive   []bool
	stores  []map[state.Key]state.Value
	queue   []delivery
	clients []clientDelivery
	history []*operation
	ops     map[defs.RequestID]*operation
	drop    func(*Packet, int) bool
}

func newSimulation(t *testing.T, n int) *simulation {
	t.Helper()
	s := &simulation{t: t, now: time.Unix(100, 0), cores: make([]*core, n), alive: make([]bool, n),
		stores: make([]map[state.Key]state.Value, n), ops: map[defs.RequestID]*operation{}}
	mask := uint64(1)<<(n/2+1) - 1
	for id := range s.cores {
		s.alive[id] = true
		s.stores[id] = map[state.Key]state.Value{42: state.Value("0")}
		i := id
		policy := &leasePolicy{n: n, leader: 0, holders: map[state.Key]uint64{42: mask}, defaultMask: mask}
		c := newCore(n, int32(id), 0, policy, func(cmd state.Command) state.Value {
			if cmd.Op == state.PUT {
				s.stores[i][cmd.K] = append(state.Value(nil), cmd.V...)
			}
			return append(state.Value(nil), s.stores[i][cmd.K]...)
		})
		c.send = func(to int32, p *Packet) { s.send(int(to), p) }
		c.broadcast = func(p *Packet) {
			for to := range s.cores {
				s.send(to, p)
			}
		}
		c.sendMask = func(mask uint64, p *Packet) {
			for to := range s.cores {
				if mask&(uint64(1)<<uint(to)) != 0 {
					s.send(to, p)
				}
			}
		}
		c.reply = func(r Request, value state.Value) {
			// Replica execution precedes client receipt. Use independent reply
			// delays so the history checker cannot assume they coincide.
			s.clients = append(s.clients, clientDelivery{at: s.now.Add(time.Duration(3+i%4) * time.Millisecond), id: r.ID, value: string(value)})
		}
		s.cores[id] = c
	}
	return s
}

func (s *simulation) send(to int, p *Packet) {
	if !s.alive[to] || !s.alive[p.From] || (s.drop != nil && s.drop(p, to)) {
		return
	}
	// Exercise real stream encoding and validation, not shared packet pointers.
	var frame bytes.Buffer
	p.Marshal(&frame)
	clone := &Packet{}
	if err := clone.Unmarshal(&frame); err != nil {
		s.t.Fatal(err)
	}
	delay := time.Duration(2+(int(p.From)+to)%5) * time.Millisecond
	s.queue = append(s.queue, delivery{to: to, at: s.now.Add(delay), msg: clone})
}

func (s *simulation) advance(duration time.Duration) {
	for end := s.now.Add(duration); s.now.Before(end); {
		s.now = s.now.Add(2 * time.Millisecond)
		for id, c := range s.cores {
			if s.alive[id] {
				c.tick(s.now, s.alive)
			}
		}
		queue := s.queue
		s.queue = nil
		for _, d := range queue {
			if !s.alive[d.to] || !s.alive[d.msg.From] {
				continue
			}
			if d.at.After(s.now) {
				s.queue = append(s.queue, d)
			} else {
				s.cores[d.to].receive(d.msg, s.now)
			}
		}
		clients := s.clients
		s.clients = nil
		for _, d := range clients {
			if d.at.After(s.now) {
				s.clients = append(s.clients, d)
			} else if op := s.ops[d.id]; op != nil && op.end.IsZero() {
				op.end = s.now
				if op.write && op.value != d.value {
					s.t.Fatalf("write reply returned unexpected value %q, want %q", d.value, op.value)
				}
				op.value = d.value
			}
		}
	}
}

func (s *simulation) request(ingress int, write bool, value string) *operation {
	id := defs.RequestID{Client: int32(20 + ingress), Sequence: int32(len(s.history) + 1)}
	op := &operation{id: id, write: write, value: value, start: s.now}
	s.history = append(s.history, op)
	s.ops[id] = op
	command := state.Command{Op: state.GET, K: 42}
	if write {
		command.Op, command.V = state.PUT, state.Value(value)
	}
	s.cores[ingress].submit(Request{ID: id, Origin: int32(ingress), Command: command}, s.now)
	return op
}

// Exact single-register history search for these bounded histories. Each chosen
// order must respect client invocation/receipt intervals and every read's value.
// Pending writes may complete in an extension; pending reads are omitted.
func (s *simulation) checkLinearizable() {
	s.t.Helper()
	var ops []*operation
	for _, op := range s.history {
		if !op.end.IsZero() || op.write {
			ops = append(ops, op)
		}
	}
	if len(ops) > 63 {
		s.t.Fatal("bounded history checker supports at most 63 operations")
	}
	predecessors := make([]uint64, len(ops))
	for i, op := range ops {
		for j, before := range ops {
			if i != j && !before.end.IsZero() && !before.end.After(op.start) {
				predecessors[i] |= uint64(1) << j
			}
		}
	}
	type searchState struct {
		done  uint64
		value string
	}
	failed := map[searchState]bool{}
	all := uint64(1)<<len(ops) - 1
	var visit func(uint64, string) bool
	visit = func(done uint64, value string) bool {
		if done == all {
			return true
		}
		key := searchState{done, value}
		if failed[key] {
			return false
		}
		for i, op := range ops {
			bit := uint64(1) << i
			if done&bit != 0 || predecessors[i]&^done != 0 {
				continue
			}
			next := value
			if op.write {
				next = op.value
			} else if op.value != value {
				continue
			}
			if visit(done|bit, next) {
				return true
			}
		}
		failed[key] = true
		return false
	}
	if !visit(0, "0") {
		s.t.Fatal("client-visible history has no legal linearization")
	}
}

func TestClusterContinuousWritesHaveLinearizableReads(t *testing.T) {
	for _, n := range []int{3, 5, 7, 9, 13} {
		t.Run(fmt.Sprint(n), func(t *testing.T) {
			s := newSimulation(t, n)
			s.advance(800 * time.Millisecond)
			for i := 1; i <= 12; i++ {
				s.request(0, true, strconv.Itoa(i))
				s.advance(6 * time.Millisecond)
				s.request(1, false, "")
				s.advance(2 * time.Millisecond)
			}
			s.advance(300 * time.Millisecond)
			for _, op := range s.history {
				if op.end.IsZero() {
					t.Fatalf("request did not finish: %+v", op)
				}
			}
			if s.cores[1].frontierConcurrentReleases == 0 {
				t.Fatal("schedule did not exercise a read released with newer writes pending")
			}
			s.checkLinearizable()
		})
	}
}

func TestClusterFollowerWaitsForDelayedCommitDespiteLeaderReply(t *testing.T) {
	s := newSimulation(t, 5)
	s.advance(800 * time.Millisecond)
	s.drop = func(p *Packet, to int) bool { return p.Kind == packetCommit && !p.Control && to == 1 }
	w := s.request(0, true, "1")
	s.advance(60 * time.Millisecond)
	if w.end.IsZero() {
		t.Fatal("setup: leader write reply did not reach the client")
	}
	r := s.request(1, false, "")
	s.advance(60 * time.Millisecond)
	if !r.end.IsZero() {
		t.Fatal("follower used leader/client completion instead of its own execution")
	}
	s.drop = nil
	s.advance(400 * time.Millisecond)
	if r.end.IsZero() || r.value != "1" {
		t.Fatalf("read did not finish after catch-up: %+v", r)
	}
	s.checkLinearizable()
}

func TestClusterPendingReadSurvivesLeaderRecovery(t *testing.T) {
	s := newSimulation(t, 5)
	s.advance(800 * time.Millisecond)
	s.drop = func(p *Packet, to int) bool { return p.Kind == packetAck && !p.Control && to == 0 && p.From != 0 }
	s.request(0, true, "1")
	s.advance(20 * time.Millisecond)
	r := s.request(1, false, "")
	if len(s.cores[1].readFrontiers) != 1 {
		t.Fatal("setup: read did not capture the old ballot's pending write")
	}
	s.drop = nil
	s.cores[1].Begin(s.now)
	s.advance(800 * time.Millisecond)
	if r.end.IsZero() || r.value != "1" || s.cores[1].frontierInvalidations == 0 {
		for id, c := range s.cores {
			t.Logf("replica=%d alive=%t high=%d executed=%d lease=%t catchup=%d grant=%d read=%d config=%d excluded=%b localqueue=%d frontiers=%v configleader=%d configactive=%t", id, s.alive[id], c.high, c.executed, c.lease.canRead(s.now, c.executed), c.lease.catchup, c.lease.promisedBy, c.lease.promisedTo, c.latestEpoch, c.policy.excluded, len(c.localPending), c.readFrontiers, c.configLog.leader, c.configLog.active)
		}
		t.Fatalf("pending read failed after leader replacement: read=%+v active=%t preparing=%t ballot=%d executed=%d epoch=%d pending=%d", r, s.cores[1].active, s.cores[1].preparing, s.cores[1].ballot, s.cores[1].executed, s.cores[1].lease.promisedTo, len(s.cores[1].pending))
	}
	next := s.request(2, false, "")
	s.advance(time.Second)
	if next.end.IsZero() || next.value != "1" {
		t.Fatalf("later surviving holder returned stale state: %+v", next)
	}
	s.checkLinearizable()
}

func TestClusterCrashKeepsReadBlockedWithoutLeaseAuthority(t *testing.T) {
	s := newSimulation(t, 5)
	s.advance(800 * time.Millisecond)
	s.drop = func(p *Packet, to int) bool { return p.Kind == packetAck && !p.Control && to == 0 && p.From != 0 }
	s.request(0, true, "1")
	s.advance(20 * time.Millisecond)
	r := s.request(1, false, "")
	s.alive[0] = false
	s.drop = nil
	s.cores[1].Begin(s.now)
	s.advance(12 * time.Second)
	if s.cores[1].frontierInvalidations == 0 {
		t.Fatal("crash/recovery reused the old ballot's read frontier")
	}
	if r.end.IsZero() {
		// This schedule also stalls the unmodified parent: a configuration
		// pause can make healthy peers look unresponsive to lease management.
		// Characterize safe blocking; do not claim that limitation repaired.
		if s.cores[1].lease.canRead(s.now, s.cores[1].executed) {
			t.Fatal("read still blocked despite usable authority")
		}
	} else if r.value != "1" {
		t.Fatalf("read returned stale or speculative state after crash: %+v", r)
	}
	s.checkLinearizable()
}
