package pql

import (
	"testing"
	"time"

	"github.com/hongzicong/ConsensusArena/replica/defs"
	"github.com/hongzicong/ConsensusArena/state"
)

func testPolicy() *leasePolicy {
	return &leasePolicy{n: 5, leader: 0, holders: map[state.Key]uint64{7: 0b00111}, defaultMask: 0b11001}
}

func testCore(id int32, execute func(state.Command) state.Value) *core {
	c := newCore(5, id, 0, testPolicy(), execute)
	c.send = func(int32, *Packet) {}
	c.broadcast = func(*Packet) {}
	c.sendMask = func(uint64, *Packet) {}
	c.reply = func(Request, state.Value) {}
	return c
}

func TestWriteUsesExactlyConfiguredMajority(t *testing.T) {
	c := testCore(0, func(state.Command) state.Value { return nil })
	var gotMask uint64
	var got *Packet
	c.sendMask = func(mask uint64, p *Packet) { gotMask, got = mask, p }
	r := Request{ID: defs.RequestID{Client: 4, Sequence: 9}, Origin: 0,
		Command: state.Command{Op: state.PUT, K: 7, V: state.Value("v")}}
	c.submit(r, time.Unix(1, 0))
	c.flush(time.Unix(1, 0))
	if gotMask != 0b00111 || bitCount(gotMask) != 3 {
		t.Fatalf("write mask = %05b; want configured three-replica majority", gotMask)
	}
	if got == nil || got.LeaseEpoch != genesisLeaseEpoch || len(got.Records) != 1 {
		t.Fatalf("unexpected accept packet: %#v", got)
	}
}

func TestLeaseRequiresCatchUpAndExpires(t *testing.T) {
	now := time.Unix(10, 0)
	l := newLeaseState(5, 2)
	l.promisedTo = genesisLeaseEpoch
	l.latestPromises[0], l.latestPromises[1] = now.Add(time.Second), now.Add(1500*time.Millisecond)
	l.frontiers[0], l.frontiers[1] = 8, 6
	l.recomputeReadUntil(now)
	if l.canRead(now, 7) {
		t.Fatal("lease allowed a read before the maximum promise frontier")
	}
	if !l.canRead(now, 8) {
		t.Fatal("caught-up holder did not acquire a majority lease")
	}
	if l.canRead(now.Add(1100*time.Millisecond), 8) {
		t.Fatal("expired majority lease allowed a read")
	}
}

func TestAcceptedWriteBlocksLocalReadUntilExecution(t *testing.T) {
	now := time.Unix(20, 0)
	executions := 0
	c := testCore(1, func(cmd state.Command) state.Value { executions++; return state.Value("value") })
	c.lease.promisedTo = genesisLeaseEpoch
	c.lease.latestPromises[0], c.lease.latestPromises[2] = now.Add(time.Second), now.Add(time.Second)
	c.lease.frontiers[0], c.lease.frontiers[2] = -1, -1
	c.lease.recomputeReadUntil(now)
	c.updating[7] = 1
	forwarded := 0
	c.send = func(to int32, p *Packet) {
		if to == 0 && p.Kind == packetForward {
			forwarded++
		}
	}
	c.submit(Request{ID: defs.RequestID{Client: 1, Sequence: 1}, Origin: 1,
		Command: state.Command{Op: state.GET, K: 7}}, now)
	if executions != 0 || forwarded != 0 || len(c.localPending) != 1 {
		t.Fatalf("holder did not wait for its accepted write: executions=%d forwards=%d queued=%d", executions, forwarded, len(c.localPending))
	}
	delete(c.updating, 7)
	c.submit(Request{ID: defs.RequestID{Client: 1, Sequence: 2}, Origin: 1,
		Command: state.Command{Op: state.GET, K: 7}}, now)
	if executions != 1 || c.localReads != 1 {
		t.Fatalf("eligible local read executions=%d local_reads=%d; want 1,1", executions, c.localReads)
	}
}

func TestConfiguredQuorumCommitsAndAllReplicasExecute(t *testing.T) {
	policy := testPolicy()
	cores := make([]*core, 5)
	stores := make([]map[state.Key]state.Value, 5)
	replies := 0
	for id := range cores {
		stores[id] = make(map[state.Key]state.Value)
		i := id
		cores[id] = newCore(5, int32(id), 0, policy, func(cmd state.Command) state.Value {
			if cmd.Op == state.PUT {
				stores[i][cmd.K] = append(state.Value(nil), cmd.V...)
			}
			return append(state.Value(nil), stores[i][cmd.K]...)
		})
		cores[id].reply = func(Request, state.Value) { replies++ }
	}
	now := time.Unix(30, 0)
	for from := range cores {
		sender := cores[from]
		sender.send = func(to int32, p *Packet) { cores[to].receive(p, now) }
		sender.broadcast = func(p *Packet) {
			for _, receiver := range cores {
				receiver.receive(p, now)
			}
		}
		sender.sendMask = func(mask uint64, p *Packet) {
			for to := range cores {
				if mask&(uint64(1)<<uint(to)) != 0 {
					cores[to].receive(p, now)
				}
			}
		}
	}
	r := Request{ID: defs.RequestID{Client: 3, Sequence: 4}, Origin: 0,
		Command: state.Command{Op: state.PUT, K: 7, V: state.Value("committed")}}
	cores[0].submit(r, now)
	cores[0].flush(now)
	if replies != 1 {
		t.Fatalf("client replies = %d; want exactly one", replies)
	}
	for id, c := range cores {
		if c.executed != 0 || string(stores[id][7]) != "committed" {
			t.Fatalf("replica %d executed=%d value=%q", id, c.executed, stores[id][7])
		}
	}
}

func TestFailoverAcceptWaitsForOutstandingPromise(t *testing.T) {
	now := time.Unix(40, 0)
	c := testCore(1, func(state.Command) state.Value { return nil })
	c.leader, c.ballot = 1, 6
	c.policies[6] = c.policy
	c.lease.writeUntil = now.Add(time.Second)
	acks := 0
	c.send = func(to int32, p *Packet) {
		if to == 1 && p.Kind == packetAck {
			acks++
		}
	}
	p := &Packet{Kind: packetAccept, From: 1, Ballot: 6, LeaseEpoch: 6,
		Records: []Record{{Slot: 0, Request: Request{ID: defs.RequestID{Client: 2, Sequence: 3},
			Command: state.Command{Op: state.PUT, K: 7}}}}}
	c.accept(p, now)
	if acks != 0 || c.leaseRejects != 1 {
		t.Fatal("new configuration bypassed an outstanding promise")
	}
	c.accept(p, now.Add(2*time.Second))
	if acks != 1 {
		t.Fatalf("accept was not released after promise expiry: acks=%d", acks)
	}
}

func TestRecoveryReplacesUnselectedLocalSuffix(t *testing.T) {
	now := time.Unix(50, 0)
	c := testCore(1, func(state.Command) state.Value { return nil })
	c.install(Record{Slot: 0, Request: Request{ID: defs.RequestID{Client: 9, Sequence: 1}, Command: state.Command{Op: state.PUT, K: 7}}})
	c.Begin(now)
	for _, from := range []int32{2, 3, 4} {
		c.receive(&Packet{Kind: packetPromise, From: from, Ballot: c.ballot, Floor: 0, High: -1, Pages: 1}, now)
	}
	if c.high != -1 || len(c.log) != 0 || len(c.updating) != 0 || len(c.assigned) != 0 {
		t.Fatalf("unselected suffix survived phase one: high=%d log=%d updating=%v assigned=%v", c.high, len(c.log), c.updating, c.assigned)
	}
}
