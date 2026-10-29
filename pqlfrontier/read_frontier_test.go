package pqlfrontier

import (
	"bytes"
	"fmt"
	"testing"
	"time"

	"github.com/hongzicong/ConsensusArena/replica/defs"
	"github.com/hongzicong/ConsensusArena/state"
)

type readFixture struct {
	c       *core
	now     time.Time
	store   map[state.Key]state.Value
	replies map[defs.RequestID][]state.Value
	sent    []Packet
}

func readerFixture(n int, id int32) *readFixture {
	f := &readFixture{now: time.Unix(100, 0), store: map[state.Key]state.Value{42: state.Value("initial")}, replies: map[defs.RequestID][]state.Value{}}
	mask := uint64(1)<<(n/2+1) - 1
	p := &leasePolicy{n: n, leader: 0, holders: map[state.Key]uint64{42: mask}, defaultMask: mask}
	f.c = newCore(n, id, 0, p, func(cmd state.Command) state.Value {
		if cmd.Op == state.PUT {
			f.store[cmd.K] = append(state.Value(nil), cmd.V...)
		}
		return append(state.Value(nil), f.store[cmd.K]...)
	})
	f.c.lease.promisedTo = genesisLeaseEpoch
	f.c.lease.readUntil = f.now.Add(2 * time.Second)
	f.c.lease.catchup = -1
	f.c.send = func(_ int32, p *Packet) { f.sent = append(f.sent, *p) }
	f.c.broadcast = func(*Packet) {}
	f.c.sendMask = func(uint64, *Packet) {}
	f.c.reply = func(r Request, v state.Value) {
		f.replies[r.ID] = append(f.replies[r.ID], append(state.Value(nil), v...))
	}
	return f
}

func (f *readFixture) read(sequence int32) Request {
	r := Request{ID: defs.RequestID{Client: 2, Sequence: sequence}, Origin: f.c.id, Command: state.Command{Op: state.GET, K: 42}}
	f.c.submit(r, f.now)
	return r
}

func (f *readFixture) accept(slot int64, value string) {
	r := Record{Slot: slot, Ballot: f.c.ballot, LeaseEpoch: f.c.lease.promisedBy,
		Request: Request{ID: defs.RequestID{Client: 1, Sequence: int32(slot)}, Origin: -2,
			Command: state.Command{Op: state.PUT, K: 42, V: state.Value(value)}}}
	f.c.receive(&Packet{Kind: packetAccept, From: f.c.leader, Ballot: f.c.ballot, LeaseEpoch: f.c.lease.promisedBy, Records: []Record{r}}, f.now)
}

func (f *readFixture) commit(slot int64) {
	r := *f.c.log[slot]
	r.Committed = true
	f.c.receive(&Packet{Kind: packetCommit, From: f.c.leader, Ballot: f.c.ballot, Records: []Record{r}}, f.now)
}

func (f *readFixture) retry(r Request) { f.c.submit(r, f.now) }

func (f *readFixture) noReply(t *testing.T, r Request) {
	t.Helper()
	if len(f.replies[r.ID]) != 0 {
		t.Fatalf("unsafe/premature reply: %q", f.replies[r.ID])
	}
}

func (f *readFixture) expect(t *testing.T, r Request, value string) {
	t.Helper()
	got := f.replies[r.ID]
	if len(got) == 0 || string(got[len(got)-1]) != value {
		t.Fatalf("expected %q, got %q (executed=%d high=%d updating=%d)", value, got, f.c.executed, f.c.high, f.c.updating[42])
	}
}

func TestReadProgressesWhileNewerWritesRemainPending(t *testing.T) {
	for _, n := range []int{3, 5, 7, 9, 13} {
		t.Run(fmt.Sprint(n), func(t *testing.T) {
			f := readerFixture(n, 0)
			f.accept(0, "write-0")
			for slot := int64(0); slot < 100; slot++ {
				r := f.read(int32(slot))
				f.noReply(t, r)
				f.accept(slot+1, fmt.Sprintf("write-%d", slot+1))
				f.commit(slot)
				f.retry(r)
				f.expect(t, r, fmt.Sprintf("write-%d", slot))
				if f.c.updating[42] == 0 || len(f.c.readFrontiers) != 0 {
					t.Fatal("read must finish before key quiescence, without retaining its frontier")
				}
			}
			if f.c.frontierConcurrentReleases != 100 {
				t.Fatalf("expected 100 reads released during overlapping writes, got %d", f.c.frontierConcurrentReleases)
			}
		})
	}
}

func TestAcceptedButUnchosenValueCannotAnswerRead(t *testing.T) {
	f := readerFixture(5, 0)
	f.accept(0, "speculative")
	r := f.read(1)
	for i := 0; i < 20; i++ {
		f.retry(r)
	}
	f.noReply(t, r)
	if string(f.store[42]) != "initial" || f.c.frontierCaptures != 1 {
		t.Fatal("acceptance changed state or duplicate attempts extended the frontier")
	}
}

func TestLaterReadMustIncludeWriteObservedByEarlierRead(t *testing.T) {
	f := readerFixture(5, 0)
	f.accept(0, "one")
	r1 := f.read(1)
	f.accept(1, "two")
	f.commit(0)
	f.retry(r1)
	f.expect(t, r1, "one")
	r2 := f.read(2)
	f.noReply(t, r2)
	if f.c.readFrontiers[r2.ID].slot != 1 {
		t.Fatal("later read reused the earlier read's boundary")
	}
	f.accept(2, "three")
	f.commit(1)
	f.retry(r2)
	f.expect(t, r2, "two")
}

func TestReadReturnsCurrentExecutedStateNotAcceptedOrCapturedValue(t *testing.T) {
	f := readerFixture(5, 0)
	f.accept(0, "one")
	r := f.read(1)
	f.accept(1, "two")
	f.commit(0)
	f.commit(1)
	f.accept(2, "unchosen-three")
	f.retry(r)
	f.expect(t, r, "two")
}

func TestExecutionCannotSkipGapBeforeReadFrontier(t *testing.T) {
	f := readerFixture(5, 1)
	f.accept(1, "two")
	r := f.read(1)
	f.commit(1)
	f.retry(r)
	f.noReply(t, r)
	if f.c.executed != -1 {
		t.Fatal("executed through an unresolved prefix gap")
	}
	f.accept(0, "one")
	f.commit(0)
	f.accept(2, "pending-three")
	f.retry(r)
	f.expect(t, r, "two")
}

func TestLeaderExecutionDoesNotAuthorizeUnappliedFollowerRead(t *testing.T) {
	leader, follower := readerFixture(5, 0), readerFixture(5, 1)
	leader.accept(0, "new")
	follower.accept(0, "new")
	r := follower.read(1)
	leader.commit(0)
	follower.retry(r)
	follower.noReply(t, r)
	if string(leader.store[42]) != "new" || string(follower.store[42]) != "initial" {
		t.Fatal("setup does not distinguish leader from follower execution")
	}
	follower.accept(1, "later")
	follower.commit(0)
	follower.retry(r)
	follower.expect(t, r, "new")
}

func TestLeaseExpiryEpochAndBallotChangesInvalidateFrontier(t *testing.T) {
	for _, change := range []string{"expiry", "renewal-after-captured-expiry", "epoch", "ballot", "preparing", "inactive"} {
		t.Run(change, func(t *testing.T) {
			f := readerFixture(5, 0)
			f.accept(0, "one")
			r := f.read(1)
			f.accept(1, "two")
			f.commit(0)
			switch change {
			case "expiry", "renewal-after-captured-expiry":
				f.now = f.now.Add(3 * time.Second)
				if change == "renewal-after-captured-expiry" {
					f.c.lease.readUntil = f.now.Add(2 * time.Second)
				}
			case "epoch":
				f.c.lease.promisedTo++
				f.c.policies[f.c.lease.promisedTo] = f.c.policy
			case "ballot":
				f.c.ballot += int64(f.c.n)
			case "preparing":
				f.c.preparing = true
			case "inactive":
				f.c.active = false
			}
			f.retry(r)
			f.noReply(t, r)
			if f.c.frontierInvalidations != 1 {
				t.Fatal("old authority was reused")
			}
			f.c.preparing, f.c.active = false, true
			f.c.lease.readUntil = f.now.Add(2 * time.Second)
			f.retry(r)
			f.noReply(t, r)
			f.commit(1)
			f.retry(r)
			f.expect(t, r, "two")
		})
	}
}

func TestActivationCatchupStillBlocksCleanKey(t *testing.T) {
	f := readerFixture(5, 1)
	f.c.lease.catchup = 0
	r := f.read(1)
	f.noReply(t, r)
	f.accept(0, "before-activation")
	f.commit(0)
	f.retry(r)
	f.expect(t, r, "before-activation")
}

func TestRetryKeepsFrozenFrontierAndOriginalCompletedResult(t *testing.T) {
	f := readerFixture(5, 0)
	f.accept(0, "one")
	r := f.read(1)
	f.accept(1, "two")
	f.retry(r)
	if f.c.readFrontiers[r.ID].slot != 0 || f.c.frontierCaptures != 1 {
		t.Fatal("duplicate request extended its wait")
	}
	f.commit(0)
	f.retry(r)
	f.expect(t, r, "one")
	f.commit(1)
	f.retry(r)
	f.expect(t, r, "one")
}

func TestNonHolderForwardsAndDropsObsoleteFrontier(t *testing.T) {
	f := readerFixture(5, 1)
	f.accept(0, "one")
	r := f.read(1)
	f.c.policy.excluded = uint64(1) << 1
	f.retry(r)
	f.noReply(t, r)
	if len(f.c.readFrontiers) != 0 || len(f.sent) == 0 || f.sent[len(f.sent)-1].Kind != packetForward {
		t.Fatal("excluded ingress retained a local frontier instead of forwarding")
	}
}

func TestWriteStillRequiresMajorityAndEveryHolderAck(t *testing.T) {
	f := readerFixture(5, 0)
	f.c.policy.holders[42] = 0b01111 // Four holders, more than the three-vote majority.
	f.c.send = func(to int32, p *Packet) {
		if to == 0 {
			f.c.receive(p, f.now)
		}
	}
	f.c.sendMask = func(mask uint64, p *Packet) {
		if mask&1 != 0 {
			f.c.receive(p, f.now)
		}
	}
	w := Request{ID: defs.RequestID{Client: 4, Sequence: 1}, Origin: 0, Command: state.Command{Op: state.PUT, K: 42, V: state.Value("chosen")}}
	f.c.submit(w, f.now)
	f.c.flush(f.now)
	ack := func(id int32) {
		f.c.receive(&Packet{Kind: packetAck, From: id, Ballot: f.c.ballot, LeaseEpoch: genesisLeaseEpoch, Records: []Record{{Slot: 0}}}, f.now)
	}
	ack(1)
	ack(2)
	ack(2)
	f.noReply(t, w)
	if f.c.executed != -1 {
		t.Fatal("majority or duplicate ACK bypassed required holder coverage")
	}
	ack(3)
	f.expect(t, w, "chosen")
}

func TestWireFramingRemainsCompatible(t *testing.T) {
	in := &Packet{Kind: packetAccept, From: 0, LeaseEpoch: 1, Records: []Record{{Slot: 0, Request: Request{Command: state.Command{Op: state.PUT, K: 42, V: state.Value("wire")}}}}}
	var b bytes.Buffer
	in.Marshal(&b)
	out := &Packet{}
	if err := out.Unmarshal(&b); err != nil || string(out.Records[0].Request.Command.V) != "wire" {
		t.Fatalf("wire round trip failed: %v", err)
	}
}
