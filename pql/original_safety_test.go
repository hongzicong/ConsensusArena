package pql

import (
	"fmt"
	"testing"
	"time"

	"github.com/hongzicong/ConsensusArena/replica/defs"
	"github.com/hongzicong/ConsensusArena/state"
)

// Counterparts to the original-handler characterizations in
// report/analysis/pql-review/*_source_test.go. Those reproduce defects in the
// authors' checkout; these assert that the Arena implementation avoids them.
func TestOriginalSafetyLocalReadReturnsApplicationValue(t *testing.T) {
	now := time.Unix(600, 0)
	store := state.InitState()
	initial := state.Command{Op: state.PUT, K: 7, V: state.Value("42")}
	initial.Execute(store)
	c := testCore(1, func(cmd state.Command) state.Value { return cmd.Execute(store) })
	c.lease.promisedTo = genesisLeaseEpoch
	c.lease.latestPromises[0], c.lease.latestPromises[2] = now.Add(time.Second), now.Add(time.Second)
	c.lease.recomputeReadUntil(now)
	var result state.Value
	c.reply = func(_ Request, v state.Value) { result = v }
	c.submit(Request{ID: defs.RequestID{Client: 1, Sequence: 1}, Origin: 1,
		Command: state.Command{Op: state.GET, K: 7}}, now)
	if string(result) != "42" || c.localReads != 1 {
		t.Fatalf("local application read returned %q; want 42", result)
	}
}

func TestOriginalSafetyObservedHoldersFormDistinctMajority(t *testing.T) {
	for _, n := range []int{5, 9, 13} {
		t.Run(fmt.Sprint(n), func(t *testing.T) {
			s := cluster(t, n)
			c := s.cores[0]
			c.readStats[7] = make([]uint64, n)
			c.readStats[7][n-1] = 10
			c.readStats[7][n-2] = 9
			c.proposeObservedHolders(s.now)
			s.advance(5 * time.Second)
			mask := c.policy.holderMask(7)
			if bitCount(mask) != n/2+1 || mask&1 == 0 {
				t.Fatalf("holder mask=%b distinct=%d required=%d", mask, bitCount(mask), n/2+1)
			}
		})
	}
}

func TestOriginalSafetyPrepareFencesExistingSlotInBothLogs(t *testing.T) {
	for _, control := range []bool{false, true} {
		t.Run(fmt.Sprint(control), func(t *testing.T) {
			c := testCore(0, func(state.Command) state.Value { return nil })
			if control {
				c = c.configLog
			}
			now := time.Unix(610, 0)
			c.install(Record{Slot: 0, Ballot: 0, Request: Request{ID: defs.RequestID{Client: 1},
				Command: state.Command{Op: state.PUT, K: 7, V: state.Value("old")}}})
			c.receive(&Packet{Kind: packetPrepare, From: 2, Ballot: 12, Floor: 0}, now)
			c.receive(&Packet{Kind: packetAccept, From: 1, Ballot: 6, LeaseEpoch: genesisLeaseEpoch,
				Records: []Record{{Slot: 0, Request: Request{ID: defs.RequestID{Client: 2},
					Command: state.Command{Op: state.PUT, K: 7, V: state.Value("forbidden")}}}}}, now)
			if c.ballot != 12 || string(c.log[0].Request.Command.V) != "old" {
				t.Fatal("old-ballot Accept bypassed the higher prepare promise")
			}
		})
	}
}

func TestOriginalSafetyPartialDeliveryCannotLowerCommitThreshold(t *testing.T) {
	s := cluster(t, 5)
	s.advance(time.Second)
	// Only the leader and one remote acceptor receive the write. No count
	// returned by a transport send is used to lower the required majority.
	s.drop = func(p *Packet, to int) bool { return !p.Control && p.Kind == packetAccept && to != 1 }
	w := s.request(0, state.PUT, 7, "unchosen")
	s.advance(time.Second)
	if _, replied := s.replies[w]; replied || s.cores[0].log[0].Committed {
		t.Fatal("partial delivery committed without a Paxos majority and all holders")
	}
	s.drop = nil
	s.advance(time.Second)
	s.expectReply(w, "unchosen")
}

func TestOriginalSafetyRepeatedGrantorCannotSkipActivationFrontier(t *testing.T) {
	now := time.Unix(620, 0)
	l := newLeaseState(5, 0)
	l.promisedTo = genesisLeaseEpoch
	l.latestPromises[1], l.frontiers[1] = now.Add(time.Second), -1
	l.recomputeReadUntil(now)
	l.recomputeReadUntil(now.Add(time.Millisecond)) // another promise from the same grantor
	if l.activationComplete {
		t.Fatal("one grantor was counted twice toward activation")
	}
	l.latestPromises[2], l.frontiers[2] = now.Add(time.Second), 5
	l.recomputeReadUntil(now.Add(2 * time.Millisecond))
	if l.catchup != 5 || l.canRead(now, -1) || !l.canRead(now, 5) {
		t.Fatal("second distinct grantor's accepted frontier was not enforced")
	}
}
