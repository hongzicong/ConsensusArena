package pql

import (
	"testing"
	"time"

	"github.com/hongzicong/ConsensusArena/replica/defs"
	"github.com/hongzicong/ConsensusArena/state"
)

func TestIdentityAndRoutingNegativeClientReceivesInitialWriteReply(t *testing.T) {
	s := cluster(t, 5)
	s.advance(time.Second)
	id := defs.RequestID{Client: -123, Sequence: 1}
	s.cores[0].submit(Request{ID: id, Origin: 0, Command: state.Command{Op: state.PUT, K: 7, V: state.Value("written")}}, s.now)
	s.advance(time.Second)
	if _, ok := s.replies[id]; !ok {
		t.Fatalf("valid signed client ID received no initial reply: executed=%d value=%q", s.cores[0].executed, s.stores[0][7])
	}
}

func TestIdentityAndRoutingClientIDMustNotCollideWithRecoveryNoop(t *testing.T) {
	s := cluster(t, 5)
	s.advance(time.Second)
	// Resolve one empty slot through ordinary Paxos, as catch-up now does.
	s.cores[0].catchupTarget = 0
	s.advance(time.Second)
	if s.cores[0].executed != 0 || s.cores[0].log[0].Request.Command.Op != state.NONE {
		t.Fatal("setup: recovery no-op did not execute")
	}
	id := defs.RequestID{Client: -1, Sequence: 0}
	s.cores[0].submit(Request{ID: id, Origin: 0, Command: state.Command{Op: state.PUT, K: 7, V: state.Value("must-execute")}}, s.now)
	s.advance(time.Second)
	if _, ok := s.replies[id]; !ok || string(s.stores[0][7]) != "must-execute" {
		t.Fatalf("client command collided with internal no-op: reply_present=%v actual_state=%q", ok, s.stores[0][7])
	}
}

func TestIdentityAndRoutingExcludedLiveIngressMustNotStrandRead(t *testing.T) {
	s := cluster(t, 5)
	s.advance(time.Second)
	s.drop = func(p *Packet, to int) bool {
		return (p.From == 1 || to == 1) && p.Kind >= packetGuard && p.Kind <= packetLeaseReply
	}
	s.advance(10 * time.Second)
	s.drop = nil
	s.advance(5 * time.Second)
	w := s.request(0, state.PUT, 7, "current")
	s.advance(time.Second)
	s.expectReply(w, "current")
	r := s.request(1, state.GET, 7, "")
	s.advance(15 * time.Second)
	if _, ok := s.replies[r]; !ok {
		c := s.cores[1]
		t.Fatalf("live excluded ingress strands read after all links healed: grant_epoch=%d read_epoch=%d excludes=%05b pending=%d forwards=%d",
			c.lease.promisedBy, c.lease.promisedTo, c.policy.excluded, len(c.localPending), c.forwardedReads)
	}
}
