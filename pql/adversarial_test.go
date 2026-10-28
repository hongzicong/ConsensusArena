package pql

import (
	"bytes"
	"container/heap"
	"fmt"
	"math/rand"
	"strconv"
	"testing"
	"time"

	"github.com/hongzicong/ConsensusArena/replica/defs"
	"github.com/hongzicong/ConsensusArena/state"
)

// Override only delivery scheduling. Every remote packet still takes the real
// protobuf encode/decode path used by the deterministic cluster driver.
func adversarialNetwork(s *testCluster, rng *rand.Rand, delay func() time.Duration, lose, duplicate int) {
	deliver := func(to int, p *Packet) {
		if to == int(p.From) {
			s.cores[to].receive(p, s.now)
			return
		}
		if rng.Intn(100) < lose {
			return
		}
		frame, err := p.MarshalProtoFrame(0, false)
		if err != nil {
			s.t.Fatal(err)
		}
		copies := 1
		if rng.Intn(100) < duplicate {
			copies++
		}
		for i := 0; i < copies; i++ {
			var cp Packet
			if err := cp.Unmarshal(bytes.NewReader(frame)); err != nil {
				s.t.Fatal(err)
			}
			s.serial++
			heap.Push(&s.queue, delivery{at: s.now.Add(delay()), serial: s.serial, to: to, packet: &cp})
		}
	}
	for id, c := range s.cores {
		from := id
		c.send = func(to int32, p *Packet) { deliver(int(to), p) }
		c.broadcast = func(p *Packet) {
			for to := range s.cores {
				if to != from {
					deliver(to, p)
				}
			}
		}
		c.sendMask = func(mask uint64, p *Packet) {
			for to := range s.cores {
				if mask&(uint64(1)<<uint(to)) != 0 {
					deliver(to, p)
				}
			}
		}
	}
}

// Characterize the inherited latest-exchange ACK filter. RTT below the guard
// alone does not ensure activation: renewing every 500ms can supersede a guard
// before its ACK arrives. This is an availability limitation, not a safety fix
// or a reason to optimize the authors' protocol in this reproduction.
func TestSourceLimitationSlowGuardACKs(t *testing.T) {
	for _, oneWay := range []time.Duration{10 * time.Millisecond, 300 * time.Millisecond} {
		t.Run(oneWay.String(), func(t *testing.T) {
			s := cluster(t, 5)
			adversarialNetwork(s, rand.New(rand.NewSource(1)), func() time.Duration { return oneWay }, 0, 0)
			s.advance(12 * time.Second)
			r := s.request(2, state.GET, 7, "")
			s.advance(3 * time.Second)
			_, replied := s.replies[r]
			wantReply := 2*oneWay < leaseRenewInterval
			if replied != wantReply {
				t.Fatalf("source ACK-filter behavior changed at RTT %v: replied=%v want=%v", 2*oneWay, replied, wantReply)
			}
			if !wantReply && s.cores[2].lease.promisesAccepted != 0 {
				t.Fatal("expected superseded guard ACKs to prevent any promise establishment")
			}
		})
	}
}

func TestReorderedDuplicateTrafficPreservesReadFreshness(t *testing.T) {
	for _, n := range []int{3, 5, 9} {
		for seed := int64(1); seed <= 8; seed++ {
			t.Run(fmt.Sprintf("n=%d/seed=%d", n, seed), func(t *testing.T) {
				s := cluster(t, n)
				rng := rand.New(rand.NewSource(seed))
				adversarialNetwork(s, rng, func() time.Duration { return time.Duration(1+rng.Intn(60)) * time.Millisecond }, 3, 10)
				type outstandingRead struct {
					request Request
					minimum int
					lastTry time.Time
				}
				reads := map[defs.RequestID]outstandingRead{}
				var write *Request
				var writeTry time.Time
				issued, completed, observed, readCount := 0, 0, 0, 0
				nextID := int32(0)
				for step := 0; step < 4000; step++ {
					if step == 1000 {
						s.alive[0] = false
					}
					if write != nil {
						if _, ok := s.replies[write.ID]; ok {
							completed = issued
							write = nil
						}
					}
					for id, r := range reads {
						if value, ok := s.replies[id]; ok {
							v := 0
							if len(value) != 0 {
								var err error
								v, err = strconv.Atoi(string(value))
								if err != nil {
									t.Fatal(err)
								}
							}
							if v < r.minimum || v > issued {
								t.Fatalf("read %v returned version %d; minimum visible before invocation=%d latest issued=%d", id, v, r.minimum, issued)
							}
							if v > observed {
								observed = v
							}
							delete(reads, id)
							readCount++
						}
					}
					// Finite load followed by a quiet drain verifies eventual progress.
					if step < 3000 && step%20 == 0 && write == nil {
						issued++
						nextID++
						r := Request{ID: defs.RequestID{Client: -1, Sequence: nextID}, Origin: 1,
							Command: state.Command{Op: state.PUT, K: 7, V: state.Value(strconv.Itoa(issued))}}
						write = &r
						writeTry = s.now
						s.cores[1].submit(r, s.now)
					}
					if step < 3000 && step%10 == 0 {
						to := 1 + rng.Intn(n-1)
						nextID++
						r := Request{ID: defs.RequestID{Client: -1, Sequence: nextID}, Origin: int32(to), Command: state.Command{Op: state.GET, K: 7}}
						minimum := completed
						if observed > minimum {
							minimum = observed
						}
						reads[r.ID] = outstandingRead{request: r, minimum: minimum, lastTry: s.now}
						s.cores[to].submit(r, s.now)
					}
					if write != nil && s.now.Sub(writeTry) >= time.Second {
						s.cores[1].submit(*write, s.now)
						writeTry = s.now
					}
					for id, r := range reads {
						if s.now.Sub(r.lastTry) >= time.Second {
							s.cores[r.request.Origin].submit(r.request, s.now)
							r.lastTry = s.now
							reads[id] = r
						}
					}
					s.advance(10 * time.Millisecond)
				}
				if write != nil || len(reads) != 0 || completed != issued || readCount != 300 {
					t.Fatalf("work did not drain: completed_writes=%d issued=%d pending_write=%v pending_reads=%d completed_reads=%d", completed, issued, write != nil, len(reads), readCount)
				}
			})
		}
	}
}
