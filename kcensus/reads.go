package kcensus

// Read state, acceptance fences, ready quorums, and future-slot release.
import (
	"math/bits"
	"sort"
	"time"

	"github.com/hongzicong/ConsensusArena/state"
)

func (c *core) readFence(k state.Key) uint64 {
	s := c.shards[k]
	if s == nil {
		return 0
	}
	fence := s.executed
	// Mirror upstream's current slot + get_my_v().is_some(). Metadata,
	// buffered payloads, offers and bare prepares are not acceptances.
	if x := s.slots[s.executed+1]; x != nil && (x.fast != nil || x.classic != nil) {
		fence++
	}
	return fence
}

// A voter's first fence is immutable for this GET. Query only missing voters;
// an existing high fence still waits for local prefix execution, while unseen
// voters may supply another ready member of the same n-f quorum.
func (c *core) sendMissingReadQueries(q *pendingRead) bool {
	sent := false
	for voter := 0; voter < c.n; voter++ {
		_, seen := q.fences[voter]
		if c.peerFailed(voter) || seen {
			c.stats.SuppressedReadRequests++
			continue
		}
		c.send(voter, message{Kind: readQuery, Key: q.record.Command.K, ID: q.record.ID})
		sent = true
	}
	return sent
}

func (c *core) readyRead(q *pendingRead) {
	if q.quorum {
		return
	}
	executed := c.executed(q.record.Command.K)
	var fences []uint64
	for voter, fence := range q.fences {
		if fence <= executed && q.replies&(1<<voter) == 0 {
			q.replies |= 1 << voter
			c.stats.ReadyReadReplies++
		}
		fences = append(fences, fence)
	}
	q.quorum = bits.OnesCount64(q.replies) >= c.n-c.f
	if q.quorum {
		return
	}
	if len(fences) >= c.n-c.f {
		sort.Slice(fences, func(i, j int) bool { return fences[i] < fences[j] })
		q.target = fences[c.n-c.f-1]
		if q.target > executed && !q.waited {
			q.waited = true
			c.stats.ReadWaits++
			c.stats.BufferedReadReplies += uint64(len(fences) - bits.OnesCount64(q.replies))
		}
	}
}

func (c *core) releaseFuture(k state.Key) {
	queued := c.future[k]
	if len(queued) == 0 {
		return
	}
	delete(c.future, k)
	var ready []message
	for _, m := range queued {
		if m.Slot <= c.executed(k)+1 {
			ready = append(ready, m)
		} else {
			c.future[k] = append(c.future[k], m)
		}
	}
	for _, m := range ready {
		c.stepMessage(m, true)
	}
}

type pendingRead struct {
	record            Record
	replies           uint64
	target            uint64
	quorum            bool
	fences            map[int]uint64
	waited            bool
	started, lastSend time.Duration
}
