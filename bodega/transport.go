package bodega

import (
	"github.com/hongzicong/ConsensusArena/replica"
	"github.com/hongzicong/ConsensusArena/replica/defs"
	"github.com/hongzicong/ConsensusArena/state"
	"sync"
	"sync/atomic"
	"time"
)

// Bodega owns admission and priority, including the promise-drained event.
// The shared Sender owns all connection writes and failures.
type sendSource struct {
	mu                      sync.Mutex
	data, control, promises chan replica.Frame
	stop                    chan struct{}
	closed                  bool
	peer                    int
	drained                 chan int
}

func newSendSource(peer int, drained chan int) *sendSource {
	return &sendSource{data: make(chan replica.Frame, 4096), control: make(chan replica.Frame, 4096), promises: make(chan replica.Frame, 32), stop: make(chan struct{}), peer: peer, drained: drained}
}
func (q *sendSource) enqueue(f replica.Frame) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return false
	}
	lane := q.control
	if kind(f.Tag) == accept || kind(f.Tag) == forward || kind(f.Tag) == committedEntry {
		lane = q.data
	} else if kind(f.Tag) == promise {
		lane = q.promises
	}
	select {
	case lane <- f:
		return true
	default:
		return false
	}
}
func (q *sendSource) Take() []replica.Frame {
	var f replica.Frame
	select {
	case <-q.stop:
		return nil
	case f = <-q.control:
	default:
		select {
		case <-q.stop:
			return nil
		case f = <-q.control:
		case f = <-q.data:
		case f = <-q.promises:
		}
	}
	if kind(f.Tag) == promise {
		select {
		case q.drained <- q.peer:
		default:
		}
	}
	return []replica.Frame{f}
}
func (q *sendSource) Close() {
	q.mu.Lock()
	defer q.mu.Unlock()
	if !q.closed {
		q.closed = true
		close(q.stop)
		for _, lane := range []chan replica.Frame{q.data, q.control, q.promises} {
		drain:
			for {
				select {
				case <-lane:
				default:
					break drain
				}
			}
		}
	}
}

type replicaTransport struct {
	sources               []*sendSource
	drained               chan int
	wireBytes, wireFrames *[kindCount]atomic.Uint64
	code                  uint8
}

func (t *replicaTransport) trySend(to int, m message) bool {
	q := t.sources[to]
	q.mu.Lock()
	closed := q.closed
	q.mu.Unlock()
	if closed {
		return false
	}
	f := replica.Encode(t.code, &m, true)
	f.Tag = uint8(m.Kind)
	return q.enqueue(f)
}
func (r *Replica) sendReply(p *defs.GPropose, value state.Value) bool {
	return r.ReplyResult(p, value, 8192) == nil
}
func (r *Replica) configureTransport(code uint8) *replicaTransport {
	t := &replicaTransport{sources: make([]*sendSource, r.N), drained: make(chan int, r.N), wireBytes: new([kindCount]atomic.Uint64), wireFrames: new([kindCount]atomic.Uint64), code: code}
	r.PeerSendOptionsFor = func(id int) replica.SenderOptions {
		q := newSendSource(id, t.drained)
		t.sources[id] = q
		return replica.SenderOptions{Source: q, WriteTimeout: 2 * time.Second, OnFrame: func(f replica.Frame, n int) {
			t.wireBytes[f.Tag].Add(uint64(n))
			t.wireFrames[f.Tag].Add(1)
		}}
	}
	return t
}
