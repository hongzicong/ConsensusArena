package replica

import (
	"errors"
	"net"
	"sync"
	"time"

	"github.com/hongzicong/ConsensusArena/replicaset"
)

var (
	ErrPeerID    = errors.New("invalid peer ID")
	ErrPeerBound = errors.New("peer stream already bound")
)

// PeerStreams owns one sender per configured process, including frames queued
// before binding. Bind and Close are permanent in the crash-stop transport.
// It creates no workers beyond the existing per-connection Sender.
type PeerStreams struct {
	mu      sync.Mutex
	peers   []PendingSender
	closed  replicaset.Set
	stopped bool
}

func NewPeerStreams(size int) *PeerStreams {
	if size < 1 || size > replicaset.MaxSize {
		panic("peer stream membership must fit Set capacity")
	}
	return &PeerStreams{peers: make([]PendingSender, size)}
}

func (p *PeerStreams) valid(id int) bool { return id >= 0 && id < len(p.peers) }

// Bind constructs a sender only after checking the slot. This avoids creating
// an unused sender for a duplicate stream or racing shutdown.
func (p *PeerStreams) Bind(id int, create func() *Sender) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.valid(id) {
		return ErrPeerID
	}
	if p.stopped || p.closed.Contains(id) {
		return ErrSendClosed
	}
	if p.peers[id].Sender != nil {
		return ErrPeerBound
	}
	s := create()
	if s == nil {
		return ErrSendClosed
	}
	return p.peers[id].Bind(s)
}

func (p *PeerStreams) BindConnection(id int, conn net.Conn, options SenderOptions) error {
	if conn == nil {
		return ErrSendClosed
	}
	return p.Bind(id, func() *Sender {
		_ = conn.SetWriteDeadline(time.Time{})
		return NewSender(conn, options)
	})
}

func (p *PeerStreams) Enqueue(id int, frame Frame) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.valid(id) {
		return ErrPeerID
	}
	if p.stopped || p.closed.Contains(id) {
		return ErrSendClosed
	}
	return p.peers[id].Enqueue(frame)
}

// Failed snapshots transport failures without invoking protocol callbacks
// from reader/writer goroutines. Unbound streams are not failed processes.
func (p *PeerStreams) Failed() replicaset.Set {
	p.mu.Lock()
	defer p.mu.Unlock()
	failed := p.closed
	for id, stream := range p.peers {
		if stream.Sender != nil && stream.Sender.Closed() {
			failed.Add(id)
		}
	}
	return failed
}

func (p *PeerStreams) ClosePeer(id int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.valid(id) {
		p.closed.Add(id)
		p.peers[id].Close()
	}
}

func (p *PeerStreams) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.stopped = true
	for id := range p.peers {
		p.closed.Add(id)
		p.peers[id].Close()
	}
}
