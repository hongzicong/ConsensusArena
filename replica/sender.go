package replica

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

const DefaultSendCapacity = 16384

var (
	ErrSendFull   = errors.New("sender queue full")
	ErrSendClosed = errors.New("sender closed")
	ErrSendSource = errors.New("sender uses a protocol-owned source")
)

// Frame owns an immutable wire image. Tag is local metadata, not a wire field.
// A successful enqueue transfers ownership of Data to the sender.
type Frame struct {
	Data []byte
	Tag  uint8
}

// Encode freezes a message before the next protocol transition can mutate it.
func Encode(code uint8, msg interface{ Marshal(io.Writer) }, tagged bool) Frame {
	var b bytes.Buffer
	if tagged {
		b.WriteByte(code)
	}
	msg.Marshal(&b)
	return Frame{Data: b.Bytes()}
}

// FrameSource supplies protocol-specific scheduling, not socket I/O. Take
// blocks until work or closure; nil ends the stream. Close must wake Take and
// tolerate repeated calls. Returned frames must remain immutable.
type FrameSource interface {
	Take() []Frame
	Close()
}

type SenderOptions struct {
	Capacity     int // 0: default bounded FIFO; -1: explicit unbounded FIFO
	BatchSize    int // 0: one frame per flush
	Source       FrameSource
	Writer       *bufio.Writer
	Locker       sync.Locker      // for a stream also used by pre-existing client code
	WriteTimeout time.Duration    // 0: rely on closure, not a short WAN timeout
	OnFrame      func(Frame, int) // bytes accepted by the buffer, before flush
	OnBatch      func(int, int)   // frames/bytes after successful flush (not peer ACKs)
	OnError      func(error)
}

// Sender is the sole writer for one connection. No lock shared with another
// connection is held during network I/O. Queue admission never waits for I/O.
type Sender struct {
	conn                    net.Conn
	w                       *bufio.Writer
	options                 SenderOptions
	source                  FrameSource
	queue                   *frameQueue
	once                    sync.Once
	done                    chan struct{}
	closed                  atomic.Bool
	frames, bytes, failures atomic.Uint64
}

func NewSender(conn net.Conn, o SenderOptions) *Sender {
	s := &Sender{conn: conn, options: o, w: o.Writer, done: make(chan struct{})}
	if s.w == nil && conn != nil {
		s.w = bufio.NewWriter(conn)
	}
	if o.Source != nil {
		s.source = o.Source
	} else {
		capacity := o.Capacity
		if capacity == 0 {
			capacity = DefaultSendCapacity
		}
		batch := o.BatchSize
		if batch <= 0 {
			batch = 1
		}
		s.queue = newFrameQueue(capacity, batch)
		s.source = s.queue
	}
	go s.run()
	return s
}

func (s *Sender) Enqueue(f Frame) error {
	if s == nil || s.closed.Load() {
		return ErrSendClosed
	}
	if s.queue == nil {
		return ErrSendSource
	}
	return s.queue.enqueue(f)
}

// PendingSender retains immutable frames until a connection's Sender is bound.
// The owner serializes access. Sender retains its identity after closure; this
// helper does not reconnect or change the protocol's admission/retry policy.
type PendingSender struct {
	Sender  *Sender
	pending []Frame
}

func (p *PendingSender) Enqueue(f Frame) error {
	if p.Sender == nil {
		p.pending = append(p.pending, f)
		return nil
	}
	return p.Sender.Enqueue(f)
}

// Bind hands early frames to the sender in FIFO order and reports the first
// admission failure. The protocol owns recovery of rejected frames.
func (p *PendingSender) Bind(s *Sender) error {
	p.Sender = s
	var first error
	for _, f := range p.pending {
		if err := s.Enqueue(f); err != nil && first == nil {
			first = err
		}
	}
	p.pending = nil
	return first
}

func (p *PendingSender) Close() {
	if p.Sender != nil {
		p.Sender.Close()
	}
	p.pending = nil
}

func (s *Sender) Closed() bool          { return s == nil || s.closed.Load() }
func (s *Sender) Done() <-chan struct{} { return s.done }

// Close aborts queued work and unblocks a socket write. Wait on Done separately.
// No reconnect is implied. Callbacks must not wait on Done.
func (s *Sender) Close() {
	s.once.Do(func() {
		s.closed.Store(true)
		s.source.Close()
		if s.conn != nil {
			_ = s.conn.Close()
		}
	})
}
func (s *Sender) Stats() (frames, bytes, failures uint64) {
	return s.frames.Load(), s.bytes.Load(), s.failures.Load()
}
func (s *Sender) run() {
	defer close(s.done)
	defer s.Close()
	for {
		batch := s.source.Take()
		if batch == nil || s.closed.Load() {
			return
		}
		n, err := s.write(batch)
		if err != nil {
			s.failures.Add(1)
			s.Close()
			if s.options.OnError != nil {
				s.options.OnError(err)
			}
			return
		}
		s.frames.Add(uint64(len(batch)))
		s.bytes.Add(uint64(n))
		if s.options.OnBatch != nil {
			s.options.OnBatch(len(batch), n)
		}
	}
}
func (s *Sender) write(batch []Frame) (int, error) {
	if s.w == nil {
		return 0, ErrSendClosed
	}
	if s.options.Locker != nil {
		s.options.Locker.Lock()
		defer s.options.Locker.Unlock()
	}
	if s.conn != nil && s.options.WriteTimeout > 0 {
		if err := s.conn.SetWriteDeadline(time.Now().Add(s.options.WriteTimeout)); err != nil {
			return 0, err
		}
	}
	total := 0
	for _, f := range batch {
		n, err := s.w.Write(f.Data)
		total += n
		if s.options.OnFrame != nil {
			s.options.OnFrame(f, n)
		}
		if err != nil {
			return total, err
		}
		if n != len(f.Data) {
			return total, io.ErrShortWrite
		}
	}
	return total, s.w.Flush()
}

type frameQueue struct {
	mu                    sync.Mutex
	ready                 *sync.Cond
	frames                []Frame
	head, capacity, batch int
	closed                bool
}

func newFrameQueue(capacity, batch int) *frameQueue {
	q := &frameQueue{capacity: capacity, batch: batch}
	q.ready = sync.NewCond(&q.mu)
	return q
}
func (q *frameQueue) enqueue(f Frame) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return ErrSendClosed
	}
	if q.capacity >= 0 && len(q.frames)-q.head >= q.capacity {
		return ErrSendFull
	}
	q.frames = append(q.frames, f)
	q.ready.Signal()
	return nil
}
func (q *frameQueue) Take() []Frame {
	q.mu.Lock()
	defer q.mu.Unlock()
	for !q.closed && q.head == len(q.frames) {
		q.ready.Wait()
	}
	if q.closed {
		return nil
	}
	n := len(q.frames) - q.head
	if n > q.batch {
		n = q.batch
	}
	batch := append([]Frame(nil), q.frames[q.head:q.head+n]...)
	for i := q.head; i < q.head+n; i++ {
		q.frames[i] = Frame{}
	}
	q.head += n
	if q.head == len(q.frames) {
		q.frames = q.frames[:0]
		q.head = 0
	} else if q.head >= 1024 && q.head >= len(q.frames)/2 {
		q.frames = append([]Frame(nil), q.frames[q.head:]...)
		q.head = 0
	}
	return batch
}
func (q *frameQueue) Close() {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.closed = true
	q.frames = nil
	q.head = 0
	q.ready.Broadcast()
}
