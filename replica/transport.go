package replica

import (
	"bufio"
	"io"
	"net"
	"sync"

	"github.com/hongzicong/ConsensusArena/replica/defs"
	fastrpc "github.com/hongzicong/ConsensusArena/rpc"
	"github.com/hongzicong/ConsensusArena/state"
)

// MessageSender shares encoding and fanout without choosing a protocol's
// recipients, failure detector, local transition, or queue policy.
type MessageSender struct {
	ID, N    int
	Eligible func(int) bool
	Enqueue  func(int, Frame) error
	Local    func(interface{ Marshal(io.Writer) })
}

type SendPlan struct {
	Order       []int32 // nil uses ascending process IDs
	Limit       int     // <= 0 attempts every eligible recipient
	IncludeSelf bool
	Encode      func() Frame // optional immutable image or observed encoder
}

func (s MessageSender) deliver(id int, msg interface{ Marshal(io.Writer) }, encode func() Frame) (bool, error) {
	if id < 0 || id >= s.N {
		return true, ErrPeerID
	}
	if id == s.ID {
		if s.Local == nil {
			return false, nil
		}
		s.Local(msg)
		return true, nil
	}
	if s.Eligible != nil && !s.Eligible(id) {
		return false, nil
	}
	return true, s.Enqueue(id, encode())
}

// Send returns queue admission errors; skipped destinations return nil.
func (s MessageSender) Send(id int, code uint8, msg interface{ Marshal(io.Writer) }, image ...func() Frame) error {
	encode := func() Frame { return Encode(code, msg, true) }
	if len(image) != 0 {
		encode = image[0]
	}
	_, err := s.deliver(id, msg, encode)
	return err
}

// SendToAll encodes once and preserves the supplied attempt order. Failed
// admissions count towards Limit; they are never replaced by extra voters.
func (s MessageSender) SendToAll(msg interface{ Marshal(io.Writer) }, code uint8, plans ...SendPlan) int {
	var plan SendPlan
	if len(plans) != 0 {
		plan = plans[0]
	}
	var frame Frame
	encoded := false
	image := func() Frame {
		if !encoded {
			if plan.Encode != nil {
				frame = plan.Encode()
			} else {
				frame = Encode(code, msg, true)
			}
			encoded = true
		}
		return frame
	}
	count := s.N
	if plan.Order != nil {
		count = len(plan.Order)
	}
	attempts, drops := 0, 0
	for i := 0; i < count; i++ {
		id := i
		if plan.Order != nil {
			id = int(plan.Order[i])
		}
		if id == s.ID && !plan.IncludeSelf {
			continue
		}
		attempted, err := s.deliver(id, msg, image)
		if !attempted {
			continue
		}
		attempts++
		if err != nil {
			drops++
		}
		if plan.Limit > 0 && attempts >= plan.Limit {
			break
		}
	}
	return drops
}

// SenderFor registers one sender per connection (or writer when no connection
// is available). The first caller selects its queue policy; protocol setup must
// precede traffic. Socket I/O never holds M.
func (r *Replica) SenderFor(w *bufio.Writer, conn net.Conn, o SenderOptions) *Sender {
	if w == nil {
		return nil
	}
	r.senderMu.Lock()
	defer r.senderMu.Unlock()
	if r.sendersClosed {
		if o.Source != nil {
			o.Source.Close()
		}
		return nil
	}
	if r.senders == nil {
		r.senders = make(map[*bufio.Writer]*Sender)
	}
	if s := r.senders[w]; s != nil {
		return s
	}
	if conn == nil {
		conn = r.clientConns[w]
	}
	if conn != nil {
		if s := r.connectionSenders[conn]; s != nil {
			r.senders[w] = s
			return s
		}
	}
	o.Writer = w
	s := NewSender(conn, o)
	r.senders[w] = s
	if conn != nil {
		if r.connectionSenders == nil {
			r.connectionSenders = make(map[net.Conn]*Sender)
		}
		r.connectionSenders[conn] = s
	}
	return s
}

// StreamSender also registers client-voter streams whose protocol binds a
// connection before producing ordinary client replies.
func (r *Replica) StreamSender(conn net.Conn, o SenderOptions) *Sender {
	if conn == nil {
		return nil
	}
	w := o.Writer
	if w == nil {
		w = bufio.NewWriter(conn)
	}
	return r.SenderFor(w, conn, o)
}

func (r *Replica) closeConnectionSender(conn net.Conn) {
	r.senderMu.Lock()
	s := r.connectionSenders[conn]
	r.senderMu.Unlock()
	if s != nil {
		s.Close()
	}
}

// PeerSender only retrieves the sender installed during connection setup.
func (r *Replica) PeerSender(id int) *Sender {
	return r.PeerSenders[id]
}
func (r *Replica) ReplySender(w *bufio.Writer, lock *sync.Mutex, capacity int) *Sender {
	o := SenderOptions{Capacity: capacity}
	if lock != nil {
		o.Locker = lock
	}
	return r.SenderFor(w, nil, o)
}

// Connection setup installs all senders before readers can answer probe traffic.
// OptionsFor replaces the common options for protocols with per-peer sources.
func (r *Replica) initPeerSenders() {
	r.PeerSenders = make([]*Sender, r.N)
	for id := range r.PeerSenders {
		if int32(id) != r.Id {
			o := r.PeerSendOptions
			if r.PeerSendOptionsFor != nil {
				o = r.PeerSendOptionsFor(id)
			}
			w := r.PeerWriters[id]
			if o.Writer != nil {
				w = o.Writer
			}
			r.PeerSenders[id] = r.SenderFor(w, r.Peers[id], o)
		}
	}
}

// ClientSender prefers the request's writer, falling back to the client registry.
func (r *Replica) ClientSender(client int32, w *bufio.Writer, capacity int) *Sender {
	if w == nil {
		r.M.Lock()
		w = r.ClientWriters[client]
		r.M.Unlock()
	}
	return r.ReplySender(w, nil, capacity)
}

// ReplyProposal reports queue admission; the protocol decides whether to retry.
func (r *Replica) ReplyProposal(p *defs.GPropose, reply *defs.ProposeReplyTS, capacity int) error {
	return r.ReplySender(p.Reply, p.Mutex, capacity).Enqueue(Encode(0, reply, false))
}

func (r *Replica) ReplyResult(p *defs.GPropose, value state.Value, capacity int) error {
	reply := &defs.ProposeReplyTS{OK: defs.TRUE, CommandId: p.RequestID().Sequence, Value: value, Timestamp: p.Timestamp}
	return r.ReplyProposal(p, reply, capacity)
}

// Transport provides event-loop-owned sending for protocols with synchronous
// local delivery. Local supplies the protocol transition; counters track admission.
type Transport struct {
	Base               *Replica
	Local              func(fastrpc.Serializable)
	SendDrops, Replies int
}

func (t *Transport) Send(id int32, code uint8, msg fastrpc.Serializable) {
	if id == t.Base.Id {
		t.Local(msg)
		return
	}
	if t.Base.Send(id, code, msg) != nil {
		t.SendDrops++
	}
}

// SendToAll uses the same admission counter as single-peer sends.
func (t *Transport) SendToAll(msg fastrpc.Serializable, code uint8) {
	t.SendDrops += t.Base.SendToAll(msg, code)
}

func (t *Transport) Reply(client int32, writer *bufio.Writer, code uint8, msg interface{ Marshal(io.Writer) }, custom bool) {
	s := t.Base.ClientSender(client, writer, -1)
	if s != nil && s.Enqueue(Encode(code, msg, custom)) == nil {
		t.Replies++
	}
}

func (r *Replica) CloseSenders() {
	r.senderMu.Lock()
	r.sendersClosed = true
	all := make([]*Sender, 0, len(r.senders))
	for _, s := range r.senders {
		all = append(all, s)
	}
	r.senderMu.Unlock()
	for _, s := range all {
		s.Close()
	}
}

// Send attempts one remote queue admission. Self and peers skipped by Alive
// return nil without encoding; Transport owns synchronous local delivery.
// A nil error reports admission or a skipped peer, not network delivery.
func (r *Replica) Send(id int32, code uint8, msg interface{ Marshal(io.Writer) }) error {
	s := r.Messages()
	s.Eligible = func(id int) bool { return r.peerAlive(int32(id)) }
	return s.Send(int(id), code, msg)
}

func (r *Replica) peerAlive(id int32) bool {
	r.M.Lock()
	defer r.M.Unlock()
	return r.Alive[id]
}

// Messages provides the common sender with no protocol failure filtering.
// Callers may supply their own eligibility and local-delivery policies.
func (r *Replica) Messages() MessageSender {
	return MessageSender{ID: int(r.Id), N: r.N, Enqueue: func(id int, f Frame) error {
		return r.PeerSender(id).Enqueue(f)
	}}
}

// Broadcast selection stays outside Sender; one wire image can be shared by
// the independently scheduled connections because it is immutable.
// Return queue admission failures, not network delivery failures. Self and
// peers skipped by Alive are excluded from the count.
func (r *Replica) SendToAll(msg fastrpc.Serializable, code uint8, plan ...SendPlan) int {
	s := r.Messages()
	s.Eligible = func(id int) bool { return r.peerAlive(int32(id)) }
	return s.SendToAll(msg, code, plan...)
}
