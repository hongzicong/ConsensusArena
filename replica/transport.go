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
	reply := &defs.ProposeReplyTS{OK: defs.TRUE, CommandId: p.CommandId, Value: value, Timestamp: p.Timestamp}
	return r.ReplyProposal(p, reply, capacity)
}

// Transport provides event-loop-owned sending for protocols with synchronous
// local delivery. Local supplies the protocol transition; counters track admission.
type Transport struct {
	Base               *Replica
	Peers              []*Sender
	Local              func(fastrpc.Serializable)
	SendDrops, Replies int
}

func (t *Transport) Send(id int32, code uint8, msg fastrpc.Serializable) {
	if id == t.Base.Id {
		t.Local(msg)
		return
	}
	t.Base.M.Lock()
	alive := t.Base.Alive[id]
	t.Base.M.Unlock()
	if alive && t.Peers[id].Enqueue(Encode(code, msg, true)) != nil {
		t.SendDrops++
	}
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

// Broadcast selection stays outside Sender; one wire image can be shared by
// the independently scheduled connections because it is immutable.
func (r *Replica) SendToAll(msg fastrpc.Serializable, code uint8) {
	f := Encode(code, msg, true)
	for id := 0; id < r.N; id++ {
		r.M.Lock()
		alive := r.Alive[id]
		r.M.Unlock()
		if alive && id != int(r.Id) {
			_ = r.PeerSender(id).Enqueue(f)
		}
	}
}
