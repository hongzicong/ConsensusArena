package kcensus

// Replica/client streams and output delivery.
import (
	"bufio"
	"fmt"
	"io"
	"net"
	"time"

	"github.com/hongzicong/ConsensusArena/protocol"
	"github.com/hongzicong/ConsensusArena/replica"
	"github.com/hongzicong/ConsensusArena/replica/defs"
	fastrpc "github.com/hongzicong/ConsensusArena/rpc"
)

type replyJob struct {
	proposal *defs.GPropose
	reply    defs.ProposeReplyTS
}

func (r *Replica) drain() {
	protocol.Drain(&r.engine.out, func(e envelope) bool {
		if e.To == int(r.Id) {
			protocol.Must(r.Handle(localMessage{e.Message}, time.Time{}))
		} else if !r.engine.peerFailed(e.To) {
			_ = r.peers.Enqueue(e.To, encodeOutput(r.code, e, &r.engine.stats))
		}
		return true
	})
}

// The image is scoped to one broadcast. Sender never mutates frame bytes;
// every recipient still receives its own FIFO frame through the common sender.
func encodeOutput(code uint8, e envelope, stats *Stats) replica.Frame {
	if e.Encoded != nil && *e.Encoded != nil {
		stats.ReusedEncodings++
		stats.EncodedBytes += uint64(len(*e.Encoded))
		return replica.Frame{Data: *e.Encoded}
	}
	stats.EncodedFrames++
	var before time.Time
	if stats.EncodedFrames%64 == 1 {
		before = time.Now()
	}
	frame := replica.Encode(code, &wireMessage{e.Message}, true)
	if !before.IsZero() {
		stats.EncodingSamples++
		stats.EncodingNanos += uint64(time.Since(before))
	}
	stats.EncodedBytes += uint64(len(frame.Data))
	if e.Encoded != nil {
		*e.Encoded = frame.Data
	}
	return frame
}

// Configure the FIFO policy before connections start; use the common writer.
func (r *Replica) configureTransport() {
	r.ClientReplyCapacity = 8192
	r.PeerSendOptions = replica.SenderOptions{Capacity: -1}
}

func (r *Replica) bindClient(m message) (int, error) {
	id, connection := m.From, m.ClientConnection
	if id < r.engine.n || id >= r.engine.m || r.topology.Identities[id] != connection.Identity {
		return -1, fmt.Errorf("invalid KCensus client peer %d", id)
	}
	if r.engine.peerFailed(id) {
		return -1, replica.ErrSendClosed
	}
	// The shared connection context invokes this exactly once, before delivery.
	if err := r.peers.Bind(id, func() *replica.Sender {
		return r.StreamSender(connection.Conn, replica.SenderOptions{Capacity: -1})
	}); err != nil {
		return -1, err
	}
	r.engine.markConnected(id)
	return id, nil
}

func (r *Replica) flushReplies() {
	for id, j := range r.pendingReplies {
		if r.ReplyProposal(j.proposal, &j.reply, 8192) == nil {
			delete(r.pendingReplies, id)
			delete(r.proposals, id)
		}
	}
}

func (c *Client) drain() {
	protocol.Drain(&c.engine.out, func(e envelope) bool {
		if e.To == c.engine.id {
			c.engine.step(e.Message)
		} else if !c.engine.peerFailed(e.To) {
			select {
			case <-c.stop:
				return false
			default:
			}
			_ = c.peers.Enqueue(e.To, encodeOutput(defs.RPC_TABLE, e, &c.engine.stats))
		}
		return true
	})
}

func (c *Client) deliver(m message) bool {
	inbox := c.inbox
	if m.Kind == heartbeat {
		inbox = c.control
	}
	if m.Kind == executedResult {
		inbox = c.resultInbox
	}
	return fastrpc.Deliver(inbox, m, c.stop)
}

// All client streams use the same immutable registration and fresh factories.
var clientMessages = func() *fastrpc.Table {
	t := fastrpc.NewTableId(defs.RPC_TABLE)
	t.Register(&wireMessage{}, nil)
	return t
}()

func decodeClientMessage(code uint8, wire io.Reader) (*wireMessage, error) {
	p, err := clientMessages.Decode(code, wire)
	if err != nil {
		return nil, err
	}
	return p.Obj.(*wireMessage), nil
}

func (c *Client) readStream(id int, conn net.Conn, r *bufio.Reader) {
	defer conn.Close()
	cancelled := false
	if id < 0 {
		// Resolve an incoming mesh connection once using its first normal frame.
		first, err := fastrpc.ReadMessage(r, decodeClientMessage)
		if err != nil {
			return
		}
		if first.From < c.engine.n || first.From >= c.engine.id || c.bindStream(first.From, conn, nil) != nil {
			c.Printf("KCENSUS_CLIENT_STREAM rejected_from=%d self=%d", first.From, c.engine.id)
			return
		}
		id = first.From
		_ = conn.SetReadDeadline(time.Time{})
		if !c.deliver(first.message) {
			return
		}
	}
	streamErr := fastrpc.ReadStream(r, decodeClientMessage, func(w *wireMessage) bool {
		w.From = id // The established stream supplies its source identity.
		cancelled = !c.deliver(w.message)
		return !cancelled
	})
	if cancelled || id < 0 {
		return
	}
	c.Printf("KCENSUS_CLIENT_STREAM peer=%d closed error=%v", id, streamErr)
	if id < c.engine.n {
		c.MarkPeerFailed(id)
	}
	c.peers.ClosePeer(id)
}

func (c *Client) bindStream(id int, conn net.Conn, w *bufio.Writer) error {
	options := replica.SenderOptions{Capacity: -1, Writer: w}
	if id < c.engine.n {
		options.Locker = c.WriteMutex(id)
	}
	return c.peers.BindConnection(id, conn, options)
}

func (c *Client) acceptMesh() {
	for {
		conn, err := c.listener.Accept()
		if err != nil {
			return
		}
		_ = conn.SetReadDeadline(time.Now().Add(2*time.Second + c.engine.retry))
		go c.readStream(-1, conn, bufio.NewReader(conn))
	}
}

// A local proxy may accept TCP before the remote listener exists. Confirm an
// ordinary protocol frame before publishing a stream or treating EOF as a
// crash. The existing heartbeat also supplies the first-frame identity; no
// separate handshake message or protocol vote is introduced.
func (c *Client) confirmMesh(id int, conn net.Conn) (*bufio.Reader, message, error) {
	if err := conn.SetDeadline(time.Now().Add(2*time.Second + c.engine.retry)); err != nil {
		return nil, message{}, err
	}
	frame := replica.Encode(defs.RPC_TABLE, &wireMessage{message{Kind: heartbeat, From: c.engine.id, Digest: c.engine.plan.Digest}}, true)
	if n, err := conn.Write(frame.Data); err != nil {
		return nil, message{}, err
	} else if n != len(frame.Data) {
		return nil, message{}, io.ErrShortWrite
	}
	r := bufio.NewReader(conn)
	w, err := fastrpc.ReadMessage(r, decodeClientMessage)
	if err != nil {
		return nil, message{}, err
	}
	if w.From != id {
		return nil, message{}, fmt.Errorf("invalid KCensus mesh peer %d (expected %d)", w.From, id)
	}
	if err := conn.SetDeadline(time.Time{}); err != nil {
		return nil, message{}, err
	}
	return r, w.message, nil
}

func (c *Client) dialMesh(id int) {
	timer := time.NewTicker(100 * time.Millisecond)
	defer timer.Stop()
	for {
		select {
		case <-c.stop:
			return
		default:
		}
		conn, err := net.DialTimeout("tcp", defs.DialAddress(c.topology.Listeners[id]), time.Second)
		if err == nil {
			r, first, confirmErr := c.confirmMesh(id, conn)
			if confirmErr == nil && c.bindStream(id, conn, nil) == nil {
				if !c.deliver(first) {
					conn.Close()
					return
				}
				go c.readStream(id, conn, r)
				return
			}
			conn.Close()
		}
		select {
		case <-c.stop:
			return
		case <-timer.C:
		}
	}
}
