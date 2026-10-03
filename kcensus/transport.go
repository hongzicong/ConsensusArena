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
			_ = r.peers[e.To].Enqueue(replica.Encode(r.code, &wireMessage{e.Message}, true))
		}
		return true
	})
}

// Configure the FIFO policy before connections start; use the common writer.
func (r *Replica) configureTransport() {
	r.ClientReplyCapacity = 8192
	r.PeerSendOptions = replica.SenderOptions{Capacity: -1}
}

func (r *Replica) startConnectionSender(id int, conn net.Conn) {
	if conn == nil {
		r.engine.markFailed(id)
		return
	}
	// Retain lossless pending sends, using the same unbounded FIFO option as Swift.
	_ = r.peers[id].Bind(r.StreamSender(conn, replica.SenderOptions{Capacity: -1}))
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
			c.sendMu.Lock()
			defer c.sendMu.Unlock()
			select {
			case <-c.stop:
				return false
			default:
			}
			_ = c.peers[e.To].Enqueue(replica.Encode(defs.RPC_TABLE, &wireMessage{e.Message}, true))
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
	select {
	case inbox <- m:
		return true
	case <-c.stop:
		return false
	}
}

func (c *Client) readStream(id int, conn net.Conn, r *bufio.Reader) {
	defer conn.Close()
	cancelled := false
	streamErr := fastrpc.ReadStream(r, func(code uint8, wire io.Reader) (*wireMessage, error) {
		if code != defs.RPC_TABLE {
			return nil, fmt.Errorf("unexpected KCensus RPC code %d", code)
		}
		w := &wireMessage{}
		err := w.Unmarshal(wire)
		if err != nil {
			c.Printf("KCENSUS_CLIENT_STREAM peer=%d error=%q", id, err)
		}
		return w, err
	}, func(w *wireMessage) bool {
		if id < 0 {
			// Incoming mesh connections identify themselves in their first frame.
			if w.From < c.engine.n || w.From >= c.engine.id || !c.attachMesh(w.From, conn) {
				c.Printf("KCENSUS_CLIENT_STREAM rejected_from=%d self=%d", w.From, c.engine.id)
				return false
			}
			id = w.From
			_ = conn.SetReadDeadline(time.Time{})
		}
		if w.From != id {
			c.Printf("KCENSUS_CLIENT_STREAM peer=%d unexpected_from=%d", id, w.From)
			return false
		}
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
	c.sendMu.Lock()
	c.peers[id].Close()
	c.sendMu.Unlock()
}

// The caller holds sendMu while publishing the sender and flushing early frames.
func (c *Client) startStreamSender(id int, conn net.Conn, w *bufio.Writer) {
	_ = conn.SetWriteDeadline(time.Time{})
	options := replica.SenderOptions{Capacity: -1, Writer: w}
	if id < c.engine.n {
		options.Locker = c.WriteMutex(id)
	}
	_ = c.peers[id].Bind(replica.NewSender(conn, options))
}

func (c *Client) attachMesh(id int, conn net.Conn) bool {
	c.sendMu.Lock()
	defer c.sendMu.Unlock()
	select {
	case <-c.stop:
		return false
	default:
	}
	if c.peers[id].Sender != nil {
		return false
	}
	c.startStreamSender(id, conn, bufio.NewWriter(conn))
	return true
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
	code, err := r.ReadByte()
	if err != nil {
		return nil, message{}, err
	}
	if code != defs.RPC_TABLE {
		return nil, message{}, fmt.Errorf("unexpected KCensus RPC code %d", code)
	}
	var w wireMessage
	if err := w.Unmarshal(r); err != nil {
		return nil, message{}, err
	}
	if w.From != id || w.Kind == heartbeat && w.Digest != c.engine.plan.Digest {
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
			if confirmErr == nil && c.attachMesh(id, conn) {
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
