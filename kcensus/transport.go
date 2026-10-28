package kcensus

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
	"github.com/hongzicong/ConsensusArena/state"
)

type replyJob struct {
	proposal *defs.GPropose
	reply    defs.ProposeReplyTS
}

type encodingStats struct {
	EncodedFrames, EncodedBytes, EncodingSamples, EncodingNanos uint64
	ReusedEncodings                                             uint64
}

func (r *Replica) drain() {
	drainOutputs(r.core.id, r.core.m, &r.core.out, r.core.peerFailed, &r.core.stats.encodingStats, r.peers, r.code, nil, func(m message) {
		protocol.Must(r.Step(localMessage{m}, time.Time{}))
	})
}

// Replica and client outputs share fanout and admission; nil stats skips accounting.
// Cancellation stops at the first eligible remote target, preserving local order.
func drainOutputs(id, processes int, out *[]envelope, peerFailed func(int) bool, stats *encodingStats, peers *replica.PeerStreams, code uint8, stop <-chan struct{}, local func(message)) {
	cancelled := false
	var attempts uint64
	sender := replica.MessageSender{ID: id, N: processes,
		Eligible: func(id int) bool {
			if cancelled || peerFailed(id) {
				return false
			}
			select {
			case <-stop:
				cancelled = true
				return false
			default:
				return true
			}
		},
		Enqueue: func(id int, f replica.Frame) error {
			if stats != nil {
				attempts++
				stats.EncodedBytes += uint64(len(f.Data))
			}
			return peers.Enqueue(id, f)
		},
		Local: func(msg interface{ Marshal(io.Writer) }) {
			if !cancelled {
				local(msg.(*wireMessage).message)
			}
		},
	}
	protocol.Drain(out, func(e envelope) bool {
		var frames uint64
		previousAttempts := attempts
		if stats != nil {
			frames = stats.EncodedFrames
		}
		encode := func() replica.Frame { return encodeOutput(code, e, stats) }
		if e.Count > 0 {
			fanout := sender
			fanout.N = e.Count
			fanout.SendToAll(&wireMessage{message: e.Message}, code, replica.SendPlan{IncludeSelf: true, Encode: encode})
		} else {
			_ = sender.Send(e.To, code, &wireMessage{message: e.Message}, encode)
		}
		// Rejected remote admissions also use an image; local delivery does not.
		if stats != nil {
			stats.ReusedEncodings += attempts - previousAttempts - (stats.EncodedFrames - frames)
		}
		return !cancelled
	})
}

// The image is scoped to one broadcast. Sender never mutates frame bytes;
// every recipient still receives its own FIFO frame through the common sender.
func encodeOutput(code uint8, e envelope, stats *encodingStats) replica.Frame {
	if e.Encoded != nil && *e.Encoded != nil {
		return replica.Frame{Data: *e.Encoded}
	}
	var before time.Time
	if stats != nil {
		stats.EncodedFrames++
		if stats.EncodedFrames%64 == 1 {
			before = time.Now()
		}
	}
	frame := replica.Encode(code, &wireMessage{message: e.Message}, true)
	if !before.IsZero() {
		stats.EncodingSamples++
		stats.EncodingNanos += uint64(time.Since(before))
	}
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

func (r *Replica) bindClient(m message, connection *fastrpc.ClientConnection) (int, error) {
	id := m.From
	if id < r.core.n || id >= r.core.m || r.topology.Identities[id] != connection.Identity {
		return -1, fmt.Errorf("invalid KCensus client peer %d", id)
	}
	if r.core.peerFailed(id) {
		return -1, replica.ErrSendClosed
	}
	if m.Digest != r.core.plan.Digest {
		return -1, fmt.Errorf("KCensus client %d requirement fingerprint mismatch", id)
	}
	// The shared connection context invokes this exactly once, before delivery.
	if err := r.peers.Bind(id, func() *replica.Sender {
		return r.StreamSender(connection.Conn, replica.SenderOptions{Capacity: -1})
	}); err != nil {
		return -1, err
	}
	r.core.markConnected(id)
	return id, nil
}

// Report whether this execution belongs to a locally admitted proposal.
func (r *Replica) queueReply(id defs.RequestID, v state.Value) bool {
	p := r.proposals[id]
	if p == nil {
		return false
	}
	r.pendingReplies[id] = replyJob{p, defs.ProposeReplyTS{OK: defs.TRUE, CommandId: id.Sequence, Value: v, Timestamp: p.Timestamp}}
	return true
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
	p := c.proposer
	drainOutputs(p.id, p.m, &p.out, p.peerFailed, nil, c.peers, defs.RPC_TABLE, c.stop, func(m message) { c.receive(projectClientMessage(m, p.id, p.m)) })
}

func (c *Client) deliver(m message) bool {
	switch m.Kind {
	case spread, accept, commit, offer, executedResult:
	default:
		return true
	}
	return fastrpc.Deliver(c.inbox, projectClientMessage(m, c.proposer.id, c.proposer.m), c.stop)
}

// All client streams use the same immutable registration and fresh factories.
var clientMessages = func() *fastrpc.Table {
	t := fastrpc.NewTableID(defs.RPC_TABLE)
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
	if id < 0 || id >= c.proposer.n {
		return
	}
	cancelled := false
	planChecked := false
	streamErr := fastrpc.ReadStream(r, decodeClientMessage, func(w *wireMessage) bool {
		w.From = id // The established stream supplies its source identity.
		if !planChecked {
			if w.Digest != c.proposer.plan.Digest {
				panic(fmt.Sprintf("KCensus voter %d requirement fingerprint mismatch", id))
			}
			planChecked = true
		}
		cancelled = !c.deliver(w.message)
		return !cancelled
	})
	if cancelled {
		return
	}
	c.Printf("KCENSUS_CLIENT_STREAM peer=%d closed error=%v", id, streamErr)
	c.MarkPeerFailed(id)
	c.peers.ClosePeer(id)
}

func (c *Client) bindStream(id int, conn net.Conn, w *bufio.Writer) error {
	if id < 0 || id >= c.proposer.n {
		return replica.ErrPeerID
	}
	options := replica.SenderOptions{Capacity: -1, Writer: w, Locker: c.WriteMutex(id)}
	return c.peers.BindConnection(id, conn, options)
}
