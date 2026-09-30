// Package consensusruntime provides event scheduling and output queues,
// without choosing values, counting votes, or deciding commitment.
package consensusruntime

import (
	"bufio"
	"bytes"
	"fmt"
	"github.com/hongzicong/ConsensusArena/replica"
	"github.com/hongzicong/ConsensusArena/replica/defs"
	fastrpc "github.com/hongzicong/ConsensusArena/rpc"
	"io"
	"sync"
	"time"
)

// Protocol callbacks are serialized by Run. Sending to self invokes Handle
// synchronously on the same goroutine, preserving reentrant local transitions.
// Tick supplies connectivity observations; the protocol decides how to recover.
type Protocol interface {
	Propose(*defs.GPropose)
	Handle(fastrpc.Serializable)
	Tick(time.Time, []bool)
	Leader() int32
	Status() string
}

type Runtime struct {
	Base               *replica.Replica
	protocol           Protocol
	in                 chan fastrpc.Serializable
	control            chan chan int32
	peers              []chan []byte
	clients            map[*bufio.Writer]*outputPipe
	sendDrops, replies int
}
type outputPipe struct {
	mu   sync.Mutex
	wake chan struct{}
	data [][]byte
}

func New(base *replica.Replica, protocol Protocol) *Runtime {
	return &Runtime{Base: base, protocol: protocol, in: make(chan fastrpc.Serializable, 65536), control: make(chan chan int32), clients: map[*bufio.Writer]*outputPipe{}}
}

// Register must run before Run and in the same order at every replica.
func (r *Runtime) Register(message fastrpc.Serializable) uint8 {
	return r.Base.RPC.Register(message, r.in)
}

// Send is called from protocol callbacks. Serialize before queueing to avoid
// aliasing later state changes. A full queue drops the frame; the protocol
// remains responsible for retransmission, as in the previous runtime.
func (r *Runtime) Send(id int32, code uint8, message fastrpc.Serializable) {
	if id == r.Base.Id {
		r.protocol.Handle(message)
		return
	}
	r.Base.M.Lock()
	alive := r.Base.Alive[id]
	r.Base.M.Unlock()
	if !alive {
		return
	}
	var b bytes.Buffer
	b.WriteByte(code)
	message.Marshal(&b)
	select {
	case r.peers[id] <- b.Bytes():
	default:
		r.sendDrops++
	}
}

// Reply queues an already-authorized client reply. A nil writer selects the
// registered client connection. The protocol chooses whether to add an RPC code.
// Like Send, Reply must be called from the serialized protocol callbacks.
func (r *Runtime) Reply(client int32, writer *bufio.Writer, code uint8, msg interface{ Marshal(io.Writer) }, custom bool) {
	if writer == nil {
		r.Base.M.Lock()
		writer = r.Base.ClientWriters[client]
		r.Base.M.Unlock()
	}
	if writer == nil {
		return
	}
	var b bytes.Buffer
	if custom {
		b.WriteByte(code)
	}
	msg.Marshal(&b)
	p := r.clients[writer]
	if p == nil {
		p = &outputPipe{wake: make(chan struct{}, 1)}
		r.clients[writer] = p
		go func() {
			for range p.wake {
				for {
					p.mu.Lock()
					if len(p.data) == 0 {
						p.mu.Unlock()
						break
					}
					data := p.data
					p.data = nil
					p.mu.Unlock()
					for _, frame := range data {
						writer.Write(frame)
					}
					writer.Flush()
				}
			}
		}()
	}
	p.mu.Lock()
	p.data = append(p.data, b.Bytes())
	p.mu.Unlock()
	select {
	case p.wake <- struct{}{}:
	default:
	}
	r.replies++
}
func (r *Runtime) Run() {
	r.Base.ConnectToPeers()
	r.Base.ComputeClosestPeers()
	r.peers = make([]chan []byte, r.Base.N)
	for id := range r.peers {
		if int32(id) == r.Base.Id {
			continue
		}
		r.peers[id] = make(chan []byte, 4096)
		go func(id int) {
			for frame := range r.peers[id] {
				if _, err := io.Copy(r.Base.Peers[id], bytes.NewReader(frame)); err != nil {
					r.Base.Peers[id].Close()
					return
				}
			}
		}(id)
	}
	go r.Base.WaitForClientConnections()
	ticker := time.NewTicker(2 * time.Millisecond)
	defer ticker.Stop()
	progress := time.NewTicker(5 * time.Second)
	defer progress.Stop()
	for {
		select {
		case reply := <-r.control:
			reply <- r.protocol.Leader()
		case g := <-r.Base.ProposeChan:
			r.protocol.Propose(g)
		case msg := <-r.in:
			r.protocol.Handle(msg)
		case now := <-ticker.C:
			r.Base.M.Lock()
			alive := append([]bool(nil), r.Base.Alive...)
			r.Base.M.Unlock()
			alive[r.Base.Id] = true
			r.protocol.Tick(now, alive)
		case <-progress.C:
			r.Base.Printf("%s replies=%d send_drops=%d", r.protocol.Status(), r.replies, r.sendDrops)
		}
	}
}

// LeaderHint observes the protocol leader; it never installs one.
func (r *Runtime) LeaderHint(reply *defs.BeTheLeaderReply) error {
	ch := make(chan int32, 1)
	timer := time.NewTimer(30 * time.Second)
	defer timer.Stop()
	select {
	case r.control <- ch:
	case <-timer.C:
		return fmt.Errorf("replica initializing")
	}
	select {
	case id := <-ch:
		reply.Leader = id
		if id == 0 {
			reply.Leader = -2
		}
		reply.NextLeader = -1
		return nil
	case <-timer.C:
		return fmt.Errorf("replica event loop unavailable")
	}
}
