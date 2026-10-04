package replica

import (
	"bufio"
	"encoding/json"
	"io"
	"net"
	"sync"
	"time"

	"github.com/hongzicong/ConsensusArena/replica/defs"
	fastrpc "github.com/hongzicong/ConsensusArena/rpc"
	"github.com/hongzicong/ConsensusArena/state"
)

// Framework messages are handled inline; registered protocol messages retain
// their original channels. Check Shutdown before every read, as before.
func (r *Replica) receiveStream(reader *bufio.Reader, decode func(uint8, io.Reader) (fastrpc.Pair, error)) error {
	return fastrpc.ReadLoop(func() (fastrpc.Pair, error) {
		if r.Shutdown {
			return fastrpc.Pair{}, io.EOF
		}
		return fastrpc.ReadMessage(reader, decode)
	}, func(p fastrpc.Pair) bool {
		if p.Obj == nil {
			return true
		}
		return p.Deliver()
	})
}

func (r *Replica) replicaListener(rid int, reader *bufio.Reader) {
	defer r.closeConnectionSender(r.Peers[rid])
	defer r.Peers[rid].Close()
	var beacon defs.Beacon
	var reply defs.BeaconReply
	err := r.receiveStream(reader, func(code uint8, wire io.Reader) (fastrpc.Pair, error) {
		switch code {
		case defs.GENERIC_SMR_BEACON:
			if err := beacon.Unmarshal(wire); err != nil {
				return fastrpc.Pair{}, err
			}
			r.ReplyBeacon(&defs.GBeacon{Rid: int32(rid), Timestamp: beacon.Timestamp})
		case defs.GENERIC_SMR_BEACON_REPLY:
			if err := reply.Unmarshal(wire); err != nil {
				return fastrpc.Pair{}, err
			}
			r.M.Lock()
			r.Latencies[rid] += time.Now().UnixNano() - reply.Timestamp
			r.M.Unlock()
			now := time.Now().UnixNano()
			r.Ewma[rid] = 0.99*r.Ewma[rid] + 0.01*float64(now-reply.Timestamp)
		default:
			return r.RPC.Decode(code, wire)
		}
		return fastrpc.Pair{}, nil
	})
	if err != nil && err != io.EOF {
		r.Printf("RECEIVE_ERROR source=replica peer=%d error=%q", rid, err)
	}
	r.M.Lock()
	r.Alive[rid] = false
	r.M.Unlock()
}

func (r *Replica) clientListener(conn net.Conn) {
	defer r.closeConnectionSender(conn)
	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)

	r.M.Lock()
	r.Println("Client up", conn.RemoteAddr(), "(", r.LRead, ")")
	r.M.Unlock()

	addr, identityErr := defs.ReadClientIdentity(reader)
	if identityErr != nil {
		r.Printf("Rejecting client %s: %v", conn.RemoteAddr(), identityErr)
		conn.Close()
		return
	}
	if !r.membership.HasClientEndpoint(addr) {
		r.Printf("Rejecting client %s with unknown endpoint identity %q", conn.RemoteAddr(), addr)
		conn.Close()
		return
	}
	isProxy := r.Config.Proxy.IsProxy(r.Alias, addr)

	r.senderMu.Lock()
	if r.clientConns == nil {
		r.clientConns = make(map[*bufio.Writer]net.Conn)
	}
	r.clientConns[writer] = conn
	r.senderMu.Unlock()
	defer func() {
		r.senderMu.Lock()
		sender := r.senders[writer]
		delete(r.clientConns, writer)
		r.senderMu.Unlock()
		if sender != nil {
			sender.Close()
		}
	}()

	mutex := &sync.Mutex{}
	var clientConnection *fastrpc.ClientConnection
	err := r.receiveStream(reader, func(code uint8, wire io.Reader) (fastrpc.Pair, error) {
		switch code {
		case defs.PROPOSE:
			propose := &defs.Propose{}
			if err := propose.Unmarshal(wire); err != nil {
				return fastrpc.Pair{}, err
			}
			if err := r.ProposalPolicy.Validate(propose.Command); err != nil {
				return fastrpc.Pair{}, err
			}
			r.M.Lock()
			r.ClientWriters[propose.ClientId] = writer
			r.M.Unlock()
			// Bind the reply sender before the proposal can produce protocol output.
			r.ReplySender(writer, mutex, r.ClientReplyCapacity)
			op := propose.Command.Op
			if r.LRead && (op == state.GET || op == state.SCAN) {
				r.ReplyProposeTS(&defs.ProposeReplyTS{OK: defs.TRUE, CommandId: propose.CommandId, Value: propose.Command.Execute(r.State), Timestamp: propose.Timestamp}, writer, mutex)
			} else {
				gpropose := &defs.GPropose{Propose: propose, Reply: writer, Mutex: mutex, Proxy: isProxy, Addr: addr}
				fastrpc.Deliver(r.ProposeChan, gpropose, nil)
			}
		case defs.READ:
			// Retain the existing decode-only behavior for these framework messages.
			if err := new(defs.Read).Unmarshal(wire); err != nil {
				return fastrpc.Pair{}, err
			}
		case defs.PROPOSE_AND_READ:
			if err := new(defs.ProposeAndRead).Unmarshal(wire); err != nil {
				return fastrpc.Pair{}, err
			}
		case defs.STATS:
			r.M.Lock()
			b, _ := json.Marshal(r.Stats)
			r.M.Unlock()
			_ = r.ReplySender(writer, mutex, -1).Enqueue(Frame{Data: b})
		default:
			p, err := r.RPC.Decode(code, wire)
			if err != nil {
				return fastrpc.Pair{}, err
			}
			// Attach local connection context without changing the message encoding.
			if bound, ok := p.Obj.(interface {
				BindClient(*fastrpc.ClientConnection)
			}); ok {
				if clientConnection == nil {
					clientConnection = &fastrpc.ClientConnection{Conn: conn, Identity: addr}
				}
				bound.BindClient(clientConnection)
			}
			return p, nil
		}
		return fastrpc.Pair{}, nil
	})
	if err != nil && err != io.EOF {
		r.Printf("RECEIVE_ERROR source=client endpoint=%q error=%q", addr, err)
	}
	conn.Close()
	r.Println("Client down", conn.RemoteAddr())
}
