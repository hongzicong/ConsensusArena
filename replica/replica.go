package replica

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/hongzicong/ConsensusArena/config"
	"github.com/hongzicong/ConsensusArena/dlog"
	"github.com/hongzicong/ConsensusArena/replica/defs"
	fastrpc "github.com/hongzicong/ConsensusArena/rpc"
	"github.com/hongzicong/ConsensusArena/state"
)

type Replica struct {
	*dlog.Logger

	M     sync.Mutex
	N     int
	F     int
	Id    int32
	Alias string

	PeerAddrList       []string
	Peers              []net.Conn
	PeerReaders        []*bufio.Reader
	PeerWriters        []*bufio.Writer
	ClientWriters      map[int32]*bufio.Writer
	Config             *config.Config
	Alive              []bool
	PreferredPeerOrder []int32

	State       *state.State
	RPC         *fastrpc.Table
	StableStore *os.File
	Stats       *defs.Stats
	Shutdown    bool
	Listener    net.Listener
	ProposeChan chan *defs.GPropose
	BeaconChan  chan *defs.GBeacon

	Thrifty bool
	Exec    bool
	LRead   bool
	Dreply  bool
	Beacon  bool
	Durable bool

	Ewma      []float64
	Latencies []int64
}

func New(alias string, id, f int, addrs []string, thrifty, exec, lread bool, config *config.Config, l *dlog.Logger) *Replica {
	n := len(addrs)
	stateMachine := state.InitState()
	if config.Preload {
		started := time.Now()
		digest, err := stateMachine.Preload(config.KeyCount, config.CommandSize, uint64(config.PreloadSeed))
		if err != nil {
			panic(fmt.Sprintf("preload state: %v", err))
		}
		l.Printf("PRELOAD_COMPLETE records=%d value_size=%d seed=%d digest=%s duration=%s",
			config.KeyCount, config.CommandSize, config.PreloadSeed, digest, time.Since(started))
	}
	r := &Replica{
		Logger: l,

		N:     n,
		F:     f,
		Id:    int32(id),
		Alias: alias,

		PeerAddrList:       addrs,
		Peers:              make([]net.Conn, n),
		PeerReaders:        make([]*bufio.Reader, n),
		PeerWriters:        make([]*bufio.Writer, n),
		ClientWriters:      make(map[int32]*bufio.Writer),
		Config:             config,
		Alive:              make([]bool, n),
		PreferredPeerOrder: make([]int32, n),

		State:       stateMachine,
		RPC:         fastrpc.NewTableId(defs.RPC_TABLE),
		StableStore: nil,
		Stats:       &defs.Stats{make(map[string]int)},
		Shutdown:    false,
		Listener:    nil,
		ProposeChan: make(chan *defs.GPropose, defs.CHAN_BUFFER_SIZE),
		BeaconChan:  make(chan *defs.GBeacon, defs.CHAN_BUFFER_SIZE),

		Thrifty: thrifty,
		Exec:    exec,
		LRead:   lread,
		Dreply:  true,
		Beacon:  false,
		Durable: false,

		Ewma:      make([]float64, n),
		Latencies: make([]int64, n),
	}

	for i := 0; i < r.N; i++ {
		r.PreferredPeerOrder[i] = int32((int(r.Id) + 1 + i) % r.N)
		r.Ewma[i] = 0.0
		r.Latencies[i] = 0
	}

	return r
}

func (r *Replica) Ping(args *defs.PingArgs, reply *defs.PingReply) error {
	return nil
}

func (r *Replica) BeTheLeader(args *defs.BeTheLeaderArgs, reply *defs.BeTheLeaderReply) error {
	return nil
}

func (r *Replica) FastQuorumSize() int {
	return r.F + (r.F+1)/2
}

func (r *Replica) SlowQuorumSize() int {
	return (r.N + 1) / 2
}

func (r *Replica) WriteQuorumSize() int {
	return r.F + 1
}

func (r *Replica) ReadQuorumSize() int {
	return r.N - r.F
}

func (r *Replica) ConnectToPeers() {
	done := make(chan bool)

	go r.waitForPeerConnections(done)

	for i := 0; i < int(r.Id); i++ {
		r.Peers[i] = r.connectToPeer(i)
		r.Alive[i] = true
		r.PeerReaders[i] = bufio.NewReader(r.Peers[i])
		r.PeerWriters[i] = bufio.NewWriter(r.Peers[i])
		r.Printf("OUT Connected to %d", i)
	}
	<-done
	r.Printf("Replica %d: done connecting to peers", r.Id)
	r.Printf("Node list %v", r.PeerAddrList)

	for rid, reader := range r.PeerReaders {
		if int32(rid) == r.Id {
			continue
		}
		go r.replicaListener(rid, reader)
	}
}

func (r *Replica) ConnectToPeersNoListeners() {
	done := make(chan bool)

	go r.waitForPeerConnections(done)

	for i := 0; i < int(r.Id); i++ {
		r.Peers[i] = r.connectToPeer(i)
		r.Alive[i] = true
		r.PeerReaders[i] = bufio.NewReader(r.Peers[i])
		r.PeerWriters[i] = bufio.NewWriter(r.Peers[i])
	}
	<-done
	r.Printf("Replica id: %d. Done connecting to peers\n", r.Id)
}

const peerHandshakeAck = byte(0xca)

func (r *Replica) connectToPeer(peerID int) net.Conn {
	var identity [4]byte
	binary.LittleEndian.PutUint32(identity[:], uint32(r.Id))
	dialAddress := defs.DialAddress(r.PeerAddrList[peerID])

	for {
		conn, err := net.DialTimeout("tcp", dialAddress, 3*time.Second)
		if err == nil {
			_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
			var ack [1]byte
			if _, err = conn.Write(identity[:]); err == nil {
				_, err = io.ReadFull(conn, ack[:])
			}
			if err == nil && ack[0] == peerHandshakeAck {
				_ = conn.SetDeadline(time.Time{})
				return conn
			}
			_ = conn.Close()
			if err == nil {
				err = fmt.Errorf("invalid peer handshake acknowledgement %#x", ack[0])
			}
		}
		r.Printf("Connect to peer %d via %s failed: %v; retrying", peerID, dialAddress, err)
		time.Sleep(time.Second)
	}
}

func (r *Replica) WaitForClientConnections() {
	r.Println("Waiting for client connections")

	for !r.Shutdown {
		conn, err := r.Listener.Accept()
		if err != nil {
			r.Println("Accept error:", err)
			continue
		}
		go r.clientListener(conn)
	}
}

func (r *Replica) SendMsg(peerId int32, code uint8, msg fastrpc.Serializable) {
	r.M.Lock()
	defer r.M.Unlock()

	w := r.PeerWriters[peerId]
	if w == nil {
		r.Printf("Connection to %d lost!", peerId)
		return
	}
	w.WriteByte(code)
	msg.Marshal(w)
	w.Flush()
}

func (r *Replica) SendClientMsg(id int32, code uint8, msg fastrpc.Serializable) {
	r.M.Lock()
	defer r.M.Unlock()

	w := r.ClientWriters[id]
	if w == nil {
		r.Printf("Connection to client %d lost!", id)
		return
	}
	w.WriteByte(code)
	msg.Marshal(w)
	w.Flush()
}

func (r *Replica) SendMsgNoFlush(peerId int32, code uint8, msg fastrpc.Serializable) {
	r.M.Lock()
	defer r.M.Unlock()

	w := r.PeerWriters[peerId]
	if w == nil {
		r.Printf("Connection to %d lost!", peerId)
		return
	}
	w.WriteByte(code)
	msg.Marshal(w)
}

func (r *Replica) ReplyProposeTS(reply *defs.ProposeReplyTS, w *bufio.Writer, lock *sync.Mutex) {
	r.M.Lock()
	defer r.M.Unlock()

	reply.Marshal(w)
	w.Flush()
}

func (r *Replica) SendBeacon(peerId int32) {
	r.M.Lock()
	defer r.M.Unlock()

	w := r.PeerWriters[peerId]
	if w == nil {
		r.Printf("Connection to %d lost!", peerId)
		return
	}
	w.WriteByte(defs.GENERIC_SMR_BEACON)
	beacon := &defs.Beacon{
		Timestamp: time.Now().UnixNano(),
	}
	beacon.Marshal(w)
	w.Flush()
}

func (r *Replica) ReplyBeacon(beacon *defs.GBeacon) {
	r.M.Lock()
	defer r.M.Unlock()

	w := r.PeerWriters[beacon.Rid]
	if w == nil {
		r.Printf("Connection to %d lost!", beacon.Rid)
		return
	}
	w.WriteByte(defs.GENERIC_SMR_BEACON_REPLY)
	rb := &defs.BeaconReply{
		Timestamp: beacon.Timestamp,
	}
	rb.Marshal(w)
	w.Flush()
}

func (r *Replica) UpdatePreferredPeerOrder(quorum []int32) {
	aux := make([]int32, r.N)
	i := 0
	for _, p := range quorum {
		if p == r.Id {
			continue
		}
		aux[i] = p
		i++
	}

	for _, p := range r.PreferredPeerOrder {
		found := false
		for j := 0; j < i; j++ {
			if aux[j] == p {
				found = true
				break
			}
		}
		if !found {
			aux[i] = p
			i++
		}
	}

	r.M.Lock()
	r.PreferredPeerOrder = aux
	r.M.Unlock()
}

func (r *Replica) ComputeClosestPeers() []float64 {
	npings := 20

	for j := 0; j < npings; j++ {
		for i := int32(0); i < int32(r.N); i++ {
			if i == r.Id {
				continue
			}
			r.M.Lock()
			if r.Alive[i] {
				r.M.Unlock()
				r.SendBeacon(i)
			} else {
				r.Latencies[i] = math.MaxInt64
				r.M.Unlock()
			}
		}
		time.Sleep(500 * time.Millisecond)
	}

	quorum := make([]int32, r.N)

	r.M.Lock()
	for i := int32(0); i < int32(r.N); i++ {
		pos := 0
		for j := int32(0); j < int32(r.N); j++ {
			if (r.Latencies[j] < r.Latencies[i]) ||
				((r.Latencies[j] == r.Latencies[i]) && (j < i)) {
				pos++
			}
		}
		quorum[pos] = int32(i)
	}
	r.M.Unlock()

	r.UpdatePreferredPeerOrder(quorum)

	latencies := make([]float64, r.N-1)

	for i := 0; i < r.N-1; i++ {
		node := r.PreferredPeerOrder[i]
		lat := float64(r.Latencies[node]) / float64(npings*1000000)
		r.Println(node, "->", lat, "ms")
		latencies[i] = lat
	}

	return latencies
}

func (r *Replica) waitForPeerConnections(done chan bool) {
	var b [4]byte
	bs := b[:4]

	port := strings.Split(r.PeerAddrList[r.Id], ":")[1]
	l, err := net.Listen("tcp", "0.0.0.0:"+port)
	if err != nil {
		r.Fatal(r.PeerAddrList[r.Id], err)
	}
	r.Listener = l
	expected := int32(r.N) - r.Id - 1
	for accepted := int32(0); accepted < expected; {
		conn, err := r.Listener.Accept()
		if err != nil {
			r.Println("Accept error:", err)
			continue
		}
		if _, err := io.ReadFull(conn, bs); err != nil {
			r.Println("Connection establish error:", err)
			_ = conn.Close()
			continue
		}
		id := int32(binary.LittleEndian.Uint32(bs))
		if id <= r.Id || id >= int32(r.N) || r.Peers[id] != nil {
			r.Printf("Invalid or duplicate peer identity %d", id)
			_ = conn.Close()
			continue
		}
		if _, err := conn.Write([]byte{peerHandshakeAck}); err != nil {
			r.Println("Connection acknowledgement error:", err)
			_ = conn.Close()
			continue
		}
		r.Peers[id] = conn
		r.PeerReaders[id] = bufio.NewReader(conn)
		r.PeerWriters[id] = bufio.NewWriter(conn)
		r.Alive[id] = true
		accepted++
		r.Printf("IN Connected to %d", id)
	}

	done <- true
}

func (r *Replica) replicaListener(rid int, reader *bufio.Reader) {
	var (
		msgType      uint8
		err          error = nil
		gbeacon      defs.Beacon
		gbeaconReply defs.BeaconReply
	)
	for err == nil && !r.Shutdown {
		if msgType, err = reader.ReadByte(); err != nil {
			break
		}

		switch uint8(msgType) {

		case defs.GENERIC_SMR_BEACON:
			if err = gbeacon.Unmarshal(reader); err != nil {
				break
			}
			r.ReplyBeacon(&defs.GBeacon{
				Rid:       int32(rid),
				Timestamp: gbeacon.Timestamp,
			})

		case defs.GENERIC_SMR_BEACON_REPLY:
			if err = gbeaconReply.Unmarshal(reader); err != nil {
				break
			}
			r.M.Lock()
			r.Latencies[rid] += time.Now().UnixNano() - gbeaconReply.Timestamp
			r.M.Unlock()
			now := time.Now().UnixNano()
			r.Ewma[rid] = 0.99*r.Ewma[rid] + 0.01*float64(now-gbeaconReply.Timestamp)

		default:
			p, exists := r.RPC.Get(msgType)
			if exists {
				obj := p.Obj.New()
				if err = obj.Unmarshal(reader); err != nil {
					break
				}
				notify := p.Chan
				notify <- obj
			} else {
				r.Fatal("Error: received unknown message type ", msgType, " from ", rid)
			}
		}
	}

	r.M.Lock()
	r.Alive[rid] = false
	r.M.Unlock()
}

func (r *Replica) clientListener(conn net.Conn) {
	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)

	var (
		msgType byte
		err     error
	)

	r.M.Lock()
	r.Println("Client up", conn.RemoteAddr(), "(", r.LRead, ")")
	r.M.Unlock()

	addr, identityErr := defs.ReadClientIdentity(reader)
	if identityErr != nil {
		r.Printf("Rejecting client %s: %v", conn.RemoteAddr(), identityErr)
		conn.Close()
		return
	}
	knownClient := false
	for _, endpoint := range r.Config.ClientAddrs {
		if endpoint == addr {
			knownClient = true
			break
		}
	}
	if !knownClient {
		r.Printf("Rejecting client %s with unknown endpoint identity %q", conn.RemoteAddr(), addr)
		conn.Close()
		return
	}
	isProxy := r.Config.Proxy.IsProxy(r.Alias, addr)

	mutex := &sync.Mutex{}
	for !r.Shutdown && err == nil {
		if msgType, err = reader.ReadByte(); err != nil {
			break
		}

		switch uint8(msgType) {
		case defs.PROPOSE:
			propose := &defs.Propose{}
			if err = propose.Unmarshal(reader); err != nil {
				break
			}
			r.M.Lock()
			r.ClientWriters[propose.ClientId] = writer
			r.M.Unlock()
			op := propose.Command.Op
			if r.LRead && (op == state.GET || op == state.SCAN) {
				r.ReplyProposeTS(&defs.ProposeReplyTS{
					OK:        defs.TRUE,
					CommandId: propose.CommandId,
					Value:     propose.Command.Execute(r.State),
					Timestamp: propose.Timestamp,
				}, writer, mutex)
			} else {
				gpropose := &defs.GPropose{
					Propose: propose,
					Reply:   writer,
					Mutex:   mutex,
					Proxy:   isProxy,
					Addr:    addr,
				}
				r.ProposeChan <- gpropose
			}

		case defs.READ:
			// TODO: do something with this
			read := &defs.Read{}
			if err = read.Unmarshal(reader); err != nil {
				break
			}

		case defs.PROPOSE_AND_READ:
			// TODO: do something with this
			pr := &defs.ProposeAndRead{}
			if err = pr.Unmarshal(reader); err != nil {
				break
			}

		case defs.STATS:
			r.M.Lock()
			b, _ := json.Marshal(r.Stats)
			r.M.Unlock()
			writer.Write(b)
			writer.Flush()

		default:
			p, exists := r.RPC.Get(msgType)
			if exists {
				obj := p.Obj.New()
				if err = obj.Unmarshal(reader); err != nil {
					break
				}
				notify := p.Chan
				notify <- obj
			} else {
				r.Fatal("Error: received unknown client message ", msgType)
			}
		}
	}

	conn.Close()
	r.Println("Client down", conn.RemoteAddr())
}

func Leader(ballot int32, repNum int) int32 {
	return ballot % int32(repNum)
}

func NextBallotOf(rid, oldBallot int32, repNum int) int32 {
	return (oldBallot/int32(repNum)+1)*int32(repNum) + rid
}
