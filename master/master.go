package master

import (
	"errors"
	"fmt"
	"math"
	"net"
	"net/http"
	"net/rpc"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/hongzicong/ConsensusArena/config"
	"github.com/hongzicong/ConsensusArena/dlog"
	"github.com/hongzicong/ConsensusArena/replica/defs"
)

type Master struct {
	*dlog.Logger

	N               int
	port            int
	nodeList        []string
	addrList        []string
	portList        []int
	lock            *sync.Mutex
	nodes           []*rpc.Client
	leader          []bool
	alive           []bool
	latencies       []float64
	registered      []bool
	registeredCount int
	membership      *config.Membership
	finishInit      bool
	initCond        *sync.Cond
	nextLeader      int
	config          *config.Config
}

func New(N, port int, conf *config.Config, logger *dlog.Logger) *Master {
	membership, err := conf.Membership()
	if err != nil {
		panic(err)
	}
	master := &Master{
		Logger: logger,

		N:          N,
		port:       port,
		nodeList:   make([]string, N),
		addrList:   make([]string, N),
		portList:   make([]int, N),
		lock:       new(sync.Mutex),
		nodes:      make([]*rpc.Client, N),
		leader:     make([]bool, N),
		alive:      make([]bool, N),
		latencies:  make([]float64, N),
		registered: make([]bool, N),
		membership: membership,
		finishInit: false,
		nextLeader: -1,
		config:     conf,
	}
	if len(conf.ReplicaAliases) != N {
		panic(fmt.Sprintf("configured replica order has %d entries, expected %d",
			len(conf.ReplicaAliases), N))
	}
	master.initCond = sync.NewCond(master.lock)
	return master
}

func (m *Master) Run() {
	m.Printf("master starting on port %d", m.port)
	m.Printf("waiting for %d replicas", m.N)

	rpc.Register(m)
	rpc.HandleHTTP()
	l, err := net.Listen("tcp", fmt.Sprintf(":%d", m.port))
	if err != nil {
		m.Fatal("master listen error:", err)
	}
	go m.run()
	http.Serve(l, nil)
}

func (m *Master) run() {
	for {
		m.lock.Lock()
		if m.registeredCount == m.N {
			m.lock.Unlock()
			break
		}
		m.lock.Unlock()
		time.Sleep(time.Second)
	}
	time.Sleep(2 * time.Second)

	for i := 0; i < m.N; {
		var err error
		addr := fmt.Sprintf("%s:%d", m.addrList[i], m.portList[i]+1000)
		m.nodes[i], err = rpc.DialHTTP("tcp", addr)
		if err != nil {
			m.Printf("error connecting to replica %d (%v), retrying...", i, addr)
			time.Sleep(time.Second)
		} else {
			btlReply := defs.NewBeTheLeaderReply()
			if m.leader[i] {
				err = m.nodes[i].Call("Replica.BeTheLeader", &defs.BeTheLeaderArgs{}, btlReply)
				if err != nil {
					m.Fatal("Not today Zurg!")
				}
				defs.UpdateBeTheLeaderReply(btlReply)
				leaderI := i
				if btlReply.Leader != -1 {
					leaderI = int(btlReply.Leader)
					m.leader[i] = false
					m.leader[leaderI] = true
				}
				m.nextLeader = int(btlReply.NextLeader)
				if leaderI == i {
					m.Printf("replica %d is the new leader", leaderI)
				} else if leaderI < i {
					i = leaderI - 1
				}
			}
			i++
		}
	}

	var new_leader bool
	pingNode := func(i int, node *rpc.Client) {
		err := node.Call("Replica.Ping", &defs.PingArgs{}, &defs.PingReply{})
		if err != nil {
			m.alive[i] = false
			if m.leader[i] {
				new_leader = true
				m.leader[i] = false
			}
		} else {
			m.alive[i] = true
		}
	}
	m.lock.Lock()
	for i, node := range m.nodes {
		pingNode(i, node)
	}
	// initialization is finished
	// (i.e., `alive` has been computed)
	m.finishInit = true
	m.initCond.Broadcast()
	m.lock.Unlock()

	beTheLeader := func(i int) error {
		if m.alive[i] {
			btlReply := defs.NewBeTheLeaderReply()
			err := m.nodes[i].Call("Replica.BeTheLeader", &defs.BeTheLeaderArgs{}, btlReply)
			if err == nil {
				defs.UpdateBeTheLeaderReply(btlReply)
				leaderI := i
				if btlReply.Leader != -1 {
					// TODO: need to set leader[old_leader] to false?
					leaderI = int(btlReply.Leader)
				}
				m.leader[leaderI] = true
				m.nextLeader = int(btlReply.NextLeader)
				m.Printf("replica %d is the new leader", leaderI)
				return nil
			}
			return err
		}
		return errors.New("dead")
	}

	for {
		time.Sleep(3 * time.Second)
		new_leader = false
		for i, node := range m.nodes {
			pingNode(i, node)
		}

		if !new_leader {
			continue
		}
		if m.nextLeader != -1 {
			if beTheLeader(m.nextLeader) == nil {
				continue
			}
		}
		for i := range m.nodes {
			if beTheLeader(i) == nil {
				break
			}
		}
	}
}

func (m *Master) Register(args *defs.RegisterArgs, reply *defs.RegisterReply) error {
	m.lock.Lock()
	defer m.lock.Unlock()

	addrPort := net.JoinHostPort(args.Addr, strconv.Itoa(args.Port))
	index, err := m.registrationIndex(args.Alias, addrPort)
	if err != nil {
		return err
	}

	if !m.registered[index] {
		m.nodeList[index] = addrPort
		m.addrList[index] = args.Addr
		m.portList[index] = args.Port
		m.leader[index] = false
		m.registered[index] = true
		m.registeredCount++

		addr := args.Addr
		if addr == "" {
			addr = "127.0.0.1"
		}
		out, err := exec.Command("ping", addr, "-c 2", "-q").Output()
		if err == nil {
			m.latencies[index], _ =
				strconv.ParseFloat(strings.Split(string(out), "/")[4], 64)
			m.Printf("node %v [%v] -> %v", index,
				m.nodeList[index], m.latencies[index])
		} else {
			m.Fatal("cannot connect to " + addr)
		}
	}

	if m.registeredCount == m.N {
		reply.Ready = true
		reply.ReplicaId = index
		reply.NodeList = m.nodeList
		reply.IsLeader = false

		minLatency := math.MaxFloat64
		leader := -1

		if m.config.Leader != nil {
			if addrPort == *m.config.Leader {
				leader = index
			}
		} else {
			for i := 0; i < len(m.leader); i++ {
				if m.latencies[i] < minLatency {
					minLatency = m.latencies[i]
					leader = i
				}
			}
		}

		if leader == index {
			m.leader[index] = true
			reply.IsLeader = true
		}

	} else {
		reply.Ready = false
	}

	return nil
}

func (m *Master) registrationIndex(alias, addrPort string) (int, error) {
	index, err := m.membership.RegistrationID(alias, addrPort)
	if err != nil {
		return 0, err
	}
	if m.registered[index] && m.nodeList[index] != addrPort {
		return 0, fmt.Errorf("replica %q changed endpoint from %q to %q",
			alias, m.nodeList[index], addrPort)
	}
	return index, nil
}

func (m *Master) GetLeader(args *defs.GetLeaderArgs, reply *defs.GetLeaderReply) error {
	m.lock.Lock()
	defer m.lock.Unlock()
	reply.LeaderId = -1 // gob must transmit an explicit nonzero no-leader sentinel.

	for i, l := range m.leader {
		if l {
			*reply = defs.GetLeaderReply{
				LeaderId: i,
			}
			break
		}
	}
	return nil
}

func (m *Master) GetReplicaList(args *defs.GetReplicaListArgs, reply *defs.GetReplicaListReply) error {
	m.lock.Lock()

	for !m.finishInit {
		m.initCond.Wait()
	}

	if m.registeredCount == m.N {
		reply.Ready = true
	} else {
		reply.Ready = false
	}

	reply.ReplicaList = make([]string, 0)
	reply.AliveList = make([]bool, 0)
	for i, node := range m.nodeList {
		reply.ReplicaList = append(reply.ReplicaList, node)
		reply.AliveList = append(reply.AliveList, m.alive[i])
	}

	m.Printf("nodes list %v", reply.ReplicaList)
	m.lock.Unlock()
	return nil
}
