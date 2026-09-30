package epaxos

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/hongzicong/ConsensusArena/replica/defs"
	fastrpc "github.com/hongzicong/ConsensusArena/rpc"
	"github.com/hongzicong/ConsensusArena/state"
)

type repairState struct {
	sends       []chan []byte
	sendHook    func(int32, uint8, fastrpc.Serializable)
	blocked     map[uint64]time.Time
	active      map[instanceId]bool
	replies     map[*bufio.Writer]*replyPipe
	requestChan chan fastrpc.Serializable
	requestRPC  uint8
	nextGapScan time.Time
}

func newRepairState() *repairState {
	return &repairState{blocked: make(map[uint64]time.Time), active: make(map[instanceId]bool), replies: make(map[*bufio.Writer]*replyPipe), requestChan: make(chan fastrpc.Serializable, 4096)}
}
func (r *Replica) peerAlive(id int32) bool {
	r.M.Lock()
	defer r.M.Unlock()
	return id == r.Id || r.Alive[id]
}
func (r *Replica) recoveryCoordinator(owner int32) int32 {
	if r.peerAlive(owner) {
		return owner
	}
	for i := int32(0); i < int32(r.N); i++ {
		if r.peerAlive(i) {
			return i
		}
	}
	return r.Id
}

func (r *Replica) fastQuorumAvailable() bool {
	live := 0
	for id := int32(0); id < int32(r.N); id++ {
		if r.peerAlive(id) {
			live++
		}
	}
	return live >= r.Replica.FastQuorumSize()
}
func (r *Replica) startSenders() {
	r.sends = make([]chan []byte, r.N)
	for i := range r.sends {
		if int32(i) == r.Id {
			continue
		}
		r.sends[i] = make(chan []byte, 16384)
		go func(id int) {
			for b := range r.sends[id] {
				if _, err := io.Copy(r.Peers[id], bytes.NewReader(b)); err != nil {
					r.Peers[id].Close()
					return
				}
			}
		}(i)
	}
}

// Copy the wire image before leaving the serialized state machine: slices in
// legacy messages otherwise alias attributes that the next transition mutates.
func (r *Replica) SendMsg(id int32, code uint8, msg fastrpc.Serializable) {
	if r.sendHook != nil {
		r.sendHook(id, code, msg)
		return
	}
	if id == r.Id || !r.peerAlive(id) {
		return
	}
	var b bytes.Buffer
	b.WriteByte(code)
	msg.Marshal(&b)
	select {
	case r.sends[id] <- b.Bytes():
	default:
		r.Stats.M["sendQueueDrops"]++
	}
}

type replyPipe struct {
	mu   sync.Mutex
	wake chan struct{}
	jobs []replyTask
}
type replyTask struct {
	reply defs.ProposeReplyTS
	lock  *sync.Mutex
}

func (r *Replica) ReplyProposeTS(reply *defs.ProposeReplyTS, w *bufio.Writer, lock *sync.Mutex) {
	p := r.replies[w]
	if p == nil {
		p = &replyPipe{wake: make(chan struct{}, 1)}
		r.replies[w] = p
		go func() {
			for range p.wake {
				for {
					p.mu.Lock()
					if len(p.jobs) == 0 {
						p.mu.Unlock()
						break
					}
					j := p.jobs[0]
					p.jobs[0] = replyTask{}
					p.jobs = p.jobs[1:]
					p.mu.Unlock()
					j.lock.Lock()
					j.reply.Marshal(w)
					w.Flush()
					j.lock.Unlock()
				}
			}
		}()
	}
	cp := *reply
	cp.Value = append(state.Value(nil), reply.Value...)
	p.mu.Lock()
	p.jobs = append(p.jobs, replyTask{cp, lock})
	p.mu.Unlock()
	select {
	case p.wake <- struct{}{}:
	default:
	}
}

type repairRequest struct{ Owner, Slot int32 }

func (*repairRequest) New() fastrpc.Serializable { return &repairRequest{} }
func (m *repairRequest) Marshal(w io.Writer) {
	binary.Write(w, binary.LittleEndian, m.Owner)
	binary.Write(w, binary.LittleEndian, m.Slot)
}
func (m *repairRequest) Unmarshal(rd io.Reader) error {
	if err := binary.Read(rd, binary.LittleEndian, &m.Owner); err != nil {
		return err
	}
	return binary.Read(rd, binary.LittleEndian, &m.Slot)
}
func (r *Replica) handleRepairRequest(m *repairRequest) {
	if m.Owner < 0 || m.Owner >= int32(r.N) || m.Slot < 0 || m.Slot >= MAX_INSTANCE {
		return
	}
	i := r.InstanceSpace[m.Owner][m.Slot]
	if i != nil && i.Status >= COMMITTED && i.Cmds != nil {
		c := &Commit{r.Id, m.Owner, m.Slot, i.vbal, i.Cmds, i.Seq, i.Deps}
		for q := int32(0); q < int32(r.N); q++ {
			if q != r.Id {
				r.SendMsg(q, r.commitRPC, c)
			}
		}
		return
	}
	if r.recoveryCoordinator(m.Owner) == r.Id {
		r.scheduleRecovery(m.Owner, m.Slot, time.Now())
	}
}
func (r *Replica) blockedOn(owner, slot int32, now time.Time) {
	if slot > r.crtInstance[owner] {
		r.crtInstance[owner] = slot
	}
	key := recoveryKey(owner, slot)
	first, ok := r.blocked[key]
	if !ok {
		r.blocked[key] = now
		r.Stats.M["dependencyBlocks"]++
		return
	}
	if now.Sub(first) < COMMIT_GRACE_PERIOD {
		return
	}
	r.blocked[key] = now
	target := r.recoveryCoordinator(owner)
	if target == r.Id {
		r.scheduleRecovery(owner, slot, now)
	} else {
		r.SendMsg(target, r.requestRPC, &repairRequest{owner, slot})
	}
}
func (r *Replica) executeReady(now time.Time) {
	for q := int32(0); q < int32(r.N); q++ {
		for budget := 0; budget < 128; budget++ {
			i := r.ExecedUpTo[q] + 1
			if i > r.crtInstance[q] {
				break
			}
			inst := r.InstanceSpace[q][i]
			if inst != nil && inst.Status == EXECUTED {
				r.ExecedUpTo[q]++
				delete(r.blocked, recoveryKey(q, i))
				continue
			}
			if inst == nil || inst.Status < COMMITTED || inst.Cmds == nil {
				r.blockedOn(q, i, now)
				break
			}
			if !r.exec.executeCommand(q, i) {
				break
			}
		}
	}
}
func (r *Replica) repairTick(now time.Time) {
	if !now.Before(r.nextGapScan) {
		r.nextGapScan = now.Add(200 * time.Millisecond)
		// Repair a bounded suffix in parallel. Waiting for DFS to uncover
		// one predecessor every grace period takes minutes after an owner
		// crashes with a window of unfinished instances.
		for owner := int32(0); owner < int32(r.N); owner++ {
			end := r.crtInstance[owner]
			if limit := r.ExecedUpTo[owner] + 256; end > limit {
				end = limit
			}
			for slot := r.ExecedUpTo[owner] + 1; slot <= end; slot++ {
				inst := r.InstanceSpace[owner][slot]
				if inst == nil || inst.Status < COMMITTED || inst.Cmds == nil {
					r.blockedOn(owner, slot, now)
				}
			}
		}
	}
	// Retry every discovered gap, even if a later DFS encounters a different
	// endpoint first. Otherwise an open component can continually move the
	// traversal frontier and starve recovery of earlier missing instances.
	for key := range r.blocked {
		owner, slot := int32(key>>32), int32(key)
		inst := r.InstanceSpace[owner][slot]
		if inst != nil && inst.Status >= COMMITTED && inst.Cmds != nil {
			delete(r.blocked, key)
			continue
		}
		r.blockedOn(owner, slot, now)
	}
	for id := range r.active {
		inst := r.InstanceSpace[id.replica][id.instance]
		if inst == nil || inst.Status >= COMMITTED || inst.lb == nil {
			delete(r.active, id)
			continue
		}
		lb := inst.lb
		if inst.bal != lb.lastTriedBallot {
			continue
		}
		age := now.Sub(lb.phaseStarted)
		if age < time.Second {
			continue
		}
		if !lb.preparing && !lb.tryingToPreAccept && (lb.status == PREACCEPTED || lb.status == PREACCEPTED_EQ) && lb.preAcceptOKs >= r.N/2 {
			lb.status = ACCEPTED
			inst.Status = ACCEPTED
			inst.Cmds = lb.cmds
			inst.Seq = lb.seq
			inst.Deps = append([]int32(nil), lb.deps...)
			inst.vbal = lb.lastTriedBallot
			lb.ballot = lb.lastTriedBallot
			r.bcastAccept(id.replica, id.instance)
			lb.phaseStarted = now
		} else if age >= COMMIT_GRACE_PERIOD {
			if r.recoveryCoordinator(id.replica) == r.Id {
				r.startRecoveryForInstance(id.replica, id.instance)
			}
		}
	}
}

func allPossible(n int) []bool {
	p := make([]bool, n)
	for i := range p {
		p[i] = true
	}
	return p
}

func sameCommands(a, b []state.Command) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].Op != b[i].Op || a[i].K != b[i].K || !bytes.Equal(a[i].V, b[i].V) {
			return false
		}
	}
	return true
}
func writeCommandBatch(w io.Writer, commands []state.Command) {
	binary.Write(w, binary.LittleEndian, uint32(len(commands)))
	for i := range commands {
		commands[i].Marshal(w)
	}
}
func readCommandBatch(rd io.Reader) ([]state.Command, error) {
	var n uint32
	if err := binary.Read(rd, binary.LittleEndian, &n); err != nil {
		return nil, err
	}
	if n > MAX_BATCH {
		return nil, fmt.Errorf("oversized EPaxos command batch")
	}
	cs := make([]state.Command, n)
	for i := range cs {
		if err := cs[i].Unmarshal(rd); err != nil {
			return nil, err
		}
	}
	return cs, nil
}
