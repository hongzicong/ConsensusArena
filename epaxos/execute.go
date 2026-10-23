package epaxos

// Execution ordering, result deduplication, and protocol completion.
import (
	"sort"
	"time"

	"github.com/hongzicong/ConsensusArena/state"
)

const (
	WHITE int8 = iota
	GRAY
	BLACK
)

type executionScheduler struct {
	core  *core
	stack []*Instance
}

type SCComponent struct {
	nodes []*Instance
	color int8
}

func (s *executionScheduler) executeCommand(replica int32, instance int32) bool {
	if s.core.instanceSpace[replica][instance] == nil {
		return false
	}
	inst := s.core.instanceSpace[replica][instance]
	if inst.Status == EXECUTED {
		return true
	}
	if inst.Status != COMMITTED {
		return false
	}

	if !s.findSCC(inst) {
		return false
	}

	return true
}

func (s *executionScheduler) findSCC(root *Instance) bool {
	index := 1
	// find SCCs using Tarjan's algorithm
	s.stack = s.stack[0:0]
	ret := s.strongconnect(root, &index)
	// reset all indexes in the stack
	for j := 0; j < len(s.stack); j++ {
		s.stack[j].Index = 0
		s.stack[j].onStack = false
	}
	return ret
}

func (s *executionScheduler) strongconnect(v *Instance, index *int) bool {
	v.Index = *index
	v.Lowlink = *index
	*index = *index + 1

	l := len(s.stack)
	if l == cap(s.stack) {
		newSlice := make([]*Instance, l, 2*l)
		copy(newSlice, s.stack)
		s.stack = newSlice
	}
	s.stack = s.stack[0 : l+1]
	s.stack[l] = v
	v.onStack = true

	if v.Cmds == nil {
		s.core.blockedOn(v.id.replica, v.id.instance, time.Now())
		return false
	}

	for q := int32(0); q < int32(s.core.n); q++ {
		inst := v.Deps[q]
		// Every newly proposed/recovered row instance explicitly depends on
		// its predecessor. Following the endpoint therefore reaches the full
		// dependency prefix without rescanning it at every DFS vertex.
		for i := inst; i > s.core.executedUpTo[q]; i = -1 {
			if s.core.instanceSpace[q][i] == nil || s.core.instanceSpace[q][i].Cmds == nil {
				s.core.blockedOn(q, i, time.Now())
				return false
			}

			if s.core.transconf {
				for _, alpha := range v.Cmds {
					for _, beta := range s.core.instanceSpace[q][i].Cmds {
						if !state.Conflict(&alpha, &beta) {
							continue
						}
					}
				}
			}

			if s.core.instanceSpace[q][i].Status == EXECUTED {
				continue
			}

			for s.core.instanceSpace[q][i].Status != COMMITTED {
				s.core.blockedOn(q, i, time.Now())
				return false
			}

			w := s.core.instanceSpace[q][i]

			if w.Index == 0 {
				if !s.strongconnect(w, index) {
					return false
				}
				if w.Lowlink < v.Lowlink {
					v.Lowlink = w.Lowlink
				}
			} else if w.onStack {
				if w.Index < v.Lowlink {
					v.Lowlink = w.Index
				}
			}
		}
	}

	if v.Lowlink == v.Index {
		//found SCC
		list := s.stack[l:]

		//execute commands in the increasing order of the Seq field
		sort.Sort(nodeArray(list))
		for _, w := range list {
			for idx := 0; idx < len(w.Cmds); idx++ {
				shouldRespond := s.core.replyAfterExecution() && w.lb != nil && w.lb.clientProposals != nil
				if w.Cmds[idx].Op == state.NONE {
					// nothing to do
				} else if shouldRespond {
					val := s.core.execute(w.Cmds[idx])
					_ = s.core.replyResult(w.lb.clientProposals[idx], val, -1)
					s.core.m.Lock()
					s.core.stats.M["clientReplies"]++
					s.core.m.Unlock()
				} else if w.Cmds[idx].Op == state.PUT {
					s.core.execute(w.Cmds[idx])
				}
			}
			w.Status = EXECUTED
			w.onStack = false
			s.core.m.Lock()
			s.core.stats.M["executedCommands"] += len(w.Cmds)
			s.core.m.Unlock()
		}
		s.stack = s.stack[0:l]
	}

	return true
}

type nodeArray []*Instance

func (na nodeArray) Len() int {
	return len(na)
}

func (na nodeArray) Less(i, j int) bool {
	return na[i].Seq < na[j].Seq || (na[i].Seq == na[j].Seq && na[i].id.replica < na[j].id.replica) || (na[i].Seq == na[j].Seq && na[i].id.replica == na[j].id.replica && na[i].id.instance < na[j].id.instance)
}

func (na nodeArray) Swap(i, j int) {
	na[i], na[j] = na[j], na[i]
}

func (c *core) apply(now time.Time) {
	for q := int32(0); q < int32(c.n); q++ {
		for budget := 0; budget < 128; budget++ {
			i := c.executedUpTo[q] + 1
			if i > c.crtInstance[q] {
				break
			}
			inst := c.instanceSpace[q][i]
			if inst != nil && inst.Status == EXECUTED {
				c.executedUpTo[q]++
				delete(c.blocked, recoveryKey(q, i))
				continue
			}
			if inst == nil || inst.Status < COMMITTED || inst.Cmds == nil {
				c.blockedOn(q, i, now)
				break
			}
			if !c.scheduler.executeCommand(q, i) {
				break
			}
		}
	}
}
