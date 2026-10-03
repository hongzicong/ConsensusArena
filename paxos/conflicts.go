package paxos

// Protocol-local conflict indexing for point operations and scans.
import (
	"encoding/binary"

	"github.com/hongzicong/ConsensusArena/replica/defs"
	"github.com/hongzicong/ConsensusArena/state"
)

// Exact key index for point operations; scans retain their original range
// conflict predicate. It changes the cost, not the commutativity test.
type conflictIndex struct {
	all, writes map[state.Key]int
	scans       map[defs.RequestID]Request
}

func newConflictIndex() *conflictIndex {
	return &conflictIndex{map[state.Key]int{}, map[state.Key]int{}, map[defs.RequestID]Request{}}
}

func (x *conflictIndex) add(r Request) {
	if r.Command.Op == state.NONE {
		return
	}
	if r.Command.Op == state.SCAN {
		x.scans[r.ID] = r
		return
	}
	x.all[r.Command.K]++
	if r.Command.Op == state.PUT {
		x.writes[r.Command.K]++
	}
}

func (x *conflictIndex) remove(r Request) {
	if r.Command.Op == state.NONE {
		return
	}
	if r.Command.Op == state.SCAN {
		delete(x.scans, r.ID)
		return
	}
	k := r.Command.K
	x.all[k]--
	if x.all[k] == 0 {
		delete(x.all, k)
	}
	if r.Command.Op == state.PUT {
		x.writes[k]--
		if x.writes[k] == 0 {
			delete(x.writes, k)
		}
	}
}

func (x *conflictIndex) conflicts(r Request) bool {
	switch r.Command.Op {
	case state.NONE:
		return false
	case state.GET:
		return x.writes[r.Command.K] > 0
	case state.PUT:
		if x.all[r.Command.K] > 0 {
			return true
		}
		for _, s := range x.scans {
			if state.Conflict(&s.Command, &r.Command) {
				return true
			}
		}
	case state.SCAN:
		end := r.Command.K + state.Key(binary.LittleEndian.Uint64(r.Command.V))
		for k := range x.writes {
			if k >= r.Command.K && k <= end {
				return true
			}
		}
	}
	return false
}
