package replica

import "fmt"

type QuorumI interface {
	Size() int
	Contains(int32) bool
}

type Majority int

func NewMajorityOf(N int) Majority {
	return Majority(N/2 + 1)
}

func (m Majority) Size() int {
	return int(m)
}

func (m Majority) Contains(int32) bool {
	return true
}

type ThreeQuarters int

func NewThreeQuartersOf(N int) ThreeQuarters {
	return ThreeQuarters((3*N)/4 + 1)
}

func (m ThreeQuarters) Size() int {
	return int(m)
}

func (m ThreeQuarters) Contains(int32) bool {
	return true
}

type Quorum map[int32]struct{}

type QuorumsOfLeader map[int32]Quorum

type QuorumSet map[int32]QuorumsOfLeader

func NewQuorum(size int) Quorum {
	return make(map[int32]struct{}, size)
}

func NewQuorumOfAll(size int) Quorum {
	q := NewQuorum(size)

	for i := int32(0); i < int32(size); i++ {
		q[i] = struct{}{}
	}

	return q
}

func (q Quorum) Size() int {
	return len(q)
}

func (q Quorum) Contains(repId int32) bool {
	_, exists := q[repId]
	return exists
}

func (q Quorum) copy() Quorum {
	nq := NewQuorum(len(q))

	for cmdId := range q {
		nq[cmdId] = struct{}{}
	}

	return nq
}

func (q1 Quorum) Equals(q2 Quorum) bool {
	if len(q1) != len(q2) {
		return false
	}
	for r := range q1 {
		if !q2.Contains(r) {
			return false
		}
	}
	return true
}

type QuorumSystem struct {
	qs      QuorumSet
	ballots []int32
}

// NewQuorumSystem binds the protocol's initial plan to the existing ballot space.
func NewQuorumSystem(quorumSize int, r *Replica, initial Quorum, leader int32) (*QuorumSystem, error) {
	if initial.Size() != quorumSize || !initial.Contains(leader) {
		return nil, fmt.Errorf("invalid initial Swift quorum")
	}
	for id := range initial {
		if id < 0 || int(id) >= r.N {
			return nil, fmt.Errorf("invalid quorum member %d", id)
		}
	}
	qs := NewQuorumSet(quorumSize, r.N)
	ballot := qs.BallotOf(leader, initial)
	if ballot < 0 {
		return nil, fmt.Errorf("initial quorum has no ballot")
	}
	return &QuorumSystem{qs: qs, ballots: []int32{ballot}}, nil
}

func (sys *QuorumSystem) SameHigher(sameAs, higherThan int32) int32 {
	l := Leader(sameAs, len(sys.qs))
	k := higherThan / int32(len(sys.qs[l]))
	return sameAs + k*int32(len(sys.qs[l]))*int32(len(sys.qs))
}

func (sys *QuorumSystem) BallotAt(i int) int32 {
	if len(sys.ballots) > i {
		return sys.ballots[i]
	}
	return -1
}

func (sys *QuorumSystem) BallotOf(leader int32, q Quorum) int32 {
	return sys.qs.BallotOf(leader, q)
}

func (sys QuorumSystem) AQ(ballot int32) Quorum {
	return sys.qs.AQ(ballot)
}

func NewQuorumsOfLeader() QuorumsOfLeader {
	return make(map[int32]Quorum)
}

func NewQuorumSet(quorumSize, repNum int) QuorumSet {
	return newQuorumSetAdvance(quorumSize, repNum, func(int32, int32, Quorum) {})
}

func newQuorumSetAdvance(quorumSize, repNum int, treat func(int32, int32, Quorum)) QuorumSet {
	ids := make([]int32, repNum)
	q := NewQuorum(quorumSize)
	qs := make(map[int32]QuorumsOfLeader, repNum)

	for id := range ids {
		ids[id] = int32(id)
		qs[int32(id)] = NewQuorumsOfLeader()
	}

	subsets(ids, repNum, quorumSize, 0, q, qs, treat)

	return qs
}

func (qs QuorumSet) AQ(ballot int32) Quorum {
	l := Leader(ballot, len(qs))
	lqs := qs[l]
	qid := (ballot / int32(len(qs))) % int32(len(lqs))
	return lqs[qid]
}

func (qs QuorumSet) BallotOf(leader int32, q Quorum) int32 {
	for qid, qj := range qs[leader] {
		if qj.Equals(q) {
			return qid*int32(len(qs)) + leader
		}
	}
	return -1
}

func subsets(ids []int32, repNum, quorumSize, i int, q Quorum,
	qs QuorumSet, treat func(int32, int32, Quorum)) {

	if quorumSize == 0 {
		for repId := int32(0); repId < int32(repNum); repId++ {
			length := int32(len(qs[repId]))
			_, exists := q[repId]
			if exists {
				qs[repId][length] = q.copy()
				treat(repId, length, q)
			}
		}
	}

	for j := i; j < repNum; j++ {
		q[ids[j]] = struct{}{}
		subsets(ids, repNum, quorumSize-1, j+1, q, qs, treat)
		delete(q, ids[j])
	}
}
