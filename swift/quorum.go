package swift

// Swift's leader-containing quorum families and ballot mapping. Replica identity
// storage is shared; quorum selection and thresholds remain protocol policy.
import (
	"fmt"

	"github.com/hongzicong/ConsensusArena/replicaset"
)

type QuorumI interface {
	Size() int
	Contains(int) bool
}

type Majority int

func NewMajorityOf(N int) Majority {
	return Majority(N/2 + 1)
}

func (m Majority) Size() int {
	return int(m)
}

func (m Majority) Contains(int) bool {
	return true
}

type ThreeQuarters int

func NewThreeQuartersOf(N int) ThreeQuarters {
	return ThreeQuarters((3*N)/4 + 1)
}

func (m ThreeQuarters) Size() int {
	return int(m)
}

func (m ThreeQuarters) Contains(int) bool {
	return true
}

type Quorum = replicaset.Set

type QuorumsOfLeader map[int32]Quorum

type QuorumSet map[int32]QuorumsOfLeader

type QuorumSystem struct {
	qs      QuorumSet
	ballots []int32
}

// NewQuorumSystem binds the protocol's initial plan to the existing ballot space.
func NewQuorumSystem(quorumSize, repNum int, initial Quorum, leader int32) (*QuorumSystem, error) {
	if initial.Size() != quorumSize || !initial.Contains(int(leader)) {
		return nil, fmt.Errorf("invalid initial Swift quorum")
	}
	if repNum < 1 || repNum > replicaset.MaxSize || !replicaset.All(repNum).Covers(initial) {
		return nil, fmt.Errorf("invalid quorum membership %d", repNum)
	}
	qs := NewQuorumSet(quorumSize, repNum)
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
	q := replicaset.New()
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
		if qj.Equal(q) {
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
			exists := q.Contains(int(repId))
			if exists {
				qs[repId][length] = q
				treat(repId, length, q)
			}
		}
	}

	for j := i; j < repNum; j++ {
		q.Add(int(ids[j]))
		subsets(ids, repNum, quorumSize-1, j+1, q, qs, treat)
		q.Remove(int(ids[j]))
	}
}

func Leader(ballot int32, repNum int) int32 {
	return ballot % int32(repNum)
}

func NextBallotOf(rid, oldBallot int32, repNum int) int32 {
	return (oldBallot/int32(repNum)+1)*int32(repNum) + rid
}
