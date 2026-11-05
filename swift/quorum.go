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
	family  quorumFamily
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
	family := quorumFamily{size: quorumSize, replicas: repNum}
	ballot := family.ballotOf(leader, initial)
	if ballot < 0 || !family.aq(ballot).Equal(initial) {
		return nil, fmt.Errorf("initial quorum has no ballot")
	}
	return &QuorumSystem{family: family, ballots: []int32{ballot}}, nil
}

func (sys *QuorumSystem) SameHigher(sameAs, higherThan int32) int32 {
	// Retain the original int32 arithmetic and cycle calculation exactly.
	count := int32(sys.family.count())
	k := higherThan / count
	return sameAs + k*count*int32(sys.family.replicas)
}

func (sys *QuorumSystem) BallotAt(i int) int32 {
	if len(sys.ballots) > i {
		return sys.ballots[i]
	}
	return -1
}

func (sys *QuorumSystem) BallotOf(leader int32, q Quorum) int32 {
	return sys.family.ballotOf(leader, q)
}

func (sys QuorumSystem) AQ(ballot int32) Quorum {
	return sys.family.aq(ballot)
}

// quorumFamily represents precisely the lexicographic, leader-containing subset
// order used by subsets below. Removing the fixed leader preserves that order:
// quorum IDs are combination ranks among C(n-1,k-1) subsets. No tables of actual
// quorums, cached mutable sets, or new wire/ballot encodings are introduced.
type quorumFamily struct{ replicas, size int }

var binomial = func() [65][65]uint64 {
	var table [65][65]uint64
	for n := 0; n <= 64; n++ {
		table[n][0], table[n][n] = 1, 1
		for k := 1; k < n; k++ {
			table[n][k] = table[n-1][k-1] + table[n-1][k]
		}
	}
	return table
}()

func choose(n, k int) uint64 {
	if n < 0 || k < 0 || k > n {
		return 0
	}
	return binomial[n][k]
}

func (f quorumFamily) count() uint64 { return choose(f.replicas-1, f.size-1) }

func (f quorumFamily) ballotOf(leader int32, q Quorum) int32 {
	if leader < 0 || int(leader) >= f.replicas || q.Size() != f.size ||
		!q.Contains(int(leader)) || !replicaset.All(f.replicas).Covers(q) {
		return -1
	}
	var rank uint64
	previous, position := -1, 0
	q.Range(func(id int) bool {
		if id == int(leader) {
			return true
		}
		index := id
		if id > int(leader) {
			index--
		}
		for skipped := previous + 1; skipped < index; skipped++ {
			rank += choose(f.replicas-2-skipped, f.size-2-position)
		}
		previous, position = index, position+1
		return true
	})
	// Deliberately keep the legacy signed int32 mapping. NewQuorumSystem rejects
	// an initial mapping if overflow would alias a different quorum on decode.
	return int32(rank)*int32(f.replicas) + leader
}

func (f quorumFamily) aq(ballot int32) Quorum {
	leader := Leader(ballot, f.replicas)
	if leader < 0 {
		panic("negative Swift quorum ballot")
	}
	// The modulus's int32 conversion also matches the legacy implementation.
	rank := uint64((ballot / int32(f.replicas)) % int32(f.count()))
	q := replicaset.New(int(leader))
	needed := f.size - 1
	for index := 0; needed > 0 && index < f.replicas-1; index++ {
		block := choose(f.replicas-2-index, needed-1)
		if rank >= block {
			rank -= block
			continue
		}
		id := index
		if id >= int(leader) {
			id++
		}
		q.Add(id)
		needed--
	}
	return q
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
		for repID := int32(0); repID < int32(repNum); repID++ {
			length := int32(len(qs[repID]))
			exists := q.Contains(int(repID))
			if exists {
				qs[repID][length] = q
				treat(repID, length, q)
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
