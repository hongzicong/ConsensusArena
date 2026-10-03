package epaxos

// Protocol messages, identifiers, constants, and value helpers.
import (
	"bytes"
	"time"

	"github.com/hongzicong/ConsensusArena/state"
)

type Prepare struct {
	LeaderId int32
	Replica  int32
	Instance int32
	Ballot   int32
}

type PrepareReply struct {
	AcceptorId int32
	Replica    int32
	Instance   int32
	Ballot     int32
	VBallot    int32
	Status     int8
	Command    []state.Command
	Seq        int32
	Deps       []int32
}

type PreAccept struct {
	LeaderId int32
	Replica  int32
	Instance int32
	Ballot   int32
	Command  []state.Command
	Seq      int32
	Deps     []int32
}

type PreAcceptReply struct {
	Replica       int32
	Instance      int32
	Ballot        int32
	VBallot       int32
	Seq           int32
	Deps          []int32
	CommittedDeps []int32
	Status        int8
	AcceptorId    int32
}

type PreAcceptOK struct {
	Instance int32
}

type Accept struct {
	LeaderId int32
	Replica  int32
	Instance int32
	Ballot   int32
	Seq      int32
	Deps     []int32
	Command  []state.Command
}

type AcceptReply struct {
	Replica    int32
	Instance   int32
	Ballot     int32
	AcceptorId int32
}

type Commit struct {
	LeaderId int32
	Replica  int32
	Instance int32
	Ballot   int32
	Command  []state.Command
	Seq      int32
	Deps     []int32
}

type TryPreAccept struct {
	LeaderId int32
	Replica  int32
	Instance int32
	Ballot   int32
	Command  []state.Command
	Seq      int32
	Deps     []int32
}

type TryPreAcceptReply struct {
	AcceptorId       int32
	Replica          int32
	Instance         int32
	Ballot           int32
	VBallot          int32
	ConflictReplica  int32
	ConflictInstance int32
	ConflictStatus   int8
}

const (
	NONE int8 = iota
	PREACCEPTED
	PREACCEPTED_EQ
	ACCEPTED
	COMMITTED
	EXECUTED
)

const MAX_INSTANCE = 10 * 1024 * 1024

const MAX_DEPTH_DEP = 10

const TRUE = uint8(1)

const FALSE = uint8(0)

const ADAPT_TIME_SEC = 10

const COMMIT_GRACE_PERIOD = 3 * time.Second

const MAX_BATCH = 1000

// Bound the unexecuted local suffix. With compressed dependencies, an
// unlimited proposal stream can keep extending an open SCC faster than its
// last dependencies commit. Backpressure lets that component close.
const MAX_INFLIGHT_INSTANCES = 32

const INITIAL_RECOVERY_BACKOFF = 3 * time.Second

const MAX_RECOVERY_BACKOFF = 6 * time.Second

const BF_K = 4

const BF_M_N = 32.0

const HT_INIT_SIZE = 200000

type repairRequest struct{ Owner, Slot int32 }

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
