package epaxos

// Protocol messages, identifiers, constants, and value helpers.
import (
	"bytes"
	"time"

	"github.com/hongzicong/ConsensusArena/replica/defs"

	"github.com/hongzicong/ConsensusArena/state"
)

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

// proposal carries protocol input. context is an opaque runtime token.
type proposal struct {
	*defs.Propose
	context any
}
