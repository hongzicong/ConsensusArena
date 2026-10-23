package swift

// Protocol messages, identifiers, constants, and value helpers.
import (
	"github.com/hongzicong/ConsensusArena/replica/defs"
)

// status
const (
	NORMAL = iota
	RECOVERING
)

// phase
const (
	START = iota
	PRE_ACCEPT
	ACCEPT
	COMMIT
)

const HISTORY_SIZE = 10010001

type Dep []defs.RequestID

func (d Dep) Contains(cmdID defs.RequestID) bool {
	for _, c := range d {
		if c == cmdID {
			return true
		}
	}
	return false
}

func NilDepOfCmdID(cmdID defs.RequestID) Dep {
	return []defs.RequestID{cmdID}
}

func IsNilDepOfCmdID(cmdID defs.RequestID, dep Dep) bool {
	return len(dep) == 1 && dep[0] == cmdID
}

func (d Dep) Equals(dep2 Dep) bool {
	if len(d) != len(dep2) {
		return false
	}

	seen1 := make(map[defs.RequestID]struct{})
	seen2 := make(map[defs.RequestID]struct{})
	for i := 0; i < len(d); i++ {
		if d[i] == dep2[i] {
			continue
		}

		_, exists := seen2[d[i]]
		if exists {
			delete(seen2, d[i])
		} else {
			seen1[d[i]] = struct{}{}
		}

		_, exists = seen1[dep2[i]]
		if exists {
			delete(seen1, dep2[i])
		} else {
			seen2[dep2[i]] = struct{}{}
		}
	}

	return len(seen1) == len(seen2) && len(seen1) == 0
}

// proposal carries protocol input. context is an opaque runtime token.
type proposal struct {
	*defs.Propose
	Proxy   bool
	Addr    string
	context any
}
