package fastpaxos

// Protocol messages, identifiers, constants, and value helpers.
import (
	"time"
)

const (
	msgVote uint8 = iota
	msgPrepare
	msgPromise
	msgAccept
	msgAccepted
	msgCommit
	msgHeartbeat
	msgFetch
	msgData
	msgForward
	msgNack
	msgRecover
)

const pageSize = 128

const repairBudget = 32 * pageSize

const inflightWindow = 16384

const fastWindow = 8192

const failureTimeout = 3 * time.Second

// A fast quorum is any set strictly larger than three quarters of membership.
func fastQuorumSize(n int) int { return 3*n/4 + 1 }

func sameValue(a, b record) bool { return a.Noop == b.Noop && (a.Noop || a.ID == b.ID) }

type envelope struct {
	To      int
	Message message
}

func newer(a, b entry) bool { return a.Epoch > b.Epoch || a.Epoch == b.Epoch && a.Round > b.Round }
