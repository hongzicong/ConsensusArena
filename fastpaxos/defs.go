package fastpaxos

// Protocol messages, identifiers, constants, and value helpers.
import (
	"time"

	"github.com/hongzicong/ConsensusArena/replica/defs"
	"github.com/hongzicong/ConsensusArena/state"
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

type record struct {
	ID      defs.RequestID
	Command state.Command
	Noop    bool
}

func sameValue(a, b record) bool { return a.Noop == b.Noop && (a.Noop || a.ID == b.ID) }

type entry struct {
	Slot             int
	Epoch, Round     uint64
	Value            record
	Payload, Decided bool
}

type message struct {
	Kind                     uint8
	From                     int
	Epoch                    uint64
	Start, High, Page, Pages int
	Entries                  []entry
}

type envelope struct {
	To      int
	Message message
}

func newer(a, b entry) bool { return a.Epoch > b.Epoch || a.Epoch == b.Epoch && a.Round > b.Round }
