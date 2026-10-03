// Package protocol defines the event boundary shared by replica implementations.
// It contains no voting, ordering, recovery, or placement policy.
package protocol

import (
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/hongzicong/ConsensusArena/replica/defs"
)

// Machine is driven by the owning replica event loop, never by network writers.
// Handle accepts decoded wire messages and protocol-local control events; each
// implementation validates the concrete type. Outputs retain their native
// synchronous callbacks/queues: returning from a transition does not define a
// new send barrier. In particular, local delivery may reenter Handle.
//
// now is the event observation time. Existing algorithms may retain additional
// internal clock reads and workers; this boundary alone is not a pure simulator.
type Machine interface {
	Propose(*defs.GPropose, time.Time) error
	Handle(any, time.Time) error
	Tick(Tick) error
}

type Timer uint8

const (
	Maintenance Timer = iota
	Batch
	Slow
)

// Tick carries the original scheduler's time and optional observations. Elapsed
// uses that runtime's startup origin. Alive is a snapshot, not a failure oracle.
// A driver must preserve the protocol's timer kinds, periods, and clock source.
type Tick struct {
	Kind    Timer
	Now     time.Time
	Elapsed time.Duration
	Alive   []bool
}

var ErrInvalidProposal = errors.New("protocol: nil proposal")

func ValidateProposal(p *defs.GPropose) error {
	if p == nil || p.Propose == nil {
		return ErrInvalidProposal
	}
	return nil
}

func UnsupportedEvent(name string, event any) error {
	// Inspect only the type: passing event itself to fmt would make every valid
	// value message escape too, adding an allocation to the hot dispatch path.
	return fmt.Errorf("%s: unsupported event %v", name, reflect.TypeOf(event))
}

func UnsupportedTimer(name string, timer Timer) error {
	return fmt.Errorf("%s: unsupported timer %d", name, timer)
}

// Must is for trusted runtime dispatch. An unsupported event indicates a wiring
// bug and must not be silently dropped. Expected protocol rejection (for example
// an unsupported command) stays in the protocol's existing reply/drop path.
func Must(err error) {
	if err != nil {
		panic(err)
	}
}

// Drain delivers queued outputs in order, including outputs produced by local
// reentrant transitions. The owner serializes access; false stops delivery.
func Drain[T any](queue *[]T, deliver func(T) bool) {
	for len(*queue) > 0 {
		out := *queue
		*queue = nil
		for _, e := range out {
			if !deliver(e) {
				return
			}
		}
	}
}
