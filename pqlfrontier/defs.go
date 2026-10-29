package pqlfrontier

import "github.com/hongzicong/ConsensusArena/state"

// Protocol messages, identifiers, constants, and value helpers.

const PageSize = 128

const AdmissionWindow = 8192

// Client identifiers use the full signed int32 range. Internal recovery no-ops
// are distinguished by their invalid ingress, not by reserving a client ID.
func recoveryNoop(r Request) bool {
	return r.Origin == -1 && r.Command.Op == state.NONE && r.Change == nil
}

const (
	packetPrepare uint8 = iota
	packetPromise
	packetAccept
	packetAck
	packetCommit
	packetReady
	packetFetch
	packetForward
	packetForwardReply
	packetGuard
	packetGuardAck
	packetLeasePromise
	packetLeaseReply
	packetSuspect
)
