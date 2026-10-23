package paxos

// Protocol messages, identifiers, constants, and value helpers.

const PageSize = 128

const AdmissionWindow = 8192

const (
	packetPrepare uint8 = iota
	packetPromise
	packetAccept
	packetAck
	packetCommit
	packetReady
	packetFetch
	packetForward
)
