package bodega

import (
	"time"
)

func (e *engine) revokeLeases(now time.Time) {
	if e.pending.Ballot == 0 {
		return
	}
	for peer, until := range e.outgoing {
		if now.Before(until) {
			e.stats.Revokes++
			e.emit(peer, message{Kind: leaseRevoke, Sequence: e.current.Ballot})
		}
	}
}

func (e *engine) receiveRevocation(m message, now time.Time) bool {
	switch m.Kind {
	case leaseRevoke:
		if m.Sequence > e.revoked[m.From] {
			e.revoked[m.From] = m.Sequence
		}
		if e.current.Ballot <= m.Sequence {
			delete(e.incoming, m.From)
		}
		// The floor also rejects delayed renewal replies for this ballot.
		e.emit(m.From, message{Kind: leaseRevoked, Sequence: m.Sequence})
		return true
	case leaseRevoked:
		if e.pending.Ballot > e.current.Ballot && m.Sequence == e.current.Ballot {
			if _, ok := e.outgoing[m.From]; ok {
				delete(e.outgoing, m.From)
				e.stats.RevokeAcks++
			}
			e.install(now)
		}
		return true
	}
	return false
}
