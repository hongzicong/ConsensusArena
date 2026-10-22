package bodega

import (
	"time"
)

func (c *core) revokeLeases(now time.Time) {
	if c.pending.Ballot == 0 {
		return
	}
	for peer, until := range c.outgoing {
		if now.Before(until) {
			c.stats.Revokes++
			c.emit(peer, message{Kind: leaseRevoke, Sequence: c.current.Ballot})
		}
	}
}

func (c *core) receiveRevocation(m message, now time.Time) bool {
	switch m.Kind {
	case leaseRevoke:
		if m.Sequence > c.revoked[m.From] {
			c.revoked[m.From] = m.Sequence
		}
		if c.current.Ballot <= m.Sequence {
			delete(c.incoming, m.From)
		}
		// The floor also rejects delayed renewal replies for this ballot.
		c.emit(m.From, message{Kind: leaseRevoked, Sequence: m.Sequence})
		return true
	case leaseRevoked:
		if c.pending.Ballot > c.current.Ballot && m.Sequence == c.current.Ballot {
			if _, ok := c.outgoing[m.From]; ok {
				delete(c.outgoing, m.From)
				c.stats.RevokeAcks++
			}
			c.install(now)
		}
		return true
	}
	return false
}
