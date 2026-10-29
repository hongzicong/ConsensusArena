package pqlfrontier

// Execution ordering, result deduplication, and protocol completion.
import (
	"github.com/hongzicong/ConsensusArena/state"
)

func (c *core) apply() {
	for {
		r := c.log[c.executed+1]
		if r == nil || !r.Committed {
			return
		}
		internal := recoveryNoop(r.Request)
		if c.control {
			c.onChange(r.Slot, r.Request.Change)
			if !internal {
				c.values[r.Request.ID] = nil
			}
		}
		if _, ok := c.values[r.Request.ID]; !ok && !internal {
			var v state.Value
			if r.Request.Command.Op != state.NONE {
				v = c.execute(r.Request.Command)
			}
			c.values[r.Request.ID] = append(state.Value(nil), v...)
			// Commit is broadcast, so the ingress replica completes an ordered
			// request locally. The leader must not also forward a duplicate.
			if r.Request.Origin == c.id {
				c.reply(r.Request, v)
			}
		}
		if r.Request.Command.Op == state.PUT {
			c.removeUpdating(r.Request.Command.K)
		}
		if !internal {
			delete(c.pending, r.Request.ID)
		}
		delete(c.votes, r.Slot)
		c.executed++
	}
}
