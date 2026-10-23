package paxos

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
		if _, ok := c.values[r.Request.ID]; !ok && r.Request.Command.Op != state.NONE {
			v := c.execute(r.Request.Command)
			c.values[r.Request.ID] = append(state.Value(nil), v...)
			c.reply(r.Request, v)
		}
		delete(c.pending, r.Request.ID)
		delete(c.votes, r.Slot)
		c.executed++
	}
}
