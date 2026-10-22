package curp

// Execution ordering, result deduplication, and protocol completion.
import (
	"github.com/hongzicong/ConsensusArena/state"
)

func (c *core) apply() {
	for {
		r := c.Log[c.Executed+1]
		if r == nil || !r.Committed {
			return
		}
		if _, ok := c.Values[r.Request.ID]; !ok && r.Request.Command.Op != state.NONE {
			v := c.execute(r.Request.Command)
			c.Values[r.Request.ID] = append(state.Value(nil), v...)
			c.reply(r.Request, v, false)
		}
		delete(c.Pending, r.Request.ID)
		if w, ok := c.Witness[r.Request.ID]; ok {
			c.witnessIndex.remove(w)
		}
		delete(c.Witness, r.Request.ID)
		c.pendingIndex.remove(r.Request)
		delete(c.recorded, r.Request.ID)
		delete(c.votes, r.Slot)
		c.Executed++
	}
}
