package fastpaxos

// Execution ordering, result deduplication, and protocol completion.
import (
	"github.com/hongzicong/ConsensusArena/state"
)

func (c *core) apply() {
	for {
		s := c.slots[c.executed+1]
		if s == nil || s.Chosen == nil {
			return
		}
		v := s.Chosen.Value
		if !v.Noop {
			full, ok := c.known[v.ID]
			if !ok {
				return
			}
			result, done := c.results[v.ID]
			if !done {
				if c.execute != nil {
					result = c.execute(full)
				}
				result = append(state.Value(nil), result...)
				c.results[v.ID] = result
				if c.complete != nil {
					c.complete(v.ID, result)
				}
			}
			c.forgetPending(v.ID)
		}
		c.executed++
		c.lastProgress = c.now
	}
}
