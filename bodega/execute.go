package bodega

// Execution ordering, result deduplication, and protocol completion.
import (
	"github.com/hongzicong/ConsensusArena/state"
)

// The live state machine is the in-memory snapshot in this crash-stop port.
// Like Summerset's all-peer snapshot watermark, GC waits for EVERY fixed member
// to execute the prefix. Failure suspicion never permits deletion. Durable
// checkpoints and installing them into restarted members are outside this model.
func (c *core) compactLog() {
	if !c.active() {
		return
	}
	through := c.prefix
	for p, prefix := range c.progress {
		if p != c.id {
			through = minSlot(through, prefix)
		}
	}
	if through <= c.compacted || through-c.compacted < 1024 {
		return
	}
	for slot := c.compacted + 1; slot <= through; slot++ {
		if v, ok := c.log[slot]; ok {
			for _, r := range v.requests() {
				if c.inflight[r.id()] == slot {
					delete(c.inflight, r.id())
				}
			}
		}
		delete(c.log, slot)
		delete(c.committed, slot)
		delete(c.votes, slot)
		delete(c.noticeSlots, slot)
	}
	c.stats.CompactedSlots += through - c.compacted
	c.compacted = through
	// Key indexes may point into compacted history: readValue uses state there.
	// Deduplication results must remain, or a delayed retry could execute twice.
}

func (c *core) apply() {
	for c.committed[c.prefix+1] {
		c.prefix++
		v := c.log[c.prefix]
		for _, r := range v.requests() {
			if r.Proposal.Command.Op == state.NONE {
				continue
			}
			value, ok := c.completed[r.id()]
			if !ok {
				value = c.execute(r.Proposal.Command)
				c.completed[r.id()] = value
			}
			if c.id == c.current.Leader {
				c.finish(r, value)
			}
		}
	}
}

func (c *core) finish(r request, value state.Value) {
	delete(c.queued, r.id())
	c.dropHeld(r.id())
	if r.Origin == c.id {
		if !c.reply(r, value) {
			c.enqueue(r)
		}
	} else if r.Origin >= 0 && r.Origin < c.n {
		c.emit(r.Origin, message{Kind: result, Request: r, Value: value})
	}
}
