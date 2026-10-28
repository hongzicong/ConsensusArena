package agreement

// Execution ordering, result deduplication, and protocol completion.
import (
	"github.com/hongzicong/ConsensusArena/state"
)

// The live state machine is the in-memory snapshot in this crash-stop port.
// Like Summerset's all-peer snapshot watermark, GC waits for EVERY fixed member
// to execute the prefix. Failure suspicion never permits deletion. Durable
// checkpoints and installing them into restarted members are outside this model.
func (e *engine) compactLog() {
	if !e.active() {
		return
	}
	through := e.prefix
	for p, prefix := range e.progress {
		if p != e.id {
			through = minSlot(through, prefix)
		}
	}
	if through <= e.compacted || through-e.compacted < 1024 {
		return
	}
	for slot := e.compacted + 1; slot <= through; slot++ {
		if v, ok := e.log[slot]; ok {
			for _, r := range v.requests() {
				if e.inflight[r.id()] == slot {
					delete(e.inflight, r.id())
				}
			}
		}
		delete(e.log, slot)
		delete(e.committed, slot)
		delete(e.votes, slot)
		delete(e.phaseTimes, slot)
		delete(e.noticeSlots, slot)
	}
	e.stats.CompactedSlots += through - e.compacted
	e.compacted = through
	// Key indexes may point into compacted history: readValue uses state there.
	// Deduplication results must remain, or a delayed retry could execute twice.
}

func (e *engine) apply() {
	for e.committed[e.prefix+1] {
		if e.beforeApply != nil && !e.beforeApply(e.log[e.prefix+1]) {
			return
		}
		e.prefix++
		v := e.log[e.prefix]
		for _, r := range v.requests() {
			if e.applySpecial != nil && e.applySpecial(r, e.prefix) {
				continue
			}
			if r.Proposal.Command.Op == state.NONE {
				continue
			}
			value, ok := e.completed[r.id()]
			if !ok {
				value = e.execute(r.Proposal.Command)
				e.completed[r.id()] = value
			}
			if e.id == e.current.Leader || r.Origin == e.id {
				e.finish(r, value)
			}
		}
	}
}

func (e *engine) finish(r request, value state.Value) {
	if e.finishSpecial != nil {
		e.finishSpecial(r)
	}
	delete(e.queued, r.id())
	e.dropHeld(r.id())
	if r.Origin == e.id {
		e.reply(r, value)
	} else if r.Origin >= 0 && r.Origin < e.n {
		e.emit(r.Origin, message{Kind: result, Request: r, Value: value})
	}
}
