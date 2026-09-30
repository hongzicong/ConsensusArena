package bodega

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
		delete(e.noteVotes, slot)
		delete(e.noticeSlots, slot)
	}
	e.stats.CompactedSlots += through - e.compacted
	e.compacted = through
	// Key indexes may point into compacted history: readValue uses state there.
	// Deduplication results must remain, or a delayed retry could execute twice.
}

func (e *engine) learnCommittedEntry(v entry) {
	if v.Slot <= e.prefix {
		return
	}
	// The installed leader supplies a chosen value from its executed history.
	// Give it current-ballot provenance so compact commit notices can repair gaps.
	v.Ballot, v.ReadFresh = e.current.Ballot, false
	e.acceptEntry(v)
	e.markCommitted(v.Slot)
	e.apply()
}
