package bodega

import "time"

// Notice evidence never supplies a value: only an Accept in the same ballot
// can satisfy it. In particular, recovery cannot commit a stale local tail.
func (e *engine) applyNoticedSlot(slot uint64) {
	if slot == 0 || (slot > e.noticePrefix && !e.noticeSlots[slot]) {
		return
	}
	v, ok := e.log[slot]
	if !ok || v.Ballot != e.current.Ballot {
		return
	}
	if !e.committed[slot] {
		e.markCommitted(slot)
	}
	delete(e.noticeSlots, slot)
}

func (e *engine) learnCommitNotice(prefix, slot uint64, now time.Time) {
	e.noticePrefix = maxSlot(e.noticePrefix, prefix)
	e.noticeHigh = maxSlot(e.noticeHigh, maxSlot(prefix, slot))
	if slot > e.prefix {
		e.noticeSlots[slot] = true
		e.applyNoticedSlot(slot)
	}
	// acceptedPrefix certifies that every entry visited exists in this ballot.
	limit := minSlot(e.noticePrefix, e.acceptedPrefix)
	for s := e.prefix + 1; s <= limit; s++ {
		e.applyNoticedSlot(s)
	}
	e.apply()
	e.requestCommitRepair(now)
}

func (e *engine) requestCommitRepair(now time.Time) {
	if !e.active() || e.id == e.current.Leader || e.acceptedPrefix >= e.noticeHigh {
		return
	}
	if !e.lastRepair.IsZero() && now.Sub(e.lastRepair) < e.opt.Heartbeat {
		return
	}
	e.lastRepair = now
	e.stats.CommitRepairRequests++
	e.emit(e.current.Leader, message{Kind: repairRequest, RepairStart: e.acceptedPrefix + 1})
}

func (e *engine) sendCommitRepair(peer int, start uint64) {
	if start == 0 || start > e.next {
		return
	}
	end := e.next
	if end-start >= 64 {
		end = start + 63
	}
	for slot := start; slot <= end; slot++ {
		v, ok := e.log[slot]
		if !ok || v.Ballot != e.current.Ballot {
			continue
		}
		e.stats.CommitRepairEntries++
		e.emit(peer, message{Kind: accept, Entry: v})
		if e.committed[slot] && slot > e.prefix {
			e.emit(peer, message{Kind: commit, CommitSlot: slot, CommitPrefix: e.prefix})
		}
	}
	e.emit(peer, message{Kind: commit, CommitPrefix: e.prefix})
}

func maxSlot(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}
func minSlot(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}
