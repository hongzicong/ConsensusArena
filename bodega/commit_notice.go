package bodega

import "time"

const repairWindow = 512

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
	// The executed prefix is already fixed. Check the unexecuted suffix
	// directly, without waiting to re-accept all earlier slots in this ballot.
	for s := e.prefix + 1; s <= e.noticePrefix; s++ {
		v, ok := e.log[s]
		if !ok || v.Ballot != e.current.Ballot {
			break
		}
		e.applyNoticedSlot(s)
	}
	e.apply()
	e.requestCommitRepair(now)
}

func (e *engine) requestCommitRepair(now time.Time) {
	start := maxSlot(e.prefix, e.acceptedPrefix) + 1
	if !e.active() || e.id == e.current.Leader || start > e.noticeHigh {
		return
	}
	if !e.lastRepair.IsZero() && now.Sub(e.lastRepair) < e.opt.Heartbeat {
		return
	}
	e.lastRepair = now
	e.stats.CommitRepairRequests++
	e.emit(e.current.Leader, message{Kind: repairRequest, RepairStart: start})
}

func (e *engine) repairPeer(peer int) {
	start := minSlot(e.progress[peer], e.prefix) + 1
	end := e.sendCommitRepair(peer, start)
	// A lagging follower must not prevent solicitation of its vote for the
	// leader's first unexecuted slot, even when that slot is far ahead.
	if end <= e.prefix {
		e.sendCommitRepair(peer, e.prefix+1)
	}
	// Preserve the stronger current-ballot accepted-prefix evidence used by
	// out-of-order reads, but repair it separately from execution progress.
	if e.acceptedProgress[peer]+1 < start {
		e.sendRepairWindow(peer, e.acceptedProgress[peer]+1, 64)
	}
}

func (e *engine) sendCommitRepair(peer int, start uint64) uint64 {
	return e.sendRepairWindow(peer, start, repairWindow)
}

func (e *engine) sendRepairWindow(peer int, start, window uint64) uint64 {
	if start == 0 || start > e.next {
		return start
	}
	end := e.next
	if end-start >= window {
		end = start + window - 1
	}
	slot, size := start, 0
	for ; slot <= end; slot++ {
		v, ok := e.log[slot]
		if !ok {
			continue
		}
		// History before the Prepare suffix is already chosen, and must not be
		// re-voted merely to catch up a lagging follower in a newer ballot.
		if slot <= e.prefix && v.Ballot != e.current.Ballot {
			bytes := entryBytes(v)
			if size > 0 && size+bytes > maxBatchBytes {
				break
			}
			size += bytes
			e.stats.CommitRepairEntries++
			e.emit(peer, message{Kind: committedEntry, Entry: v})
			continue
		}
		if v.Ballot != e.current.Ballot {
			continue
		}
		// An acknowledged value survives for this ballot in the crash-stop
		// model. Do not flood the data queue with copies of it on every tick.
		if e.votes[slot]&bit(peer) == 0 {
			bytes := entryBytes(v)
			if size > 0 && size+bytes > maxBatchBytes {
				break
			}
			size += bytes
			e.stats.CommitRepairEntries++
			e.emit(peer, message{Kind: accept, Entry: v})
		}
		if e.committed[slot] && slot > e.prefix {
			e.emit(peer, message{Kind: commit, CommitSlot: slot, CommitPrefix: e.prefix})
		}
	}
	e.emit(peer, message{Kind: commit, CommitPrefix: e.prefix})
	return slot
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
