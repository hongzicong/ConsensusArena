package bodega

import "time"

func (e *engine) startPrepare(now time.Time) {
	e.takeSnapshot(e.prepareStart)
	e.addSnapshot(e.id, e.prepareStart, 0, 1, e.ownSnapshot, now)
	e.broadcast(message{Kind: prepare, PrepareStart: e.prepareStart})
}

// Freeze accepted evidence once per ballot and requested suffix, like upstream's
// trigger_slot. Executed history is not part of a new leader's Prepare.
func (e *engine) takeSnapshot(start uint64) bool {
	if start == 0 || start <= e.compacted {
		return false
	}
	if e.snapshotTaken {
		return start == e.snapshotStart
	}
	e.snapshotTaken, e.snapshotStart = true, start
	e.ownSnapshot = make([]entry, 0)
	for s := start; s <= e.high; s++ {
		if v, ok := e.log[s]; ok {
			e.ownSnapshot = append(e.ownSnapshot, v)
		}
	}
	first, size := 0, 0
	for i, v := range e.ownSnapshot {
		if i > first && (i-first >= 64 || size+entryBytes(v) > maxBatchBytes) {
			e.snapshotChunks = append(e.snapshotChunks, e.ownSnapshot[first:i])
			first, size = i, 0
		}
		size += entryBytes(v)
	}
	e.snapshotChunks = append(e.snapshotChunks, e.ownSnapshot[first:])
	return true
}

func (e *engine) sendSnapshot(to int, start uint64) {
	if !e.takeSnapshot(start) {
		return
	}
	if e.snapshotCursor == nil {
		e.snapshotCursor = map[int]int{}
	}
	if e.snapshotCursor[to] == len(e.snapshotChunks) {
		e.snapshotCursor[to] = 0 // retry a lost reply, not a transfer pacing credit
	}
	e.flushPromises(to)
}

// The writer wakes this pump as queue space becomes available. Heartbeats only
// retry lost requests/replies; they impose no slots-per-second transfer budget.
func (e *engine) flushPromises(to int) {
	if !e.active() || to != e.current.Leader || !e.snapshotTaken {
		return
	}
	for p := e.snapshotCursor[to]; p < len(e.snapshotChunks); p++ {
		m := message{Kind: promise, PrepareStart: e.snapshotStart,
			Part: p, Parts: len(e.snapshotChunks), Entries: e.snapshotChunks[p]}
		if !e.emit(to, m) {
			return
		}
		e.snapshotCursor[to] = p + 1
	}
}

func (e *engine) addSnapshot(from int, start uint64, part, parts int, entries []entry, now time.Time) {
	if e.prepared || start != e.prepareStart || parts <= 0 || part < 0 || part >= parts {
		return
	}
	for _, v := range entries {
		if v.Slot < start || v.Ballot > e.current.Ballot {
			return
		}
	}
	s := e.snapshots[from]
	if s == nil {
		s = &snapshot{parts, map[int][]entry{}}
		e.snapshots[from] = s
	}
	if s.Parts != parts {
		return
	}
	s.Chunks[part] = entries
	count := 0
	for _, s := range e.snapshots {
		if len(s.Chunks) == s.Parts {
			count++
		}
	}
	if count < e.majority {
		return
	}
	selected := map[uint64]entry{}
	high := start - 1
	for _, s := range e.snapshots {
		if len(s.Chunks) != s.Parts {
			continue
		}
		for _, vs := range s.Chunks {
			for _, v := range vs {
				if old, ok := selected[v.Slot]; !ok || v.Ballot > old.Ballot {
					selected[v.Slot] = v
				}
				high = maxSlot(high, v.Slot)
			}
		}
	}
	e.next = high
	e.inflight = map[requestID]uint64{}
	e.prepared = true
	for slot := start; slot <= high; slot++ {
		v, ok := selected[slot]
		if !ok {
			v = entry{Slot: slot, Request: request{Origin: -1}}
		}
		v.Ballot, v.ReadFresh = e.current.Ballot, false
		e.acceptEntry(v)
		for _, r := range v.requests() {
			e.inflight[r.id()] = slot
		}
		e.votes[slot] = bit(e.id)
		// Already committed entries are fixed; others must cover the new roster.
		e.stats.RecoveredSlots++
	}
	e.stats.PrepareEntries += high - start + 1
	e.stats.PrepareCompleted++
	e.stats.PrepareNanos += uint64(now.Sub(e.prepareSince))
	e.reindex()
	for slot := start; slot <= high; slot++ {
		e.broadcast(message{Kind: accept, Entry: e.log[slot]})
		if e.committed[slot] {
			e.broadcast(message{Kind: commit, CommitSlot: slot, CommitPrefix: e.prefix})
		}
	}
	for p := 0; p < e.n; p++ {
		if p != e.id {
			e.repairPeer(p)
		}
	}
	// Values remain in the log; release the quorum's duplicate payloads.
	e.snapshots = map[int]*snapshot{}
}
