package pql

import (
	"sort"
	"time"
)

const (
	genesisLeaseEpoch  int64 = 1
	guardDuration            = 2 * time.Second
	leaseDuration            = 2 * time.Second
	leaseRenewInterval       = 500 * time.Millisecond
)

// Timers implement PQL §3.5/Figure 5. They use local monotonic durations;
// Timestamp is an echoed sequence, never a remote clock reading. A grantor's
// obligation encloses the holder's usable interval, including delayed traffic.
type leaseState struct {
	n, id                                                        int
	promisedBy, promisedTo                                       int64
	renewAt                                                      time.Time
	latestPromises, latestReplies                                []time.Time
	guardExpires, lastGuard, lastResponse                        []time.Time
	lastSent, lastAckTime                                        []time.Time
	guardSequence, sentSequence, receivedSequence, ackSequence   []int64
	receivedGuardSequence                                        []int64
	ackTimes                                                     []map[int64]time.Time
	frontiers                                                    []int64
	readUntil, writeUntil                                        time.Time
	catchup                                                      int64
	activationComplete                                           bool
	started                                                      bool
	establishments, renewals, promisesAccepted, promisesRejected uint64
}

func newLeaseState(n int, id int32) *leaseState {
	l := &leaseState{n: n, id: int(id), promisedBy: genesisLeaseEpoch, promisedTo: -1, catchup: -1,
		latestPromises: make([]time.Time, n), latestReplies: make([]time.Time, n),
		guardExpires: make([]time.Time, n), lastGuard: make([]time.Time, n), lastResponse: make([]time.Time, n),
		lastSent: make([]time.Time, n), lastAckTime: make([]time.Time, n),
		guardSequence: make([]int64, n), sentSequence: make([]int64, n), receivedSequence: make([]int64, n), ackSequence: make([]int64, n),
		receivedGuardSequence: make([]int64, n),
		ackTimes:              make([]map[int64]time.Time, n), frontiers: make([]int64, n)}
	for peer := 0; peer < n; peer++ {
		l.frontiers[peer] = -1
		l.ackTimes[peer] = make(map[int64]time.Time)
	}
	return l
}

func (l *leaseState) switchEpoch(epoch int64, now time.Time) {
	l.promisedBy = epoch
	l.started = false
	l.renewAt = now
	for peer := range l.latestReplies {
		l.latestReplies[peer] = time.Time{}
		l.lastGuard[peer] = time.Time{}
		l.ackSequence[peer] = 0
	}
	l.writeUntil = now
}

func (l *leaseState) tick(now time.Time, alive []bool, frontier int64, send func(int32, *Packet)) {
	if l.started && now.Before(l.renewAt) {
		return
	}
	for peer := 0; peer < l.n; peer++ {
		if peer == l.id || !alive[peer] {
			continue
		}
		if l.ackSequence[peer] > 0 && now.Before(l.latestReplies[peer]) {
			l.sendPromise(now, peer, frontier, false, send)
			l.renewals++
		} else {
			l.guardSequence[peer]++
			l.lastGuard[peer] = now
			l.lastSent[peer] = now
			send(int32(peer), &Packet{Kind: packetGuard, From: int32(l.id), LeaseEpoch: l.promisedBy,
				Timestamp: l.guardSequence[peer], Duration: int64(guardDuration)})
			l.establishments++
		}
	}
	l.started = true
	l.renewAt = now.Add(leaseRenewInterval)
	l.recomputeWriteUntil(now)
}

func (l *leaseState) sendPromise(now time.Time, peer int, frontier int64, guarded bool, send func(int32, *Packet)) {
	l.lastSent[peer] = now
	l.sentSequence[peer]++
	p := &Packet{Kind: packetLeasePromise, From: int32(l.id), LeaseEpoch: l.promisedBy,
		Timestamp: l.sentSequence[peer], Duration: int64(leaseDuration), Frontier: frontier}
	if !guarded {
		p.AckSequence = l.ackSequence[peer]
		p.Elapsed = int64(now.Sub(l.lastAckTime[peer]))
	} else {
		// A negative reference identifies the guard ACK; positive references
		// identify promise ACKs. A later guard cannot validate an old promise.
		p.AckSequence = -l.guardSequence[peer]
	}
	l.latestReplies[peer] = now.Add(guardDuration + leaseDuration)
	l.recomputeWriteUntil(now)
	send(int32(peer), p)
}

func (l *leaseState) receive(p *Packet, now time.Time, frontier int64, send func(int32, *Packet)) bool {
	peer := int(p.From)
	if peer < 0 || peer >= l.n || peer == l.id {
		return false
	}
	switch p.Kind {
	case packetGuard:
		if p.Duration <= 0 || time.Duration(p.Duration) > 30*time.Second {
			return true
		}
		if p.Timestamp <= l.receivedGuardSequence[peer] {
			return true
		}
		l.receivedGuardSequence[peer] = p.Timestamp
		l.guardExpires[peer] = now.Add(time.Duration(p.Duration))
		send(p.From, &Packet{Kind: packetGuardAck, From: int32(l.id), LeaseEpoch: p.LeaseEpoch, Timestamp: p.Timestamp})
		return true
	case packetGuardAck:
		if p.LeaseEpoch != l.promisedBy || p.Timestamp != l.guardSequence[peer] || l.lastGuard[peer].IsZero() {
			return true
		}
		l.lastGuard[peer] = time.Time{}
		l.lastResponse[peer] = now
		l.sendPromise(now, peer, frontier, true, send)
		return true
	case packetLeasePromise:
		if p.Timestamp <= l.receivedSequence[peer] {
			return true
		}
		admissible := p.AckSequence < 0 && p.AckSequence == -l.receivedGuardSequence[peer] && now.Before(l.guardExpires[peer])
		if p.AckSequence > 0 {
			ack, ok := l.ackTimes[peer][p.AckSequence]
			admissible = ok && p.Elapsed >= 0 && now.Sub(ack) < time.Duration(p.Elapsed)+guardDuration
		}
		if !admissible || p.Duration <= 0 || time.Duration(p.Duration) > 30*time.Second {
			l.promisesRejected++
			return true
		}
		if p.LeaseEpoch < l.promisedTo {
			send(p.From, &Packet{Kind: packetLeaseReply, From: int32(l.id), LeaseEpoch: l.promisedTo, Timestamp: p.Timestamp})
			l.promisesRejected++
			return true
		}
		if p.LeaseEpoch > l.promisedTo {
			l.promisedTo = p.LeaseEpoch
			l.activationComplete = false
			l.catchup = -1
			for i := range l.latestPromises {
				l.latestPromises[i] = time.Time{}
				l.frontiers[i] = -1
			}
		}
		l.receivedSequence[peer] = p.Timestamp
		l.latestPromises[peer] = now.Add(time.Duration(p.Duration))
		l.frontiers[peer] = p.Frontier
		l.ackTimes[peer][p.Timestamp] = now
		for sequence, stamp := range l.ackTimes[peer] {
			if now.Sub(stamp) > failureGrace+guardDuration+leaseDuration {
				delete(l.ackTimes[peer], sequence)
			}
		}
		l.recomputeReadUntil(now)
		send(p.From, &Packet{Kind: packetLeaseReply, From: int32(l.id), LeaseEpoch: l.promisedTo, Timestamp: p.Timestamp})
		l.promisesAccepted++
		return true
	case packetLeaseReply:
		if p.LeaseEpoch != l.promisedBy || p.Timestamp != l.sentSequence[peer] || !now.Before(l.latestReplies[peer]) {
			return true
		}
		l.ackSequence[peer] = p.Timestamp
		l.lastResponse[peer] = now
		l.lastAckTime[peer] = now
		// Figure 5: acknowledgement can shorten an outstanding grant, but
		// cannot extend the original guard-plus-lease deadline.
		if expiry := now.Add(leaseDuration); expiry.Before(l.latestReplies[peer]) {
			l.latestReplies[peer] = expiry
		}
		l.recomputeWriteUntil(now)
		return true
	default:
		return false
	}
}

func (l *leaseState) recomputeReadUntil(now time.Time) {
	ids := make([]int, 0, l.n-1)
	for peer, expiry := range l.latestPromises {
		if peer != l.id && now.Before(expiry) {
			ids = append(ids, peer)
		}
	}
	if len(ids) < l.n/2 {
		l.readUntil = time.Time{}
		return
	}
	sort.Slice(ids, func(i, j int) bool {
		if l.latestPromises[ids[i]].Equal(l.latestPromises[ids[j]]) {
			return ids[i] < ids[j]
		}
		return l.latestPromises[ids[i]].After(l.latestPromises[ids[j]])
	})
	l.readUntil = l.latestPromises[ids[l.n/2-1]]
	// Only activation needs a prefix barrier (§3.6). Once active, every
	// interfering write must notify this holder; renewing the lease must not
	// repeatedly stall a read behind unrelated keys' newer Paxos instances.
	if !l.activationComplete {
		l.catchup = -1
		for _, peer := range ids[:l.n/2] {
			if l.frontiers[peer] > l.catchup {
				l.catchup = l.frontiers[peer]
			}
		}
		l.activationComplete = true
	}
}

func (l *leaseState) recomputeWriteUntil(now time.Time) {
	l.writeUntil = now
	for peer, expiry := range l.latestReplies {
		if peer != l.id && expiry.After(l.writeUntil) {
			l.writeUntil = expiry
		}
	}
}

func (l *leaseState) canRead(now time.Time, executed int64) bool {
	return l.promisedTo >= 0 && now.Before(l.readUntil) && executed >= l.catchup
}

func (l *leaseState) canWriteOutside(now time.Time) bool { return !now.Before(l.writeUntil) }
