package pql

import (
	"testing"
	"time"
)

func TestLateAndDuplicatePromisesCannotExtendReadLease(t *testing.T) {
	now := time.Unix(60, 0)
	grantor, holder := newLeaseState(5, 0), newLeaseState(5, 1)
	var guard, ack, promise, reply *Packet
	grantor.tick(now, []bool{true, true, false, false, false}, -1, func(_ int32, p *Packet) { guard = p })
	holder.receive(guard, now.Add(10*time.Millisecond), -1, func(_ int32, p *Packet) { ack = p })
	grantor.receive(ack, now.Add(20*time.Millisecond), -1, func(_ int32, p *Packet) { promise = p })
	holder.receive(promise, now.Add(30*time.Millisecond), -1, func(_ int32, p *Packet) { reply = p })
	grantor.receive(reply, now.Add(40*time.Millisecond), -1, func(int32, *Packet) {})
	expiry := holder.latestPromises[0]
	holder.receive(promise, now.Add(time.Second), -1, func(int32, *Packet) {})
	if !holder.latestPromises[0].Equal(expiry) {
		t.Fatal("duplicate promise extended holder timer")
	}
	grantor.tick(now.Add(500*time.Millisecond), []bool{true, true, false, false, false}, -1, func(_ int32, p *Packet) { promise = p })
	if promise.AckSequence == 0 {
		t.Fatal("renewal did not reference an acknowledged promise")
	}
	holder.receive(promise, now.Add(3*time.Second), -1, func(int32, *Packet) {})
	if !holder.latestPromises[0].Equal(expiry) || holder.promisesRejected == 0 {
		t.Fatal("late renewal was accepted beyond its acknowledgement-relative guard")
	}
	if grantor.writeUntil.After(now.Add(4500 * time.Millisecond)) {
		t.Fatal("unacknowledged renewal created an unbounded grant timer")
	}
}

func TestRenewalDoesNotRaiseActivationBarrier(t *testing.T) {
	now := time.Unix(70, 0)
	l := newLeaseState(5, 2)
	l.promisedTo = genesisLeaseEpoch
	l.latestPromises[0], l.latestPromises[1] = now.Add(time.Second), now.Add(time.Second)
	l.frontiers[0], l.frontiers[1] = 4, 5
	l.recomputeReadUntil(now)
	if !l.canRead(now, 5) {
		t.Fatal("initial majority activation failed")
	}
	l.frontiers[0], l.frontiers[1] = 100, 200
	l.recomputeReadUntil(now.Add(time.Millisecond))
	if !l.canRead(now.Add(time.Millisecond), 5) {
		t.Fatal("renewal stalled a read behind unrelated newer instances")
	}
}

func TestReplayedGuardCannotValidateDelayedPromise(t *testing.T) {
	now := time.Unix(80, 0)
	grantor, holder := newLeaseState(5, 0), newLeaseState(5, 1)
	var guard, ack, promise *Packet
	grantor.tick(now, []bool{true, true, false, false, false}, -1, func(_ int32, p *Packet) { guard = p })
	holder.receive(guard, now.Add(10*time.Millisecond), -1, func(_ int32, p *Packet) { ack = p })
	grantor.receive(ack, now.Add(20*time.Millisecond), -1, func(_ int32, p *Packet) { promise = p })
	// The original promise is delayed beyond its guard. Replaying that guard
	// must not restart the receiver's timer and make the old promise usable.
	holder.receive(guard, now.Add(3*time.Second), -1, func(int32, *Packet) {})
	holder.receive(promise, now.Add(3100*time.Millisecond), -1, func(int32, *Packet) {})
	if !holder.latestPromises[0].IsZero() {
		t.Fatal("replayed guard revalidated an expired promise")
	}
	// Even a fresh guard must not validate a promise referring to an older one.
	next := *guard
	next.Timestamp++
	holder.receive(&next, now.Add(3200*time.Millisecond), -1, func(int32, *Packet) {})
	holder.receive(promise, now.Add(3300*time.Millisecond), -1, func(int32, *Packet) {})
	if !holder.latestPromises[0].IsZero() {
		t.Fatal("new guard revalidated a promise from an old guard exchange")
	}
}
