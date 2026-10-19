package xpaxos

import (
	"math/bits"
	"time"
)

type phaseTiming struct{ Start, Majority time.Time }

func histogramBucket(d time.Duration) int {
	ms := uint64((d + time.Millisecond - 1) / time.Millisecond)
	b := bits.Len64(ms)
	if b > 31 {
		b = 31
	}
	return b
}

func (e *engine) observeMajority(slot uint64, now time.Time) {
	if e.id != e.current.Leader {
		return
	}
	t, ok := e.phaseTimes[slot]
	if !ok || !t.Majority.IsZero() {
		return
	}
	t.Majority = now
	e.phaseTimes[slot] = t
	d := now.Sub(t.Start)
	e.stats.MajoritySamples++
	e.stats.MajorityNanos += uint64(d)
	e.stats.MajorityHistogram[histogramBucket(d)]++
}

func (e *engine) observeCoverage(slot uint64, now time.Time) {
	t, ok := e.phaseTimes[slot]
	if e.id != e.current.Leader {
		if ok {
			e.stats.LearnerWaitSamples++
			e.stats.LearnerWaitNanos += uint64(now.Sub(t.Start))
			delete(e.phaseTimes, slot)
		}
		return
	}
	if !ok || t.Majority.IsZero() {
		return
	}
	d := now.Sub(t.Majority)
	e.stats.CoverageSamples++
	e.stats.CoverageNanos += uint64(d)
	e.stats.CoverageHistogram[histogramBucket(d)]++
	delete(e.phaseTimes, slot)
}
