package defs

import (
	"container/heap"
	"sync"
	"sync/atomic"
	"time"
)

const delayQueueInputBuffer = 256

type delayedValue[T any] struct {
	value T
	due   time.Time
	seq   uint64
}

type delayedHeap[T any] []delayedValue[T]

func (h delayedHeap[T]) Len() int {
	return len(h)
}

func (h delayedHeap[T]) Less(i, j int) bool {
	if h[i].due.Equal(h[j].due) {
		return h[i].seq < h[j].seq
	}
	return h[i].due.Before(h[j].due)
}

func (h delayedHeap[T]) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *delayedHeap[T]) Push(value any) {
	*h = append(*h, value.(delayedValue[T]))
}

func (h *delayedHeap[T]) Pop() any {
	old := *h
	last := len(old) - 1
	value := old[last]
	var zero delayedValue[T]
	old[last] = zero
	*h = old[:last]
	return value
}

// DelayQueue schedules deliveries by absolute deadline. Each queue owns one
// goroutine and one reusable timer. The timer remains armed for the earliest
// deadline and is reset only when that deadline changes.
//
// A queue represents one ordered network link. Sequence numbers break
// equal-deadline ties and preserve the order in which Write accepted values.
type DelayQueue[T any] struct {
	delay   time.Duration
	deliver func(T, <-chan struct{}) bool
	input   chan delayedValue[T]
	abort   chan struct{}
	drain   chan struct{}
	done    chan struct{}

	next      atomic.Uint64
	scheduled atomic.Uint64
	delivered atomic.Uint64
	maxQueued atomic.Uint64

	writeMu   sync.Mutex
	writers   sync.WaitGroup
	stopped   bool
	closeOnce sync.Once
	abortOnce sync.Once
	drainOnce sync.Once
}

type DelayQueueStats struct {
	Scheduled uint64
	Delivered uint64
	Dropped   uint64
	MaxQueued uint64
}

func NewDelayQueue[T any](delay time.Duration, deliver func(T, <-chan struct{}) bool) *DelayQueue[T] {
	if delay < 0 {
		delay = 0
	}
	q := &DelayQueue[T]{
		delay:   delay,
		deliver: deliver,
		input:   make(chan delayedValue[T], delayQueueInputBuffer),
		abort:   make(chan struct{}),
		drain:   make(chan struct{}),
		done:    make(chan struct{}),
	}
	go q.run()
	return q
}

// Delivery is a stop-aware delayed action. Implementations must select on the
// supplied abort channel when their destination can block.
type Delivery func(<-chan struct{}) bool

func NewDeliveryQueue(delay time.Duration) *DelayQueue[Delivery] {
	return NewDelayQueue(delay, func(delivery Delivery, abort <-chan struct{}) bool {
		return delivery(abort)
	})
}

func ChannelDelivery[T any](channel chan<- T, value T) Delivery {
	return func(abort <-chan struct{}) bool {
		select {
		case channel <- value:
			return true
		case <-abort:
			return false
		}
	}
}

// Write schedules a value for arrival+delay. It returns false after Stop or
// CloseAndDrain has begun. The state lock is released before the potentially
// blocking enqueue so Stop can always close abort and release blocked writers.
func (q *DelayQueue[T]) Write(value T) bool {
	q.writeMu.Lock()
	if q.stopped {
		q.writeMu.Unlock()
		return false
	}
	q.writers.Add(1)
	q.writeMu.Unlock()
	defer q.writers.Done()

	item := delayedValue[T]{
		value: value,
		due:   time.Now().Add(q.delay),
		seq:   q.next.Add(1),
	}
	q.scheduled.Add(1)
	select {
	case q.input <- item:
		return true
	case <-q.abort:
		return false
	}
}

func (q *DelayQueue[T]) beginClose() {
	q.closeOnce.Do(func() {
		q.writeMu.Lock()
		q.stopped = true
		q.writeMu.Unlock()
	})
}

// Stop aborts pending deliveries and waits for the scheduler and every writer
// that entered before the stop to exit.
func (q *DelayQueue[T]) Stop() DelayQueueStats {
	q.beginClose()
	q.abortOnce.Do(func() {
		close(q.abort)
	})
	q.writers.Wait()
	<-q.done
	return q.Stats()
}

// CloseAndDrain rejects new writes, waits for writes already in progress to
// enqueue, then delivers every accepted value at its original deadline.
// A concurrent Stop escalates the graceful close to an abort.
func (q *DelayQueue[T]) CloseAndDrain() DelayQueueStats {
	q.beginClose()
	q.writers.Wait()
	q.drainOnce.Do(func() {
		close(q.drain)
	})
	<-q.done
	return q.Stats()
}

func (q *DelayQueue[T]) Stats() DelayQueueStats {
	scheduled := q.scheduled.Load()
	delivered := q.delivered.Load()
	return DelayQueueStats{
		Scheduled: scheduled,
		Delivered: delivered,
		Dropped:   scheduled - delivered,
		MaxQueued: q.maxQueued.Load(),
	}
}

func (q *DelayQueue[T]) updateMaxQueued(queued int) {
	value := uint64(queued)
	for {
		old := q.maxQueued.Load()
		if value <= old || q.maxQueued.CompareAndSwap(old, value) {
			return
		}
	}
}

func (q *DelayQueue[T]) run() {
	defer close(q.done)

	pending := make(delayedHeap[T], 0, delayQueueInputBuffer)
	heap.Init(&pending)
	drainC := q.drain
	draining := false
	timer := time.NewTimer(time.Hour)
	if !timer.Stop() {
		<-timer.C
	}
	defer timer.Stop()
	var timerC <-chan time.Time
	var timerDue time.Time
	stopTimer := func() {
		if timerC == nil {
			return
		}
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timerC = nil
		timerDue = time.Time{}
	}
	armTimer := func(due time.Time) {
		if timerC != nil && due.Equal(timerDue) {
			return
		}
		stopTimer()
		wait := time.Until(due)
		if wait < 0 {
			wait = 0
		}
		timer.Reset(wait)
		timerC = timer.C
		timerDue = due
	}

	for {
		select {
		case <-q.abort:
			return
		default:
		}

		if draining {
			for {
				select {
				case item := <-q.input:
					heap.Push(&pending, item)
					q.updateMaxQueued(len(pending) + len(q.input))
				default:
					goto inputDrained
				}
			}
		inputDrained:
			if len(pending) == 0 {
				return
			}
		}

		if len(pending) > 0 {
			next := pending[0]
			if wait := time.Until(next.due); wait <= 0 {
				stopTimer()
				item := heap.Pop(&pending).(delayedValue[T])
				if q.deliver(item.value, q.abort) {
					q.delivered.Add(1)
				}
				continue
			}
		}

		if len(pending) > 0 {
			armTimer(pending[0].due)
		}

		select {
		case <-q.abort:
			return
		case <-drainC:
			draining = true
			drainC = nil
		case item := <-q.input:
			heap.Push(&pending, item)
			q.updateMaxQueued(len(pending) + len(q.input))
		case <-timerC:
			timerC = nil
			timerDue = time.Time{}
		}
	}
}
