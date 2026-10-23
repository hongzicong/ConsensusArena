package client

// Opt-in diagnostics for a load sweep. Fixed-window completion counts include
// warmup requests finishing during measurement; cohort histograms are separate.
// A one-second atomic snapshot survives a killed or overloaded client.
import (
	"math"
	"path/filepath"
	"sync"
	"time"
)

type throughputObserver struct {
	mu                         sync.Mutex
	run                        *latencyRun
	epoch                      time.Time
	offered, issued, completed [15]int64
	hist                       map[int64]int64
	done                       chan struct{}
	wg                         sync.WaitGroup
}

func newThroughputObserver(run *latencyRun) *throughputObserver {
	return &throughputObserver{run: run, hist: make(map[int64]int64), done: make(chan struct{})}
}

func (o *throughputObserver) start(epoch time.Time) {
	o.mu.Lock()
	o.epoch = epoch
	o.mu.Unlock()
	o.wg.Add(1)
	go func() {
		defer o.wg.Done()
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				o.snapshot(false)
			case <-o.done:
				o.snapshot(true)
				return
			}
		}
	}()
}

func (o *throughputObserver) count(at time.Time, buckets *[15]int64) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.epoch.IsZero() {
		return
	}
	i := int(at.Sub(o.epoch) / time.Second)
	if !at.Before(o.epoch) && i < len(buckets) {
		buckets[i]++
	}
}
func (o *throughputObserver) offer(at time.Time)    { o.count(at, &o.offered) }
func (o *throughputObserver) issue(at time.Time)    { o.count(at, &o.issued) }
func (o *throughputObserver) complete(at time.Time) { o.count(at, &o.completed) }
func (o *throughputObserver) latency(ms float64) {
	o.mu.Lock()
	o.hist[int64(math.Ceil(ms))]++
	o.mu.Unlock()
}
func (o *throughputObserver) snapshot(finished bool) {
	o.mu.Lock()
	hist := make(map[int64]int64, len(o.hist))
	for key, value := range o.hist {
		hist[key] = value
	}
	row := map[string]interface{}{
		"epoch_ns": o.epoch.UnixNano(), "snapshot_ns": time.Now().UnixNano(),
		"warmup_s": 5, "measurement_s": 10, "finished": finished,
		"offered_per_second": o.offered, "issued_per_second": o.issued,
		"completed_per_second": o.completed, "cohort_latency_histogram_ms_ceil": hist,
		"latency_origin": "scheduled_arrival", "completion_origin": "deduplicated_protocol_authorized_reply",
	}
	o.mu.Unlock()
	if err := latencyWrite(filepath.Join(o.run.dir, "results", o.run.label+"-throughput.json"), row); err != nil {
		panic(err)
	}
}
func (o *throughputObserver) close() { close(o.done); o.wg.Wait() }
