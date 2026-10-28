package client

// Ordinary latency experiments share one offered-load clock. The fault observer
// retains its separate fixed observation window and unresolved-request policy.
import (
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

type latencyRun struct {
	dir, label string
}

const (
	latencyWarmup        = 5 * time.Second
	latencyMeasurement   = 10 * time.Second
	latencySustain       = 2 * time.Second
	latencyTimeout       = 19 * time.Second
	latencyWorkloadModel = "synchronized-ramp-fixed-sustain-v2"
)

type latencyReport struct {
	WorkloadModel     string  `json:"workload_model"`
	EpochNS           int64   `json:"epoch_ns"`
	ActualStartNS     int64   `json:"actual_start_ns"`
	MeasurementEndNS  int64   `json:"measurement_end_ns"`
	MeasuredDoneNS    int64   `json:"measured_done_ns"`
	LoadStopNS        int64   `json:"load_stop_ns"`
	DrainDoneNS       int64   `json:"drain_done_ns"`
	DeadlineNS        int64   `json:"deadline_ns"`
	WarmupS           float64 `json:"warmup_s"`
	RampS             float64 `json:"ramp_s"`
	MeasurementS      float64 `json:"measurement_s"`
	SustainS          float64 `json:"sustain_s"`
	TimeoutS          float64 `json:"experiment_timeout_s"`
	ArrivalRate       float64 `json:"arrival_rate"`
	WarmupRequests    int     `json:"warmup_requests"`
	MeasuredRequests  int     `json:"measured_requests"`
	CompletedRequests int     `json:"completed_requests"`
	IssuedRequests    int     `json:"issued_requests"`
	AllCompleted      int     `json:"all_completed_requests"`
	SustainRequests   int     `json:"sustain_requests"`
	OfferedPerSecond  []int   `json:"offered_per_second"`
	IssuedPerSecond   []int   `json:"issued_per_second"`
	QueueSumMS        float64 `json:"queue_sum_ms"`
	QueueMaxMS        float64 `json:"queue_max_ms"`
}

// ConfigureLatencyRun opts Slurm clients into a shared-filesystem barrier. The
// file label includes the clone index so every logical generator participates.
func (c *BufferClient) ConfigureLatencyRun(alias string, clone int) error {
	dir := os.Getenv("CONSENSUSARENA_LATENCY_RUN")
	if dir == "" {
		return nil
	}
	if c.fault != nil {
		return fmt.Errorf("latency and fault barriers cannot be enabled together")
	}
	c.latency = &latencyRun{dir: dir, label: alias + "-" + strconv.Itoa(clone)}
	return nil
}

func latencyWrite(path string, value interface{}) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	if err := os.WriteFile(path+".tmp", append(data, '\n'), 0644); err != nil {
		return err
	}
	return os.Rename(path+".tmp", path)
}

func (r *latencyRun) status(name string) string {
	return filepath.Join(r.dir, "status", name)
}

func (r *latencyRun) waitEpoch(name string) (time.Time, error) {
	for {
		if _, err := os.Stat(r.status("stop")); err == nil {
			return time.Time{}, fmt.Errorf("latency experiment stopped before %s", name)
		}
		data, err := os.ReadFile(r.status(name))
		if err == nil {
			ns, err := strconv.ParseInt(strings.TrimSpace(string(data)), 10, 64)
			if err != nil || ns <= 0 {
				return time.Time{}, fmt.Errorf("invalid latency barrier %s", name)
			}
			// Retain a monotonic clock after translating the shared wall epoch.
			now := time.Now()
			return now.Add(time.Unix(0, ns).Sub(now)), nil
		}
		if !os.IsNotExist(err) {
			return time.Time{}, err
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func (c *BufferClient) latencyStart() (time.Time, error) {
	if c.latency == nil {
		return time.Now(), nil
	}
	r := c.latency
	if err := latencyWrite(r.status("client-"+r.label+".ready"), map[string]interface{}{
		"ready_ns": time.Now().UnixNano(), "arrival_rate": c.arrivalRate,
		"warmup_s": c.warmup.Seconds(), "measurement_s": c.duration.Seconds(),
		"sustain_s": latencySustain.Seconds(), "experiment_timeout_s": latencyTimeout.Seconds(),
		"workload_model": latencyWorkloadModel,
	}); err != nil {
		return time.Time{}, err
	}
	epoch, err := r.waitEpoch("start-unix-ns")
	if err != nil {
		return time.Time{}, err
	}
	if time.Since(epoch) > 100*time.Millisecond {
		return time.Time{}, fmt.Errorf("client missed the shared workload epoch")
	}
	if delay := time.Until(epoch); delay > 0 {
		time.Sleep(delay)
	}
	if time.Since(epoch) > 100*time.Millisecond {
		return time.Time{}, fmt.Errorf("client started more than 100ms late")
	}
	return epoch, nil
}

func (c *BufferClient) latencyDrain(done time.Time) error {
	if c.latency == nil {
		return nil
	}
	r := c.latency
	if err := latencyWrite(r.status("client-"+r.label+".drained"), map[string]int64{"done_ns": done.UnixNano()}); err != nil {
		return err
	}
	// Keep every client's streams alive until all generators have drained.
	epoch, err := r.waitEpoch("drain-complete-unix-ns")
	if err != nil {
		return err
	}
	if epoch.Before(done) {
		return fmt.Errorf("shared drain completed before this client")
	}
	return nil
}

func (c *BufferClient) writeLatencyReport(report latencyReport) error {
	if c.latency == nil {
		return nil
	}
	return latencyWrite(filepath.Join(c.latency.dir, "results", c.latency.label+"-workload.json"), report)
}

func countArrival(buckets []int, epoch, at time.Time) []int {
	i := int(at.Sub(epoch) / time.Second)
	if i < 0 {
		panic("arrival before workload epoch")
	}
	for len(buckets) <= i {
		buckets = append(buckets, 0)
	}
	buckets[i]++
	return buckets
}

type requestTiming struct {
	plannedAt, sentAt time.Time
	write             bool
	measured          bool
}

func (c *BufferClient) loopOpen(getKey func() int64) {
	c.warmup, c.duration = latencyWarmup, latencyMeasurement
	c.Printf("Workload seed %d\n", c.workloadSeed)
	c.Printf("Open-loop Poisson arrival rate %v requests/second\n", c.arrivalRate)
	c.Printf("Warm-up %v, measurement duration %v\n", c.warmup, c.duration)
	// Expensive key setup (Loop), connections and one end-to-end bootstrap all
	// finish before READY. No client starts its warmup while peers still prepare.
	c.sendScheduledRequest(scheduledRequest{key: getKey(), write: c.randomTrue(c.writes)})
	<-c.Reply
	if os.Getenv("CONSENSUSARENA_THROUGHPUT") == "1" && c.latency != nil {
		c.throughput = newThroughputObserver(c.latency)
		defer c.throughput.close()
	}

	requests := make(chan scheduledRequest, 1024)
	stopReplies := make(chan struct{})
	measurementEnqueued := make(chan struct{})
	measurementFinished := make(chan struct{})
	var senders, replies, allReplies, receiver sync.WaitGroup
	var timings sync.Map
	var start, measuredDone time.Time
	var offered, issued []int
	measuredRequests, warmupRequests, sustainRequests, completedRequests := 0, 0, 0, 0
	issuedRequests, allCompleted := 0, 0
	var queueSum, queueMax float64

	receiver.Add(1)
	go func() {
		defer receiver.Done()
		for {
			select {
			case reply := <-c.Reply:
				value, exists := timings.LoadAndDelete(reply.Seqnum)
				if !exists {
					continue // Bootstrap and duplicate replies are not outstanding work.
				}
				timing := value.(requestTiming)
				allCompleted++
				if !timing.measured {
					allReplies.Done()
					continue
				}
				milliseconds := float64(reply.Time.Sub(timing.plannedAt)) / float64(time.Millisecond)
				queueMS := float64(timing.sentAt.Sub(timing.plannedAt)) / float64(time.Millisecond)
				queueSum += queueMS
				queueMax = math.Max(queueMax, queueMS)
				completedRequests++
				if c.throughput != nil {
					c.throughput.latency(milliseconds)
				}
				operation := "READ"
				if timing.write {
					operation = "UPDATE"
				}
				c.Printf("latency %s %v\n", operation, milliseconds)
				replies.Done()
				allReplies.Done()
			case <-stopReplies:
				return
			}
		}
	}()
	senders.Add(1)
	go func() {
		defer senders.Done()
		for request := range requests {
			sentAt := time.Now()
			if c.throughput != nil {
				c.throughput.issue(sentAt)
			}
			issued = countArrival(issued, start, sentAt)
			issuedRequests++
			timings.Store(int(c.seqnum+1), requestTiming{request.plannedAt, sentAt, request.write, request.measured})
			c.sendScheduledRequest(request)
		}
	}()
	var err error
	start, err = c.latencyStart()
	if err != nil {
		panic(err)
	}
	actualStart := time.Now()
	if c.throughput != nil {
		c.throughput.start(start)
	}
	warmupEnd := start.Add(c.warmup)
	measurementEnd := warmupEnd.Add(c.duration)
	loadStop := measurementEnd.Add(latencySustain)
	deadline := start.Add(latencyTimeout)
	watchdog := time.AfterFunc(time.Until(deadline), func() {
		panic("latency experiment timed out: requests did not drain within 19s of the workload epoch")
	})
	defer watchdog.Stop()
	c.Printf("LATENCY_WORKLOAD_START epoch_ns=%d actual_start_ns=%d warmup_s=%g ramp_s=%g measurement_s=%g sustain_s=2 experiment_timeout_s=19 load_stop_ns=%d arrival_rate=%g latency_origin=scheduled_arrival workload_model=%s\n",
		start.UnixNano(), actualStart.UnixNano(), c.warmup.Seconds(), (c.warmup / 2).Seconds(), c.duration.Seconds(), loadStop.UnixNano(), c.arrivalRate, latencyWorkloadModel)
	go func() {
		// Wait begins only once the producer has added every measured request.
		<-measurementEnqueued
		replies.Wait()
		measuredDone = time.Now()
		close(measurementFinished)
	}()
	arrivalClock := 0.0
	measurementClosed := false
	for {
		// Invert integrated intensity for a linear ramp followed by constant rate.
		// Scheduled timestamps survive scheduler lag and a full sender queue.
		arrivalClock += c.rand.ExpFloat64() / c.arrivalRate
		ramp := (c.warmup / 2).Seconds()
		elapsed := arrivalClock + ramp/2
		if ramp > 0 && arrivalClock < ramp/2 {
			elapsed = math.Sqrt(2 * ramp * arrivalClock)
		}
		nextArrival := start.Add(time.Duration(elapsed * float64(time.Second)))
		if !measurementClosed && !nextArrival.Before(measurementEnd) {
			time.Sleep(time.Until(measurementEnd))
			close(measurementEnqueued)
			measurementClosed = true
		}
		if !nextArrival.Before(loadStop) {
			time.Sleep(time.Until(loadStop))
			break
		}
		time.Sleep(time.Until(nextArrival))
		measured := !nextArrival.Before(warmupEnd) && nextArrival.Before(measurementEnd)
		if measured {
			measuredRequests++
			replies.Add(1)
		} else if nextArrival.Before(warmupEnd) {
			warmupRequests++
		} else {
			sustainRequests++
		}
		offered = countArrival(offered, start, nextArrival)
		if c.throughput != nil {
			c.throughput.offer(time.Now())
		}
		request := scheduledRequest{key: getKey(), write: c.randomTrue(c.writes), measured: measured, plannedAt: nextArrival}
		allReplies.Add(1)
		requests <- request
	}
	close(requests)
	// Drain every queued request, including warmup and sustain, without dropping
	// measured arrivals that were delayed in the sender queue past second 17.
	senders.Wait()
	allReplies.Wait()
	<-measurementFinished
	close(stopReplies)
	receiver.Wait()
	drainDone := time.Now()
	if !drainDone.Before(deadline) {
		panic("latency experiment exceeded its 19s deadline")
	}
	watchdog.Stop()
	if measuredRequests != completedRequests || issuedRequests != allCompleted ||
		issuedRequests != warmupRequests+measuredRequests+sustainRequests {
		panic("latency requests did not all drain")
	}
	report := latencyReport{
		WorkloadModel: latencyWorkloadModel,
		EpochNS:       start.UnixNano(), ActualStartNS: actualStart.UnixNano(), MeasurementEndNS: measurementEnd.UnixNano(),
		MeasuredDoneNS: measuredDone.UnixNano(), LoadStopNS: loadStop.UnixNano(),
		DrainDoneNS: drainDone.UnixNano(), DeadlineNS: deadline.UnixNano(),
		WarmupS: c.warmup.Seconds(), RampS: (c.warmup / 2).Seconds(), MeasurementS: c.duration.Seconds(), ArrivalRate: c.arrivalRate,
		SustainS: latencySustain.Seconds(), TimeoutS: latencyTimeout.Seconds(),
		WarmupRequests: warmupRequests, MeasuredRequests: measuredRequests, CompletedRequests: completedRequests, SustainRequests: sustainRequests,
		IssuedRequests: issuedRequests, AllCompleted: allCompleted,
		OfferedPerSecond: offered, IssuedPerSecond: issued, QueueSumMS: queueSum, QueueMaxMS: queueMax,
	}
	if err := c.writeLatencyReport(report); err != nil {
		panic(err)
	}
	if err := c.latencyDrain(drainDone); err != nil {
		panic(err)
	}
	c.Printf("Warm-up requests %d\n", warmupRequests)
	c.Printf("Measured requests %d\n", measuredRequests)
	c.Printf("Sustain requests %d\n", sustainRequests)
	c.Printf("LATENCY_QUEUE measured=%d sum_ms=%g max_ms=%g\n", completedRequests, queueSum, queueMax)
	c.Printf("Test took %v\n", time.Since(start))
	c.Disconnect()
}
