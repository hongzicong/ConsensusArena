package client

// Opt-in fault-experiment transport and observer. Protocol voting, certificates,
// caches and completion predicates are deliberately unchanged. Unanswered
// operations are censored, never resubmitted under a new consensus instance.
import (
	"encoding/json"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hongzicong/ConsensusArena/replica/defs"
)

type faultTransport struct {
	dir, alias string
	dead       []atomic.Bool
	replyFrom  atomic.Int32
	errors     atomic.Int64
	lastLookup time.Time // accessed by the single proposal sender
	rerouting  bool
}

func (c *BufferClient) ConfigureFaultRun(alias string, clone int) error {
	dir := os.Getenv("CONSENSUSARENA_FAULT_RUN")
	if dir == "" {
		return nil
	}
	if clone != 0 {
		return fmt.Errorf("fault runs require clones=0")
	}
	c.fault = &faultTransport{dir: dir, alias: alias, dead: make([]atomic.Bool, len(c.servers))}
	c.fault.replyFrom.Store(int32(c.ClosestId))
	return nil
}

func (c *Client) markFaultPeer(rid int) {
	if c.fault != nil && rid >= 0 && rid < len(c.fault.dead) {
		if !c.fault.dead[rid].Swap(true) {
			c.Printf("FAULT_PEER_DOWN replica=%d\n", rid)
		}
	}
}

func (c *Client) faultWrite(rid int, code uint8, msg interface{ Marshal(io.Writer) }) {
	if rid < 0 || rid >= len(c.servers) || c.fault.dead[rid].Load() || c.writers[rid] == nil {
		return
	}
	c.writeMu[rid].Lock()
	defer c.writeMu[rid].Unlock()
	if c.fault.dead[rid].Load() {
		return
	}
	_ = c.servers[rid].SetWriteDeadline(time.Now().Add(2 * time.Second))
	_ = c.writers[rid].WriteByte(code)
	msg.Marshal(c.writers[rid])
	if err := c.writers[rid].Flush(); err != nil {
		c.fault.errors.Add(1)
		c.markFaultPeer(rid)
		_ = c.servers[rid].Close()
	}
}

func (c *Client) nearestFaultSurvivor() int {
	best := -1
	for i := range c.servers {
		if c.fault.dead[i].Load() {
			continue
		}
		if best == -1 || (i < len(c.Ping) && best < len(c.Ping) && c.Ping[i] < c.Ping[best]) {
			best = i
		}
	}
	return best
}

func (c *Client) sendFaultProposal(cmd defs.Propose) {
	if c.Fast {
		for i := range c.servers {
			c.faultWrite(i, defs.PROPOSE, &cmd)
		}
		return
	}
	d := c.LeaderId
	if c.Leaderless {
		d = c.ClosestId
		if c.fault.dead[d].Load() {
			d = c.nearestFaultSurvivor()
			if d >= 0 {
				c.ClosestId = d
				c.fault.replyFrom.Store(int32(d))
			}
		}
	} else if d < 0 || c.fault.dead[d].Load() || c.fault.rerouting {
		c.fault.rerouting = true
		// Refresh only after a locally observed broken connection, not from the
		// injection schedule; the application pays the actual failover delay.
		if time.Since(c.fault.lastLookup) >= time.Second {
			c.fault.lastLookup = time.Now()
			r := &defs.GetLeaderReply{LeaderId: -1}
			if err := c.call(c.master, "Master.GetLeader", &defs.GetLeaderArgs{}, r); err == nil && r.LeaderId >= 0 && r.LeaderId < len(c.servers) && !c.fault.dead[r.LeaderId].Load() {
				c.LeaderId = r.LeaderId
				c.fault.replyFrom.Store(int32(r.LeaderId))
			}
		}
		d = c.LeaderId
	}
	c.faultWrite(d, defs.PROPOSE, &cmd)
}

func (c *BufferClient) waitFaultReplies(waitFrom int) {
	c.fault.replyFrom.Store(int32(waitFrom))
	for i := range c.readers {
		if c.readers[i] == nil {
			continue
		}
		go func(rid int) {
			for {
				r, err := c.GetReplyFrom(rid)
				if err != nil {
					c.markFaultPeer(rid)
					// Broadcast protocols may accept a completed reply from another
					// existing stream after the old reply source is lost.
					if c.Fast && int(c.fault.replyFrom.Load()) == rid {
						c.fault.replyFrom.Store(int32(c.nearestFaultSurvivor()))
					}
					return
				}
				if r.OK == defs.TRUE && int(c.fault.replyFrom.Load()) == rid {
					c.RegisterReply(r.Value, r.CommandId)
				}
			}
		}(i)
	}
}

type faultTiming struct {
	offered, sent time.Time
	operation     string
	phase         string
}
type faultRequest struct {
	scheduledRequest
	offered time.Time
}
type faultBucket struct {
	Offered        int         `json:"offered"`
	Issued         int         `json:"issued"`
	Dropped        int         `json:"dropped"`
	Completed      int         `json:"completed"`
	LatencySum     float64     `json:"latency_sum_ms"`
	SendLatencySum float64     `json:"send_latency_sum_ms"`
	Histogram      map[int]int `json:"latency_histogram_ms_ceil"`
}

func (b *faultBucket) complete(latency, sendLatency float64) {
	if b.Histogram == nil {
		b.Histogram = map[int]int{}
	}
	b.Completed++
	b.LatencySum += latency
	b.SendLatencySum += sendLatency
	b.Histogram[int(math.Ceil(latency))]++
}
func faultPhase(sec float64) string {
	switch {
	case sec < 0:
		return "warmup"
	case sec < 20:
		return "normal"
	case sec < 40:
		return "slow"
	case sec < 60:
		return "restored"
	default:
		return "crashed"
	}
}

func (c *BufferClient) loopFault(getKey func() int64) {
	f := c.fault
	ready := filepath.Join(f.dir, "status", "client-"+f.alias+".ready")
	if err := os.WriteFile(ready, []byte("ready\n"), 0644); err != nil {
		panic(err)
	}
	var epoch time.Time
	for {
		data, err := os.ReadFile(filepath.Join(f.dir, "status", "start-unix-ns"))
		if err == nil {
			ns, err := strconv.ParseInt(strings.TrimSpace(string(data)), 10, 64)
			if err != nil {
				panic(err)
			}
			epoch = time.Unix(0, ns)
			break
		}
		if _, err := os.Stat(filepath.Join(f.dir, "status", "stop")); err == nil {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	measurement := epoch.Add(c.warmup)
	end := measurement.Add(c.duration)
	if d := time.Until(epoch); d > 0 {
		time.Sleep(d)
	}
	path := filepath.Join(f.dir, "results", f.alias+"-fault.jsonl")
	file, err := os.Create(path)
	if err != nil {
		panic(err)
	}
	defer file.Close()
	enc := json.NewEncoder(file)
	if err := enc.Encode(map[string]interface{}{"type": "start", "epoch_ns": epoch.UnixNano(), "measurement_ns": measurement.UnixNano(), "actual_start_ns": time.Now().UnixNano(), "duration_s": c.duration.Seconds(), "client": f.alias, "client_id": c.ClientId, "retry_policy": "none; unresolved requests are censored", "histogram_resolution_ms": 1}); err != nil {
		panic(err)
	}
	var mu sync.Mutex
	stats := faultBucket{}
	cohorts := map[string]*faultBucket{}
	for _, p := range []string{"warmup", "normal", "slow", "restored", "crashed"} {
		for _, op := range []string{"READ", "UPDATE"} {
			cohorts[p+"/"+op] = &faultBucket{}
		}
	}
	pending := map[int]faultTiming{}
	queue := make(chan faultRequest, 1024)
	stop := make(chan struct{})
	var totalOffered, totalIssued, totalCompleted, totalDropped, duplicates int
	// Includes requests between the generator, channel and sender. len(queue)
	// alone can miss a request during a handoff at the observation deadline.
	var unissued int
	var observerSamples, observerSampleNS, encodeNS int64
	go func() {
		next := epoch
		for {
			next = next.Add(c.poissonInterval())
			if !next.Before(end) {
				close(queue)
				return
			}
			d := time.Until(next)
			if d > 0 {
				select {
				case <-time.After(d):
				case <-stop:
					return
				}
			}
			r := faultRequest{scheduledRequest: scheduledRequest{key: getKey(), write: c.randomTrue(c.writes)}, offered: next}
			op := "READ"
			if r.write {
				op = "UPDATE"
			}
			mu.Lock()
			stats.Offered++
			totalOffered++
			unissued++
			cohorts[faultPhase(next.Sub(measurement).Seconds())+"/"+op].Offered++
			mu.Unlock()
			select {
			case <-stop:
				return
			case queue <- r:
			default:
				mu.Lock()
				stats.Dropped++
				totalDropped++
				unissued--
				cohorts[faultPhase(next.Sub(measurement).Seconds())+"/"+op].Dropped++
				mu.Unlock()
			}
		}
	}()
	go func() {
		for {
			select {
			case <-stop:
				return
			case req, ok := <-queue:
				if !ok {
					return
				}
				op := "READ"
				if req.write {
					op = "UPDATE"
				}
				timing := faultTiming{req.offered, time.Now(), op, faultPhase(req.offered.Sub(measurement).Seconds())}
				mu.Lock()
				pending[int(c.seqnum+1)] = timing
				stats.Issued++
				totalIssued++
				unissued--
				cohorts[timing.phase+"/"+op].Issued++
				mu.Unlock()
				c.sendScheduledRequest(req.scheduledRequest)
			}
		}
	}()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	deadline := time.NewTimer(time.Until(end))
	defer deadline.Stop()
	last := epoch
	writeSample := func(now time.Time, final bool) {
		encodeStart := time.Now()
		mu.Lock()
		defer mu.Unlock()
		row := map[string]interface{}{"type": "sample", "start_s": last.Sub(measurement).Seconds(), "end_s": now.Sub(measurement).Seconds(), "stats": stats, "pending": len(pending), "queue": len(queue), "send_errors": f.errors.Load(), "total_offered": totalOffered, "total_issued": totalIssued, "total_completed": totalCompleted, "total_dropped": totalDropped, "duplicate_or_unknown_replies": duplicates}
		row["unissued"] = unissued
		if final {
			unresolved := map[string]int{}
			for _, t := range pending {
				unresolved[t.phase+"/"+t.operation]++
			}
			row["type"] = "final"
			row["cohorts"] = cohorts
			row["unresolved"] = unresolved
			row["completion_observer_samples"] = observerSamples
			row["completion_observer_sample_ns"] = observerSampleNS
			row["prior_sample_serialization_ns"] = encodeNS
		}
		if err := enc.Encode(row); err != nil {
			panic(err)
		}
		encodeNS += time.Since(encodeStart).Nanoseconds()
		stats = faultBucket{}
		last = now
	}
	for {
		select {
		case r := <-c.Reply:
			var observeStart time.Time
			if totalCompleted%128 == 0 {
				observeStart = time.Now()
			}
			mu.Lock()
			if t, ok := pending[r.Seqnum]; ok {
				delete(pending, r.Seqnum)
				totalCompleted++
				latency := float64(r.Time.Sub(t.offered)) / float64(time.Millisecond)
				sendLatency := float64(r.Time.Sub(t.sent)) / float64(time.Millisecond)
				stats.complete(latency, sendLatency)
				cohorts[t.phase+"/"+t.operation].complete(latency, sendLatency)
			} else {
				duplicates++
			}
			mu.Unlock()
			if !observeStart.IsZero() {
				observerSamples++
				observerSampleNS += time.Since(observeStart).Nanoseconds()
			}
		case now := <-ticker.C:
			writeSample(now, false)
		case <-deadline.C:
			close(stop)
			writeSample(time.Now(), true)
			c.Disconnect()
			return
		}
	}
}

// Nearest-rank percentile on mergeable millisecond-ceiling histogram values.
func faultPercentile(hist map[int]int, fraction float64) int {
	keys := make([]int, 0, len(hist))
	total := 0
	for k, n := range hist {
		keys = append(keys, k)
		total += n
	}
	sort.Ints(keys)
	target := int(math.Ceil(float64(total) * fraction))
	sum := 0
	for _, k := range keys {
		sum += hist[k]
		if sum >= target {
			return k
		}
	}
	return 0
}
