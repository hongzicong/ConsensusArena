// Sampling and clocks copied from pinned modular master; original protocol clients retained.
package client

import (
	"encoding/binary"
	"github.com/hongzicong/ConsensusArena/config"
	"github.com/hongzicong/ConsensusArena/state"
	"hash/fnv"
	"math"
	"math/rand"
	"sort"
	"sync"
	"time"
)

type BenchmarkClient struct {
	*BufferClient

	psize          int
	writes         int
	arrivalRate    float64
	keyCount       int
	zipfSkew       float64
	shuffleKeys    bool
	keyMappingSeed int64
	warmup         time.Duration
	duration       time.Duration

	rand          *rand.Rand
	valueRand     *rand.Rand
	workloadSeed  int64
	updateVersion uint64
	uniqueKeys    bool
	keyStart      int64
	keyEnd        int64
	uniqueIssued  int64
}
type zipfParameters struct {
	keyCount int
	skew     float64
}

var zipfCDFCache = struct {
	sync.Mutex
	values map[zipfParameters][]float64
}{
	values: make(map[zipfParameters][]float64),
}

type keyMappingParameters struct {
	keyCount int
	seed     int64
}

// Cached permutations are immutable and shared by clones in this process.
var keyMappingCache = struct {
	sync.Mutex
	values map[keyMappingParameters][]int
}{
	values: make(map[keyMappingParameters][]int),
}

func (b *BufferClient) ConfigureBenchmark(c *config.Config, clone int) {
	seed := DeriveWorkloadSeed(int64(c.WorkloadSeed), c.Alias, clone)
	b.Reply = make(chan *ReqReply, 1024)
	b.benchmark = &BenchmarkClient{BufferClient: b, psize: c.CommandSize, writes: c.Writes, arrivalRate: c.ArrivalRate, keyCount: c.KeyCount, zipfSkew: c.ZipfSkew, shuffleKeys: c.ShuffleKeys, keyMappingSeed: int64(c.KeyMappingSeed), warmup: c.Warmup, duration: c.Duration, workloadSeed: seed, rand: rand.New(rand.NewSource(seed)), valueRand: rand.New(rand.NewSource(mixWorkloadSeed(seed)))}
}

type scheduledRequest struct {
	key      int64
	write    bool
	measured bool
}

type requestTiming struct {
	sentAt   time.Time
	measured bool
	write    bool
}

func DeriveWorkloadSeed(baseSeed int64, alias string, clone int) int64 {
	hasher := fnv.New64a()
	var encoded [16]byte
	binary.LittleEndian.PutUint64(encoded[:8], uint64(baseSeed))
	binary.LittleEndian.PutUint64(encoded[8:], uint64(clone))
	hasher.Write(encoded[:])
	hasher.Write([]byte(alias))
	return int64(hasher.Sum64() & ((uint64(1) << 63) - 1))
}

func mixWorkloadSeed(seed int64) int64 {
	value := uint64(seed) + 0x9e3779b97f4a7c15
	value = (value ^ (value >> 30)) * 0xbf58476d1ce4e5b9
	value = (value ^ (value >> 27)) * 0x94d049bb133111eb
	return int64(value ^ (value >> 31))
}

func (c *BenchmarkClient) loopOpen(getKey func() int64) {
	c.Printf("Workload seed %d\n", c.workloadSeed)
	c.Printf("Open-loop Poisson arrival rate %v requests/second\n", c.arrivalRate)
	c.Printf("Warm-up %v, measurement duration %v\n", c.warmup, c.duration)

	c.sendScheduledRequest(scheduledRequest{
		key:   getKey(),
		write: c.randomTrue(c.writes),
	})
	<-c.Reply

	requests := make(chan scheduledRequest, 1024)
	stopReplies := make(chan struct{})
	var senders sync.WaitGroup
	var replies sync.WaitGroup
	var timings sync.Map
	measuredRequests := 0
	warmupRequests := 0

	go func() {
		for {
			select {
			case reply := <-c.Reply:
				value, exists := timings.LoadAndDelete(reply.Seqnum)
				if !exists {
					continue
				}
				timing := value.(requestTiming)
				if timing.measured {
					d := reply.Time.Sub(timing.sentAt)
					milliseconds := float64(d.Nanoseconds()) / float64(time.Millisecond)
					operation := "READ"
					if timing.write {
						operation = "UPDATE"
					}
					c.Printf("latency %s %v\n", operation, milliseconds)
				}
				replies.Done()
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
			sequence := int(c.seqnum + 1)
			timings.Store(sequence, requestTiming{
				sentAt:   sentAt,
				measured: request.measured,
				write:    request.write,
			})
			replies.Add(1)
			c.sendScheduledRequest(request)
		}
	}()

	start := time.Now()
	warmupEnd := start.Add(c.warmup)
	measurementEnd := warmupEnd.Add(c.duration)
	nextArrival := start
	for {
		nextArrival = nextArrival.Add(c.poissonInterval())
		if !nextArrival.Before(measurementEnd) {
			break
		}
		if delay := time.Until(nextArrival); delay > 0 {
			time.Sleep(delay)
		}

		measured := !nextArrival.Before(warmupEnd)
		if measured {
			measuredRequests++
		} else {
			warmupRequests++
		}
		requests <- scheduledRequest{
			key:      getKey(),
			write:    c.randomTrue(c.writes),
			measured: measured,
		}
	}
	close(requests)
	senders.Wait()
	replies.Wait()
	close(stopReplies)

	c.Printf("Warm-up requests %d\n", warmupRequests)
	c.Printf("Measured requests %d\n", measuredRequests)
	c.Printf("Test took %v\n", time.Since(start))
	c.Disconnect()
}

func (c *BenchmarkClient) poissonInterval() time.Duration {
	seconds := c.rand.ExpFloat64() / c.arrivalRate
	return time.Duration(seconds * float64(time.Second))
}

func (c *BenchmarkClient) sendScheduledRequest(request scheduledRequest) {
	if request.write {
		c.SendWrite(request.key, c.nextUpdateValue(request.key))
	} else {
		c.SendRead(request.key)
	}
}

func (c *BenchmarkClient) nextUpdateValue(key int64) state.Value {
	c.updateVersion++
	value := make(state.Value, c.psize)
	c.valueRand.Read(value)
	var metadata [24]byte
	binary.LittleEndian.PutUint64(metadata[:8], c.updateVersion)
	binary.LittleEndian.PutUint64(metadata[8:16], uint64(c.workloadSeed))
	binary.LittleEndian.PutUint64(metadata[16:], uint64(key))
	copy(value, metadata[:])
	return value
}

func (c *BenchmarkClient) genGetKey() func() int64 {
	if c.uniqueKeys {
		c.Printf("WORKLOAD_KEY_MODE mode=unique start=%d end=%d", c.keyStart, c.keyEnd)
		return func() int64 {
			// Preserve the shared workload's RNG consumption so arrival and
			// operation streams stay identical across key modes and protocols.
			c.rand.Float64()
			key := c.keyStart + c.uniqueIssued
			if key >= c.keyEnd {
				panic("unique-key range exhausted; refusing to reuse a key")
			}
			c.uniqueIssued++
			return key
		}
	}
	cdf := sharedZipfCDF(c.keyCount, c.zipfSkew)
	c.Printf("Zipfian key distribution: keyCount=%d skew=%v\n", c.keyCount, c.zipfSkew)
	if c.shuffleKeys {
		// Generate once before the workload clock starts, with a separate RNG.
		// A bijection preserves all rank probabilities and same-key conflicts.
		keys := sharedKeyMapping(c.keyCount, c.keyMappingSeed)
		c.Printf("WORKLOAD_KEY_MAPPING mode=shuffled algorithm=rand-perm-v1 seed=%d keyCount=%d\n", c.keyMappingSeed, c.keyCount)
		return func() int64 {
			rank := sort.SearchFloat64s(cdf, c.rand.Float64())
			return int64(keys[rank])
		}
	}
	c.Printf("WORKLOAD_KEY_MAPPING mode=ordered keyCount=%d\n", c.keyCount)
	getKey := func() int64 {
		return int64(sort.SearchFloat64s(cdf, c.rand.Float64()))
	}
	return getKey
}

func sharedKeyMapping(keyCount int, seed int64) []int {
	parameters := keyMappingParameters{keyCount: keyCount, seed: seed}
	keyMappingCache.Lock()
	defer keyMappingCache.Unlock()
	if keys, exists := keyMappingCache.values[parameters]; exists {
		return keys
	}
	keys := rand.New(rand.NewSource(seed)).Perm(keyCount)
	keyMappingCache.values[parameters] = keys
	return keys
}

func sharedZipfCDF(keyCount int, skew float64) []float64 {
	parameters := zipfParameters{
		keyCount: keyCount,
		skew:     skew,
	}

	zipfCDFCache.Lock()
	defer zipfCDFCache.Unlock()
	if cdf, exists := zipfCDFCache.values[parameters]; exists {
		return cdf
	}

	cdf := make([]float64, keyCount)
	total := 0.0
	for key := 1; key <= keyCount; key++ {
		total += math.Pow(float64(key), -skew)
		cdf[key-1] = total
	}
	for key := range cdf {
		cdf[key] /= total
	}
	cdf[len(cdf)-1] = 1
	zipfCDFCache.values[parameters] = cdf
	return cdf
}

func (c *BenchmarkClient) randomTrue(prob int) bool {
	if prob >= 100 {
		return true
	}
	if prob > 0 {
		return c.rand.Intn(100) < prob
	}
	return false
}
