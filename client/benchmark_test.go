package client

import (
	"bufio"
	"encoding/binary"
	"io"
	"net"
	"os"
	"regexp"
	"strconv"
	"testing"
	"time"

	"github.com/imdea-software/swiftpaxos/config"
	"github.com/imdea-software/swiftpaxos/dlog"
	"github.com/imdea-software/swiftpaxos/replica/defs"
	"github.com/imdea-software/swiftpaxos/state"
)

func benchmarkConfig() *config.Config {
	return &config.Config{Benchmark: true, Alias: "us-east-1", WorkloadSeed: 1,
		KeyCount: 1000, KeyMappingSeed: 2, ShuffleKeys: true, ZipfSkew: .9,
		CommandSize: 1000, Writes: 50, ArrivalRate: 200,
		Warmup: 50 * time.Millisecond, Duration: 100 * time.Millisecond}
}

func TestBenchmarkStreamIndependentOfIdentityAndPayload(t *testing.T) {
	c := benchmarkConfig()
	first := NewBufferClient(&Client{ClientId: 10}, 0, 1000, 2, 50, 42)
	second := NewBufferClient(&Client{ClientId: 20}, 0, 1000, 2, 50, 42)
	first.ConfigureBenchmark(c, 0)
	second.ConfigureBenchmark(c, 0)
	a, b := first.benchmark, second.benchmark
	a.Logger, b.Logger = dlog.New("", false), dlog.New("", false)
	keyA, keyB := a.genGetKey(), b.genGetKey()
	for i := 0; i < 1000; i++ {
		if a.poissonInterval() != b.poissonInterval() || keyA() != keyB() || a.randomTrue(50) != b.randomTrue(50) {
			t.Fatal("identity or payload generation changed the sampling stream")
		}
		value := a.nextUpdateValue(123)
		if len(value) != 1000 || binary.LittleEndian.Uint64(value[:8]) != uint64(i+1) || binary.LittleEndian.Uint64(value[16:24]) != 123 {
			t.Fatal("UPDATE payload is missing its version/key")
		}
	}
}

func TestBenchmarkClockAndReplyAccounting(t *testing.T) {
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	defer writer.Close()
	previousStdout := os.Stdout
	os.Stdout = writer
	defer func() { os.Stdout = previousStdout }()
	logs := make(chan []byte, 1)
	go func() { data, _ := io.ReadAll(reader); logs <- data }()
	out, in := net.Pipe()
	defer in.Close()
	cl := &Client{Logger: dlog.New("", true), ClientId: 1, LeaderId: 0, seqnum: -1,
		servers: []net.Conn{out}, writers: []*bufio.Writer{bufio.NewWriter(out)}}
	b := NewBufferClient(cl, 0, 1000, 2, 50, 42)
	b.ConfigureBenchmark(benchmarkConfig(), 0)
	done := make(chan int, 1)
	go func() {
		reader, count := bufio.NewReader(in), 0
		for {
			kind, err := reader.ReadByte()
			if err != nil {
				done <- count
				return
			}
			if kind != defs.PROPOSE {
				panic("unexpected request marker")
			}
			var proposal defs.Propose
			if err := proposal.Unmarshal(reader); err != nil {
				panic(err)
			}
			if proposal.Command.K < 0 || proposal.Command.K >= 1000 || (proposal.Command.Op == state.PUT && len(proposal.Command.V) != 1000) {
				panic("workload request outside configuration")
			}
			count++
			time.Sleep(time.Millisecond)
			b.RegisterReply(nil, proposal.CommandId)
		}
	}()
	b.Loop()
	received := <-done
	writer.Close()
	log := <-logs
	count := func(label string) int {
		match := regexp.MustCompile(label + ` (\d+)`).FindSubmatch(log)
		if len(match) != 2 {
			t.Fatalf("missing %s", label)
		}
		n, _ := strconv.Atoi(string(match[1]))
		return n
	}
	warmup, measured := count("Warm-up requests"), count("Measured requests")
	samples := len(regexp.MustCompile(`latency (READ|UPDATE) `).FindAll(log, -1))
	if warmup == 0 || measured == 0 || received != 1+warmup+measured || samples != measured {
		t.Fatalf("bootstrap/warmup contaminated samples: received=%d warmup=%d measured=%d samples=%d", received, warmup, measured, samples)
	}
}
