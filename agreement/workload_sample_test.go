package agreement

import (
	"fmt"
	"math"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/hongzicong/ConsensusArena/client"
	"github.com/hongzicong/ConsensusArena/config"
	"github.com/hongzicong/ConsensusArena/heatprofile"
	"github.com/hongzicong/ConsensusArena/placement"
	"github.com/hongzicong/ConsensusArena/replica/defs"
	"github.com/hongzicong/ConsensusArena/state"
)

func sampledMixFixture(t *testing.T, writes uint64) (string, string, *placement.Topology) {
	t.Helper()
	hotWrites := writes
	if hotWrites > 900 {
		hotWrites = 900
	}
	tailWrites := writes - hotWrites
	sample := heatprofile.File{Schema: 1, Source: "independent-offline-offered-requests",
		KeyCount: 2, Requests: 1000, SampleSeed: 101,
		Keys: []heatprofile.KeyCount{
			{Key: 0, Reads: 900 - hotWrites, Writes: hotWrites},
			{Key: 1, Reads: 100 - tailWrites, Writes: tailWrites},
		}}
	path := filepath.Join(t.TempDir(), "heat-profile.json")
	digest, err := heatprofile.Write(path, sample)
	if err != nil {
		t.Fatal(err)
	}
	topology := &placement.Topology{
		Replicas: []config.PlannedReplica{
			{Alias: "r0", Rank: 0}, {Alias: "r1", Rank: 1}, {Alias: "r2", Rank: 2},
		},
		D:       [][]float64{{0, 10, 20}, {7, 0, 12}, {9, 14, 0}},
		Clients: []string{"c0", "c1"}, Weights: []float64{.5, .5},
		To: [][]float64{{0, 10, 20}, {20, 10, 0}}, Back: [][]float64{{0, 7, 9}, {20, 12, 0}},
	}
	return path, digest, topology
}

func sampledMixConfig(path, digest string, configuredWrites int) *config.Config {
	return &config.Config{KeyCount: 2, Writes: configuredWrites, ArrivalRate: 100,
		ReplicaAliases: []string{"r0", "r1", "r2"}, Alias: "c0",
		HeatProfilePath: path, HeatProfileSHA256: digest}
}

func TestSampledMixControlsPlansAndFeaturesWithoutChangingWorkload(t *testing.T) {
	// Include rare writes below one percent: rounding to an integer percentage
	// would incorrectly disable the certificate paths in this case.
	for _, sampledWrites := range []uint64{0, 1, 200, 1000} {
		t.Run(fmt.Sprintf("sample_writes_%d", sampledWrites), func(t *testing.T) {
			path, digest, topology := sampledMixFixture(t, sampledWrites)
			var reference config.DeploymentPlan
			for i, configuredWrites := range []int{0, 5, 50, 100} {
				c := sampledMixConfig(path, digest, configuredWrites)
				p, err := PlanDeployment(c, topology)
				if err != nil {
					t.Fatal(err)
				}
				if c.Writes != configuredWrites {
					t.Fatal("planning changed the workload generator's write percentage")
				}
				if c.HeatProfile.WriteRequests != sampledWrites || c.HeatProfile.ReadRequests != 1000-sampledWrites {
					t.Fatal("cached operation counts disagree with the frozen sample")
				}
				if math.Abs(sampledWriteFraction(c)-float64(sampledWrites)/1000) > 1e-15 {
					t.Fatal("sampled write fraction lost precision")
				}
				if i == 0 {
					reference = p
				} else if !reflect.DeepEqual(reference, p) {
					t.Fatalf("configured writes=%d changed placement despite identical observations", configuredWrites)
				}
				c.Plan = p
				opt, err := readOptions(c, 3)
				if err != nil {
					t.Fatal(err)
				}
				adapter := NewClient(&client.BufferClient{Client: &client.Client{}}, c)
				if opt.ClientLearning != (sampledWrites > 0) || adapter.learningEnabled != opt.ClientLearning {
					t.Fatal("replica/client certificate switches do not follow sampled writes")
				}
			}
		})
	}
}

func TestNoSampledWritesStillAllowsOrdinaryWriteCompletion(t *testing.T) {
	path, digest, topology := sampledMixFixture(t, 0)
	c := sampledMixConfig(path, digest, 50)
	plan, err := PlanDeployment(c, topology)
	if err != nil {
		t.Fatal(err)
	}
	c.Plan = plan
	opt, err := readOptions(c, 3)
	if err != nil {
		t.Fatal(err)
	}
	if opt.ClientLearning || plan.ColdEnabled {
		t.Fatal("read-only sample enabled write-specific optimizations")
	}
	type packet struct {
		to int
		m  message
	}
	var packets []packet
	now := time.Now()
	nodes := make([]*engine, 3)
	states := make([]*state.State, 3)
	id := defs.RequestID{Client: 1, Sequence: 1}
	completed := false
	for rank := range nodes {
		e := newEngine(rank, 3, opt, now)
		e.current = roster{Ballot: 1, Leader: 0, Responders: opt.Responders, Ranges: opt.Ranges}
		e.prepared = true
		for peer := range nodes {
			e.incoming[peer] = grant{Until: now.Add(time.Hour)}
		}
		states[rank] = state.InitState()
		e.execute = func(cmd state.Command) state.Value { return cmd.Execute(states[rank]) }
		e.send = func(to int, m message) { packets = append(packets, packet{to, m}) }
		e.reply = func(r request, _ state.Value) {
			if r.id() == id {
				completed = true
			}
		}
		nodes[rank] = e
	}
	req := request{Origin: 0, Proposal: defs.Propose{ClientId: id.Client, CommandId: id.Sequence,
		Command: state.Command{Op: state.PUT, K: 0, V: state.Value("new")}}}
	nodes[0].submit(req, now)
	for budget := 1000; len(packets) > 0; budget-- {
		if budget == 0 {
			t.Fatal("ordinary write did not quiesce")
		}
		p := packets[0]
		packets = packets[1:]
		nodes[p.to].receive(p.m, now)
	}
	if !completed {
		t.Fatal("a write arriving after a read-only sample never completed")
	}
	for rank, s := range states {
		read := state.Command{Op: state.GET, K: 0}
		if got := read.Execute(s); string(got) != "new" {
			t.Fatalf("replica %d did not execute the unexpected write: %q", rank, got)
		}
	}
}
