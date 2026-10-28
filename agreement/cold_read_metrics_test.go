package agreement

import (
	"strings"
	"testing"
	"time"

	"github.com/hongzicong/ConsensusArena/config"
	"github.com/hongzicong/ConsensusArena/replica"
	"github.com/hongzicong/ConsensusArena/replica/defs"
	"github.com/hongzicong/ConsensusArena/state"
)

func TestColdReadMetricsIncludeImmediateAndReleasedReads(t *testing.T) {
	for _, wait := range []string{"none", "key", "authority"} {
		t.Run(wait, func(t *testing.T) {
			now := time.Unix(100, 0)
			e := &engine{id: 0, n: 3, majority: 2, prepared: true,
				current: roster{Ballot: 1, Leader: 0}, incoming: map[int]grant{1: {Until: now.Add(time.Second)}}}
			r := &Replica{Replica: &replica.Replica{State: state.InitState()}, engine: e}
			e.observeLocalRead = r.readPaths.Local
			replies := 0
			e.reply = func(req request, v state.Value) {
				if string(v) != "value" {
					t.Fatalf("unexpected read result %q", v)
				}
				replies++
				r.readPaths.Reply(req.id()) // Model a successful reply enqueue.
			}
			put := state.Command{Op: state.PUT, K: 42, V: state.Value("value")}
			put.Execute(r.State)
			c := newColdReplica(r, config.DeploymentPlan{})
			c.ready, c.q = true, bit(0)
			c.installed = coldGeneration{B: 1}
			c.promised = c.installed
			key := c.key(42)
			key.Ready = wait != "key"
			c.frozen = wait == "authority"
			req := request{Origin: 0, Proposal: defs.Propose{ClientId: 1, CommandId: 7,
				Command: state.Command{Op: state.GET, K: 42}}}
			c.read(req, now)
			if wait != "none" {
				if replies != 0 || !strings.Contains(r.readPaths.Status("agreement", 0), "local_read_successes=0") {
					t.Fatal("blocked cold read counted before execution/reply")
				}
				key.Ready, c.frozen = true, false
				c.releaseReads(now.Add(time.Millisecond))
			}
			if replies != 1 || !strings.Contains(r.readPaths.Status("agreement", 0), "completed_reads=1 local_read_successes=1") {
				t.Fatal("successful cold read missing from local reply metric")
			}
		})
	}
}
