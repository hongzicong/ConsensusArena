package replica

import (
	"strings"
	"testing"

	"github.com/hongzicong/ConsensusArena/replica/defs"
)

func TestReadPathMetricsCountSuccessfulReplyEnqueues(t *testing.T) {
	var m ReadPathMetrics
	local := defs.RequestID{Client: 1, Sequence: 1}
	remote := defs.RequestID{Client: 1, Sequence: 2}
	blocked := defs.RequestID{Client: 2, Sequence: 1}
	m.Local(local)
	m.Local(local)   // A retry observes the same local execution.
	m.Local(blocked) // No successful enqueue: this is not a success.
	m.Reply(local)
	m.Reply(local) // Duplicate response enqueue.
	m.Reply(remote)
	m.Local(remote) // Late local work cannot reclassify an earlier remote reply.
	m.Reply(remote)
	if m.completed != 2 || m.local != 1 {
		t.Fatalf("got completed=%d local=%d, want 2 and 1", m.completed, m.local)
	}
	if !strings.Contains(m.Status("test", 0), "local_read_fraction=0.500000") {
		t.Fatal("missing local success fraction")
	}
	m.Reply(blocked) // A later successful enqueue now counts exactly once.
	if m.completed != 3 || m.local != 2 {
		t.Fatalf("got completed=%d local=%d after retry", m.completed, m.local)
	}
}

func TestReadPathMetricsNoReadReplies(t *testing.T) {
	var m ReadPathMetrics
	if !strings.Contains(m.Status("test", 1), "completed_reads=0 local_read_successes=0 local_read_fraction=0.000000") {
		t.Fatal("invalid empty snapshot")
	}
}
