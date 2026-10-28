package replica

import (
	"fmt"
	"time"

	"github.com/hongzicong/ConsensusArena/replica/defs"
)

// ReadPathMetrics observes the serialized replica loop. A local success means
// a lease-authorized read at this ingress followed by successful reply enqueue.
// It does not assert TCP delivery or client completion. IDs survive retries so
// hedges and duplicate replies at this replica do not inflate either counter.
type ReadPathMetrics struct {
	reads     map[defs.RequestID]readPath
	completed uint64
	local     uint64
}

type readPath struct {
	local, replied bool
}

func (m *ReadPathMetrics) Local(id defs.RequestID) {
	if m.reads == nil {
		m.reads = make(map[defs.RequestID]readPath)
	}
	p := m.reads[id]
	if !p.replied {
		p.local = true
		m.reads[id] = p
	}
}

// Reply must be called only for a GET whose result was successfully enqueued.
func (m *ReadPathMetrics) Reply(id defs.RequestID) {
	if m.reads == nil {
		m.reads = make(map[defs.RequestID]readPath)
	}
	p := m.reads[id]
	if p.replied {
		return
	}
	p.replied = true
	m.reads[id] = p
	m.completed++
	if p.local {
		m.local++
	}
}

func (m *ReadPathMetrics) Status(protocol string, id int) string {
	fraction := 0.0
	if m.completed > 0 {
		fraction = float64(m.local) / float64(m.completed)
	}
	return fmt.Sprintf("LOCAL_READ_STATS protocol=%s replica=%d snapshot_unix_ns=%d scope=replica_reply_enqueue_lifetime completed_reads=%d local_read_successes=%d local_read_fraction=%.6f", protocol, id, time.Now().UnixNano(), m.completed, m.local, fraction)
}
