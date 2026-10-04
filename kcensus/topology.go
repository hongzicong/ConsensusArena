package kcensus

// Process identities, deployment topology, and latency input parsing.
import (
	"bufio"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/hongzicong/ConsensusArena/config"
)

const clientPortBase = 7170

type processTopology struct {
	Plan                           Plan
	Aliases, Identities, Listeners []string
	Latency                        [][]time.Duration
}

// Replicas retain configuration order; non-voters use sorted alias/clone order.
// Every process builds this same catalog before opening protocol streams.
func configuredTopology(conf *config.Config) (processTopology, error) {
	n := len(conf.ReplicaAliases)
	membership, err := conf.Membership()
	if err != nil {
		return processTopology{}, err
	}
	clients := membership.Clients
	baseAliases := append([]string(nil), conf.ReplicaAliases...)
	baseEndpoints := make([]string, n)
	for i, r := range membership.Replicas {
		baseEndpoints[i] = r.Endpoint
	}
	for _, a := range clients {
		baseAliases = append(baseAliases, a)
		baseEndpoints = append(baseEndpoints, membership.ClientEndpoint(a))
	}
	m := n + len(clients)*(conf.Clones+1)
	if m > 63 || conf.Clones < 0 {
		return processTopology{}, fmt.Errorf("KCensus supports at most 63 configured processes, got %d", m)
	}
	latency := make([][]time.Duration, len(baseAliases))
	for i := range latency {
		latency[i] = make([]time.Duration, len(latency))
		for j := range latency {
			if i != j {
				latency[i][j] = time.Millisecond
			}
		}
	}
	if conf.KCensusTopology != "" {
		f, err := os.Open(conf.KCensusTopology)
		if err != nil {
			return processTopology{}, err
		}
		latency, err = ReadLatency(f, baseAliases, baseEndpoints)
		f.Close()
		if err != nil {
			return processTopology{}, err
		}
	}
	t := processTopology{Aliases: append([]string(nil), conf.ReplicaAliases...), Identities: append([]string(nil), baseEndpoints[:n]...), Listeners: make([]string, n)}
	location := make([]int, n)
	for i := range location {
		location[i] = i
	}
	for j, a := range clients {
		host := membership.ClientEndpoint(a)
		if h, _, err := net.SplitHostPort(host); err == nil {
			host = h
		}
		for clone := 0; clone <= conf.Clones; clone++ {
			ordinal := j*(conf.Clones+1) + clone
			t.Aliases = append(t.Aliases, fmt.Sprintf("%s#%d", a, clone))
			t.Identities = append(t.Identities, membership.ClientEndpoint(a))
			t.Listeners = append(t.Listeners, net.JoinHostPort(host, strconv.Itoa(clientPortBase+ordinal)))
			location = append(location, n+j)
		}
	}
	expanded := make([][]time.Duration, m)
	for i := range expanded {
		expanded[i] = make([]time.Duration, m)
		for j := range expanded {
			expanded[i][j] = latency[location[i]][location[j]]
		}
	}
	t.Latency = expanded
	t.Plan, err = SynthesizeProcesses(expanded, n, nil)
	if err == nil {
		h := sha256.New()
		h.Write(t.Plan.Digest[:])
		var length [8]byte
		for i, a := range t.Aliases {
			for _, identity := range []string{a, t.Identities[i], t.Listeners[i]} {
				binary.LittleEndian.PutUint64(length[:], uint64(len(identity)))
				h.Write(length[:])
				h.Write([]byte(identity))
			}
		}
		copy(t.Plan.Digest[:], h.Sum(nil))
	}
	return t, err
}

// A failed execution delegate can be replaced by any voter. Estimate the
// unchanged ready-read majority's round trip plus the client's return path.
func survivingReadDelegate(latency [][]time.Duration, requester, n, f int, alive []bool) int {
	if requester >= len(latency) || len(alive) != n {
		return -1
	}
	best := -1
	var bestCost time.Duration
	for delegate := 0; delegate < n; delegate++ {
		if !alive[delegate] {
			continue
		}
		var roundTrips []time.Duration
		for voter := 0; voter < n; voter++ {
			if alive[voter] {
				roundTrips = append(roundTrips, latency[delegate][voter]+latency[voter][delegate])
			}
		}
		if len(roundTrips) < n-f {
			continue
		}
		sort.Slice(roundTrips, func(i, j int) bool { return roundTrips[i] < roundTrips[j] })
		cost := latency[requester][delegate] + latency[delegate][requester] + roundTrips[n-f-1]
		if best < 0 || cost < bestCost {
			best, bestCost = delegate, cost
		}
	}
	return best
}

// ReadLatency accepts Arena's "from to RTT" matrix. Alias, advertised endpoint,
// and bare host (only when unambiguous) identify replicas, never registration order.
func ReadLatency(in io.Reader, aliases, endpoints []string) ([][]time.Duration, error) {
	n := len(aliases)
	if len(endpoints) != n {
		return nil, fmt.Errorf("latency identity count differs")
	}
	names := map[string]int{}
	add := func(name string, id int) {
		if old, exists := names[name]; exists && old != id {
			names[name] = -1
		} else if !exists {
			names[name] = id
		}
	}
	host := func(e string) string {
		if h, _, err := net.SplitHostPort(e); err == nil {
			return h
		}
		return e
	}
	for i, a := range aliases {
		add(a, i)
		add(endpoints[i], i)
	}
	for i, e := range endpoints {
		h := host(e)
		unique := true
		for j, other := range endpoints {
			if i != j && host(other) == h {
				unique = false
				break
			}
		}
		if unique {
			add(h, i)
		}
	}
	d := make([][]time.Duration, n)
	seen := make([][]bool, n)
	for i := range d {
		d[i] = make([]time.Duration, n)
		seen[i] = make([]bool, n)
		seen[i][i] = true
	}
	s := bufio.NewScanner(in)
	for s.Scan() {
		fs := strings.Fields(s.Text())
		if len(fs) == 0 || strings.HasPrefix(fs[0], "#") || strings.HasPrefix(fs[0], "//") {
			continue
		}
		if len(fs) < 3 {
			return nil, fmt.Errorf("invalid RTT row")
		}
		i, ok := names[fs[0]]
		j, ok2 := names[fs[1]]
		if !ok || !ok2 {
			continue
		}
		if i < 0 || j < 0 {
			return nil, fmt.Errorf("ambiguous RTT identity %s -> %s; use full endpoints", fs[0], fs[1])
		}
		v, err := time.ParseDuration(fs[2])
		if err != nil || v < 0 {
			return nil, fmt.Errorf("invalid RTT %q", fs[2])
		}
		d[i][j] = v / 2
		seen[i][j] = true
	}
	if err := s.Err(); err != nil {
		return nil, err
	}
	for i := range d {
		for j := range d {
			if !seen[i][j] {
				return nil, fmt.Errorf("missing RTT %s -> %s", aliases[i], aliases[j])
			}
		}
	}
	return d, nil
}
