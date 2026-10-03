// Package placement supplies topology input and deterministic arithmetic.
// Leader, quorum, responder, and routing policies belong to each protocol.
package placement

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/hongzicong/ConsensusArena/config"
	"github.com/hongzicong/ConsensusArena/replicaset"
)

type Topology struct {
	ConfigDir   string                  `json:"-"`
	Replicas    []config.PlannedReplica `json:"replicas"`
	Clients     []string                `json:"clients"`
	Weights     []float64               `json:"weights"`
	Objective   string                  `json:"objective"`
	Source      string                  `json:"topology_source"`
	SHA256      string                  `json:"topology_sha256,omitempty"`
	D, To, Back [][]float64             `json:"-"` // one-way milliseconds
}

// Load uses latency.conf beside the deployment file unless topology is explicit.
// Without a matrix, local deployments use a documented uniform 1ms one-way model.
func Load(c *config.Config, configPath string) (*Topology, error) {
	n := len(c.ReplicaAliases)
	if n < 3 || n%2 == 0 || n > replicaset.MaxSize {
		return nil, fmt.Errorf("planning requires odd membership of 3..63 replicas")
	}
	t := &Topology{Objective: "slow-first", ConfigDir: filepath.Dir(configPath)}
	if v := os.Getenv("CONSENSUSARENA_QUORUM_OBJECTIVE"); v != "" {
		t.Objective = v
	}
	if t.Objective != "slow-first" && t.Objective != "fast-first" {
		return nil, fmt.Errorf("unknown plan objective %q", t.Objective)
	}
	endpoints := []string{}
	for i, a := range c.ReplicaAliases {
		t.Replicas = append(t.Replicas, config.PlannedReplica{Alias: a, Endpoint: c.ReplicaAddrs[a], Rank: i})
		endpoints = append(endpoints, c.ReplicaAddrs[a])
	}
	for a := range c.ClientAddrs {
		t.Clients = append(t.Clients, a)
	}
	sort.Strings(t.Clients)
	if len(t.Clients) == 0 {
		return nil, fmt.Errorf("planning requires at least one client")
	}
	for _, a := range t.Clients {
		endpoints = append(endpoints, c.ClientAddrs[a])
	}
	weights := map[string]float64{}
	weightPath := os.Getenv("CONSENSUSARENA_CLIENT_WEIGHTS")
	if weightPath != "" {
		data, err := os.ReadFile(weightPath)
		if err != nil {
			return nil, err
		}
		if err = json.Unmarshal(data, &weights); err != nil {
			return nil, err
		}
		if len(weights) != len(t.Clients) {
			return nil, fmt.Errorf("weights must cover exactly the configured clients")
		}
	}
	total := 0.0
	for _, a := range t.Clients {
		w := 1.0
		if weightPath != "" {
			var ok bool
			w, ok = weights[a]
			if !ok {
				return nil, fmt.Errorf("missing weight for %s", a)
			}
		}
		if w < 0 || math.IsNaN(w) || math.IsInf(w, 0) {
			return nil, fmt.Errorf("invalid weight for %s", a)
		}
		t.Weights = append(t.Weights, w)
		total += w
	}
	if total <= 0 || math.IsInf(total, 0) {
		return nil, fmt.Errorf("weights need a finite positive sum")
	}
	for i := range t.Weights {
		t.Weights[i] /= total
	}
	path := c.Topology
	if path == "" {
		path = filepath.Join(filepath.Dir(configPath), "latency.conf")
		if _, err := os.Stat(path); os.IsNotExist(err) {
			path = ""
		}
	} else if !filepath.IsAbs(path) {
		path = filepath.Join(filepath.Dir(configPath), path)
	}
	c.Topology = path
	m := len(endpoints)
	d := make([][]float64, m)
	for i := range d {
		d[i] = make([]float64, m)
		for j := range d[i] {
			if i != j {
				d[i][j] = 1
			}
		}
	}
	t.Source = "uniform-local-1ms"
	if path != "" {
		data, err := os.ReadFile(path)
		if err != nil {
			return nil, err
		}
		t.Source = path
		digest := sha256.Sum256(data)
		t.SHA256 = hex.EncodeToString(digest[:])
		ids := map[string]int{}
		for i, e := range endpoints {
			if _, ok := ids[e]; ok {
				return nil, fmt.Errorf("duplicate endpoint %s", e)
			}
			ids[e] = i
		}
		seen := make([][]bool, m)
		for i := range seen {
			seen[i] = make([]bool, m)
			seen[i][i] = true
			d[i][i] = 0
		}
		s := bufio.NewScanner(strings.NewReader(string(data)))
		rows := map[string]bool{}
		for s.Scan() {
			fs := strings.Fields(s.Text())
			if len(fs) == 0 || strings.HasPrefix(fs[0], "#") || strings.HasPrefix(fs[0], "//") {
				continue
			}
			if len(fs) != 3 {
				return nil, fmt.Errorf("invalid RTT row %q", s.Text())
			}
			i, ok := ids[fs[0]]
			j, ok2 := ids[fs[1]]
			if !ok || !ok2 {
				continue
			}
			key := fs[0] + " " + fs[1]
			if rows[key] {
				return nil, fmt.Errorf("duplicate RTT row %s", key)
			}
			rows[key] = true
			v, err := time.ParseDuration(fs[2])
			if err != nil || v < 0 {
				return nil, fmt.Errorf("invalid RTT %q", fs[2])
			}
			d[i][j] = float64(v) / float64(time.Millisecond) / 2
			seen[i][j] = true
		}
		if err := s.Err(); err != nil {
			return nil, err
		}
		for i := range d {
			for j := range d {
				if (i < n || j < n) && !seen[i][j] {
					return nil, fmt.Errorf("missing RTT %s -> %s", endpoints[i], endpoints[j])
				}
			}
		}
	}
	t.D = make([][]float64, n)
	for i := range t.D {
		t.D[i] = d[i][:n]
	}
	for i := range t.Clients {
		t.To = append(t.To, d[n+i][:n])
		back := make([]float64, n)
		for j := range back {
			back[j] = d[j][n+i]
		}
		t.Back = append(t.Back, back)
	}
	return t, nil
}

func (t *Topology) Mean(values []float64) float64 {
	// Compensated summation avoids candidate ties depending on client iteration.
	sum, correction := 0.0, 0.0
	for i, v := range values {
		x := t.Weights[i] * v
		next := sum + x
		if math.Abs(sum) >= math.Abs(x) {
			correction += (sum - next) + x
		} else {
			correction += (x - next) + sum
		}
		sum = next
	}
	return sum + correction
}
func Max(v ...float64) float64 {
	result := v[0]
	for _, x := range v[1:] {
		result = math.Max(result, x)
	}
	return result
}
func Min(a, b float64) float64           { return math.Min(a, b) }
func (t *Topology) RTT(c, r int) float64 { return t.To[c][r] + t.Back[c][r] }
func (t *Topology) Nearest(c int) int {
	best := 0
	for r := 1; r < len(t.Replicas); r++ {
		if t.RTT(c, r) < t.RTT(c, best) || (t.RTT(c, r) == t.RTT(c, best) && t.Replicas[r].Alias < t.Replicas[best].Alias) {
			best = r
		}
	}
	return best
}
func (t *Topology) NearestRoutes() map[string]string {
	routes := map[string]string{}
	for c, a := range t.Clients {
		routes[a] = t.Replicas[t.Nearest(c)].Alias
	}
	return routes
}
func Kth(v []float64, k int) float64 { sort.Float64s(v); return v[k] }
func (t *Topology) Members(ids []int) []config.PlannedReplica {
	members := []config.PlannedReplica{}
	for _, i := range ids {
		members = append(members, t.Replicas[i])
	}
	sort.Slice(members, func(i, j int) bool { return members[i].Alias < members[j].Alias })
	return members
}
func (t *Topology) Key(leader int, q []int) string {
	s := ""
	if leader >= 0 {
		s = t.Replicas[leader].Alias
	}
	for _, r := range t.Members(q) {
		s += "\x00" + r.Alias
	}
	return s
}
func (t *Topology) Better(fast, slow float64, key string, best config.DeploymentPlan, bestKey string) bool {
	if bestKey == "" {
		return true
	}
	a, b, x, y := slow, fast, best.SlowMean, best.FastMean
	if t.Objective == "fast-first" {
		a, b, x, y = fast, slow, best.FastMean, best.SlowMean
	}
	return a < x || (a == x && (b < y || (b == y && key < bestKey)))
}
func Subsets(n, k int, visit func([]int)) {
	var walk func(int, []int)
	walk = func(start int, q []int) {
		if len(q) == k {
			visit(q)
			return
		}
		for i := start; i <= n-(k-len(q)); i++ {
			walk(i+1, append(q, i))
		}
	}
	walk(0, nil)
}

// Apply changes only startup inputs. The plan JSON is diagnostic output and is
// never loaded by replicas; they compute the same plan from the same raw inputs.
func Apply(c *config.Config, t *Topology, p config.DeploymentPlan) error {
	p.Protocol = strings.ToLower(c.Protocol)
	n := len(t.Replicas)
	if p.Leader != nil && (p.Leader.Rank < 0 || p.Leader.Rank >= n || t.Replicas[p.Leader.Rank] != *p.Leader) {
		return fmt.Errorf("invalid planned leader")
	}
	seen := map[int]bool{}
	for _, r := range p.FastQuorum {
		if r.Rank < 0 || r.Rank >= n || t.Replicas[r.Rank] != r || seen[r.Rank] {
			return fmt.Errorf("invalid planned quorum")
		}
		seen[r.Rank] = true
	}
	if p.ClientRoutes != nil {
		if len(p.ClientRoutes) != len(t.Clients) {
			return fmt.Errorf("plan must route every client")
		}
		local := map[string]bool{}
		for i, a := range t.Clients {
			found := false
			for j, r := range t.Replicas {
				if p.ClientRoutes[a] == r.Alias {
					found = true
					local[a] = t.RTT(i, j) == 0
					break
				}
			}
			if !found {
				return fmt.Errorf("invalid route for %s", a)
			}
		}
		c.SetRoutes(p.ClientRoutes, local)
	}
	c.Leader = nil
	if p.Leader != nil {
		addr := p.Leader.Endpoint
		if _, _, err := net.SplitHostPort(addr); err != nil {
			addr = net.JoinHostPort(addr, "7070")
		}
		c.Leader = &addr
	}
	c.Plan = p
	return nil
}
