package swiftplacement

import (
	"github.com/hongzicong/ConsensusArena/config"
	"github.com/hongzicong/ConsensusArena/placement"
	"reflect"
	"testing"
)

func TestLargePlanMajorityAndDeterminism(t *testing.T) {
	const n = 31
	topology := &placement.Topology{Objective: "slow-first", Weights: make([]float64, n), D: make([][]float64, n), To: make([][]float64, n), Back: make([][]float64, n)}
	for i := 0; i < n; i++ {
		alias := string(rune('A' + i))
		topology.Replicas = append(topology.Replicas, config.PlannedReplica{Alias: alias, Rank: i})
		topology.Clients = append(topology.Clients, alias)
		topology.Weights[i] = 1.0 / n
		topology.D[i] = make([]float64, n)
		topology.To[i] = make([]float64, n)
		topology.Back[i] = make([]float64, n)
		for j := 0; j < n; j++ {
			d := float64((i - j) * (i - j))
			topology.D[i][j] = d
			topology.To[i][j] = d
			topology.Back[i][j] = d
		}
	}
	a, err := PlanDeployment(&config.Config{}, topology)
	if err != nil {
		t.Fatal(err)
	}
	b, err := PlanDeployment(&config.Config{}, topology)
	if err != nil || !reflect.DeepEqual(a, b) {
		t.Fatal("large placement is not deterministic", err)
	}
	if a.Leader == nil || len(a.FastQuorum) != n/2+1 || len(a.ClientRoutes) != n {
		t.Fatal("incomplete plan")
	}
	seen := map[int]bool{}
	for _, r := range a.FastQuorum {
		if seen[r.Rank] {
			t.Fatal("duplicate quorum member")
		}
		seen[r.Rank] = true
	}
	if !seen[a.Leader.Rank] {
		t.Fatal("quorum excludes leader")
	}
}
