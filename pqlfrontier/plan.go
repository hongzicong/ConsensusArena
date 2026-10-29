package pqlfrontier

import (
	"fmt"
	"sort"

	"github.com/hongzicong/ConsensusArena/config"
	"github.com/hongzicong/ConsensusArena/heatprofile"
	"github.com/hongzicong/ConsensusArena/paxos"
	"github.com/hongzicong/ConsensusArena/placement"
)

// PlanDeployment selects the same stable leader as the Paxos baseline. The
// default majority is that leader's lowest-RTT majority; sampled object leases
// replace it with a majority-sized holder set that always includes the leader.
func PlanDeployment(c *config.Config, t *placement.Topology) (config.DeploymentPlan, error) {
	base, err := paxos.PlanDeployment(c, t)
	if err != nil {
		return config.DeploymentPlan{}, fmt.Errorf("PQL Paxos leader placement: %w", err)
	}
	leader := base.Leader.Rank
	peers := make([]int, 0, len(t.Replicas)-1)
	for id := range t.Replicas {
		if id != leader {
			peers = append(peers, id)
		}
	}
	sort.Slice(peers, func(i, j int) bool {
		a := t.D[leader][peers[i]] + t.D[peers[i]][leader]
		b := t.D[leader][peers[j]] + t.D[peers[j]][leader]
		if a != b {
			return a < b
		}
		return peers[i] < peers[j]
	})
	base.Model = "pql-frontier-sampled-majority-holders"
	base.ClientRoutes = t.NearestRoutes()
	base.FastQuorum = []config.PlannedReplica{*base.Leader}
	for _, id := range peers[:len(t.Replicas)/2] {
		base.FastQuorum = append(base.FastQuorum, t.Replicas[id])
	}
	base.FastQuorumSize = len(base.FastQuorum)
	_, err = heatprofile.LoadRegional(c.HeatProfilePath, c.HeatProfileSHA256, c.KeyCount, t.Clients)
	if err != nil {
		return config.DeploymentPlan{}, fmt.Errorf("PQL regional empirical profile: %w", err)
	}
	return base, nil
}
