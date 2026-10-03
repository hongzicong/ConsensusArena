package epaxos

import (
	"github.com/hongzicong/ConsensusArena/config"
	"github.com/hongzicong/ConsensusArena/placement"
)

// PlanDeployment gives every client its nearest replica. No global leader or
// deployment-selected voter set is imposed on EPaxos.
func PlanDeployment(_ *config.Config, t *placement.Topology) (config.DeploymentPlan, error) {
	return config.DeploymentPlan{Model: "nearest-replica-ingress", ClientRoutes: t.NearestRoutes()}, nil
}
