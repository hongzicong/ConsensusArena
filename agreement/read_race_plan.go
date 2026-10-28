package agreement

import (
	"github.com/hongzicong/ConsensusArena/config"
	"github.com/hongzicong/ConsensusArena/placement"
)

func parentColdPlan(c *config.Config, t *placement.Topology) (config.DeploymentPlan, error) {
	return exploratoryHotPlan(c, t)
}

// Both the baseline and certificate candidate use XPaxos's visibility geometry.
func baselineHotPlan(c *config.Config, t *placement.Topology) (config.DeploymentPlan, error) {
	p, err := parentPlanDeploymentLocal(c, t)
	if err != nil {
		return p, err
	}
	return planJointReadRaces(c, t, p), nil
}
