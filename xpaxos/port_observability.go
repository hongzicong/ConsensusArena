package xpaxos

import (
	"encoding/json"
	"fmt"
	"github.com/hongzicong/ConsensusArena/config"
	"github.com/hongzicong/ConsensusArena/heatprofile"
	"github.com/hongzicong/ConsensusArena/placement"
	"github.com/hongzicong/ConsensusArena/xpaxos/heatpolicy"
	"os"
	"time"
)

func PlanDeployment(c *config.Config, t *placement.Topology) (config.DeploymentPlan, error) {
	start := time.Now()
	profile, err := heatprofile.Load(c.HeatProfilePath, c.HeatProfileSHA256, c.KeyCount)
	if err != nil {
		return config.DeploymentPlan{}, fmt.Errorf("XPaxos empirical profile: %w", err)
	}
	if c.UniqueKeys {
		return config.DeploymentPlan{}, fmt.Errorf("XPaxos sampling requires reusable keys")
	}
	c.HeatProfile = profile
	p, err := importedPlanDeployment(c, t)
	if err != nil {
		return p, err
	}
	var coldMass, collision, topCold float64
	for rank, probability := range profile.Probabilities {
		if p.ColdEnabled && int64(rank) >= p.ColdCutoff {
			coldMass += probability
			collision += probability * probability
			if rank < heatpolicy.HotCutoff(c) {
				topCold += probability
			}
		}
	}
	row := map[string]interface{}{
		"visibility_model": "owner-first-delayed-accepted",
		"protocol":         "xpaxos", "hot_cutoff": heatpolicy.HotCutoff(c), "hot_rule": "frequency above mean observed key", "policy": "frozen offline sample ranks; unknown keys ordinary",
		"profile_sha256": profile.SHA256, "sample_requests": profile.File.Requests,
		"observed_keys": len(profile.File.Keys), "unobserved_keys": c.KeyCount - len(profile.File.Keys),
		"cold_enabled": p.ColdEnabled, "cold_cutoff": p.ColdCutoff,
		"sample_cold_mass": coldMass, "sample_cold_collision_mass": collision, "sample_hot_mass_in_cold_cohort": topCold,
		"client_learning_all_keys": p.ClientLearningAllKeys, "mapping_table_bytes": len(profile.Ranks) * 4,
		"probability_table_bytes": len(profile.Probabilities) * 8, "diagnostic_startup_ns": time.Since(start).Nanoseconds(),
	}
	data, _ := json.Marshal(row)
	fmt.Fprintln(os.Stderr, "XPAXOS_SAMPLE_INPUTS", string(data))
	return p, nil
}
