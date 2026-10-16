package bodega

import (
	"encoding/json"
	"fmt"
	"math"
	"os"
	"strings"

	"github.com/hongzicong/ConsensusArena/config"
	"github.com/hongzicong/ConsensusArena/heatprofile"
	"github.com/hongzicong/ConsensusArena/placement"
)

// PlanDeployment retains the uniform/non-uniform author-policy placements.
// The branch is inferred from a frozen sample, never generator parameters.
func PlanDeployment(c *config.Config, t *placement.Topology) (config.DeploymentPlan, error) {
	skewed, err := sampledSkew(c)
	if err != nil {
		return config.DeploymentPlan{}, err
	}
	leader := t.Replicas[0]
	p := config.DeploymentPlan{Model: "author-policy-shared-key-space", Leader: &leader, ClientRoutes: t.NearestRoutes()}
	if !skewed {
		for _, r := range t.Replicas {
			p.Responders = append(p.Responders, r.Alias)
		}
	} else {
		p.Responders = []string{leader.Alias}
		set := map[string]bool{leader.Alias: true}
		for _, r := range p.ClientRoutes {
			set[r] = true
		}
		names := []string{}
		for _, r := range t.Replicas {
			if set[r.Alias] {
				names = append(names, r.Alias)
			}
		}
		p.ResponderRanges = fmt.Sprintf("0..%d=%s", c.KeyCount-1, strings.Join(names, ","))
	}
	if c.BodegaResponderRanges != "" && c.BodegaResponderRanges != p.ResponderRanges {
		return p, fmt.Errorf("explicit Bodega responder ranges conflict with shared-key author policy")
	}
	c.BodegaResponders = strings.Join(p.Responders, ",")
	c.BodegaResponderRanges = p.ResponderRanges
	return p, nil
}

// The one-sided Cantelli bound limits false rejection of IID uniform key
// observations to this probability; non-rejection does not prove uniformity.
const uniformFalseRejection = .001

func sampledSkew(c *config.Config) (bool, error) {
	p, err := heatprofile.Load(c.HeatProfilePath, c.HeatProfileSHA256, c.KeyCount)
	if err != nil {
		return false, fmt.Errorf("Bodega empirical profile: %w", err)
	}
	// Count equal-key pairs. Under IID uniform keys, pair indicators are
	// pairwise independent, including overlapping pairs, so the moments below
	// are exact. No Zipf parameter, key-rank mapping or write ratio is needed.
	n, keys := float64(p.File.Requests), float64(c.KeyCount)
	pairs := n * (n - 1) / 2
	expected := pairs / keys
	variance := expected * (1 - 1/keys)
	threshold := expected + math.Sqrt(variance*(1-uniformFalseRejection)/uniformFalseRejection)
	observed := 0.0
	for _, row := range p.File.Keys {
		count := float64(row.Reads + row.Writes)
		observed += count * (count - 1) / 2
	}
	skewed := observed > threshold
	branch := "uniform-compatible"
	if skewed {
		branch = "non-uniform"
	}
	record := map[string]interface{}{
		"policy": "author-policy-sampled-uniformity", "branch": branch,
		"profile_sha256": p.SHA256, "sample_source": p.File.Source,
		"sample_requests": p.File.Requests, "sample_seed": p.File.SampleSeed,
		"observed_keys": len(p.File.Keys), "known_key_count": c.KeyCount,
		"equal_key_pairs": observed, "uniform_expected_pairs": expected,
		"uniform_variance": variance, "non_uniform_threshold": threshold,
		"uniform_false_rejection_bound": uniformFalseRejection,
		"assumption":                    "independent identically distributed sampled keys",
		"frozen":                        true,
	}
	data, err := json.Marshal(record)
	if err != nil {
		return false, err
	}
	fmt.Fprintln(os.Stderr, "BODEGA_SAMPLE_POLICY", string(data))
	return skewed, nil
}
