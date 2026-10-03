package bodega

import (
	"fmt"
	"github.com/hongzicong/ConsensusArena/config"
	"github.com/hongzicong/ConsensusArena/placement"
	"strings"
)

// PlanDeployment preserves the author-policy placement adapted to a shared key
// space: replica zero is leader; the shared Zipf range covers all reader regions.
// Neither workload write ratio nor a latency objective selects responders.
func PlanDeployment(c *config.Config, t *placement.Topology) (config.DeploymentPlan, error) {
	leader := t.Replicas[0]
	p := config.DeploymentPlan{Model: "author-policy-shared-key-space", Leader: &leader, ClientRoutes: t.NearestRoutes()}
	if c.ZipfSkew == 0 {
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
