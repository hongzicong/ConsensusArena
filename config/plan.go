package config

// DeploymentPlan is startup output, never an external quorum configuration.
// Protocol packages own its policy; the common launcher only applies it.
type DeploymentPlan struct {
	Protocol        string            `json:"protocol"`
	Model           string            `json:"model"`
	Leader          *PlannedReplica   `json:"leader"`
	FastQuorum      []PlannedReplica  `json:"fast_quorum"`
	ClientRoutes    map[string]string `json:"client_routes,omitempty"`
	Responders      []string          `json:"responders,omitempty"`
	ResponderRanges string            `json:"responder_ranges,omitempty"`
	FastMean        float64           `json:"fast_mean_ms,omitempty"`
	SlowMean        float64           `json:"slow_mean_ms,omitempty"`
}

type PlannedReplica struct {
	Alias    string `json:"alias"`
	Endpoint string `json:"endpoint"`
	Rank     int    `json:"rank"`
}

func (p DeploymentPlan) LeaderID() int32 {
	if p.Leader == nil {
		return 0
	}
	return int32(p.Leader.Rank)
}

// SetRoutes installs exactly one ingress per client. Local means zero modeled RTT.
func (c *Config) SetRoutes(routes map[string]string, local map[string]bool) {
	p := &ProxyInfo{locals: map[string]map[string]struct{}{}, proxies: map[string]map[string]struct{}{}, servers: map[string]string{}}
	for client, server := range routes {
		if p.proxies[server] == nil {
			p.proxies[server] = map[string]struct{}{}
			p.locals[server] = map[string]struct{}{}
		}
		addr := c.ClientAddrs[client]
		p.proxies[server][addr] = struct{}{}
		if local[client] {
			p.locals[server][addr] = struct{}{}
			p.servers[addr] = server
		}
	}
	c.Proxy = p
}
