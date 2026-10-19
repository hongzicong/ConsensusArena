package config

// DeploymentPlan is startup output, never an external quorum configuration.
// Protocol packages own its policy; the common launcher only applies it.
type DeploymentPlan struct {
	ClientLearningAllKeys        bool                              `json:"client_learning_all_keys,omitempty"`
	ColdEnabled                  bool                              `json:"cold_enabled,omitempty"`
	ColdCutoff                   int64                             `json:"cold_cutoff,omitempty"`
	ColdQuorum                   uint64                            `json:"cold_quorum,omitempty"`
	ReadRaceRanges               map[string][]PlannedReadRaceRange `json:"read_race_ranges,omitempty"`
	PlacementParentMean          float64                           `json:"placement_parent_mean_ms,omitempty"`
	PlacementSelectedMean        float64                           `json:"placement_selected_mean_ms,omitempty"`
	PlacementParentP99           float64                           `json:"placement_parent_p99_ms,omitempty"`
	PlacementSelectedP99         float64                           `json:"placement_selected_p99_ms,omitempty"`
	PlacementShortlist           int                               `json:"placement_shortlist,omitempty"`
	PlacementChanged             bool                              `json:"placement_changed,omitempty"`
	Protocol                     string                            `json:"protocol"`
	Model                        string                            `json:"model"`
	Leader                       *PlannedReplica                   `json:"leader"`
	FastQuorum                   []PlannedReplica                  `json:"fast_quorum"`
	FastQuorumSize               int                               `json:"fast_quorum_size,omitempty"`
	ClientRoutes                 map[string]string                 `json:"client_routes,omitempty"`
	WriteClientRoutes            map[string]string                 `json:"write_client_routes,omitempty"`
	CompletionRanges             []PlannedCompletionRange          `json:"completion_ranges,omitempty"`
	CompletionModelMeanSaving    float64                           `json:"completion_model_mean_saving_ms,omitempty"`
	CompletionModelP99Delta      float64                           `json:"completion_model_p99_delta_ms,omitempty"`
	CompletionModelTailBandDelta float64                           `json:"completion_model_tail_band_delta_ms,omitempty"`
	CompletionTailRejected       bool                              `json:"completion_tail_rejected,omitempty"`
	Responders                   []string                          `json:"responders,omitempty"`
	ResponderRanges              string                            `json:"responder_ranges,omitempty"`
	FastMean                     float64                           `json:"fast_mean_ms,omitempty"`
	SlowMean                     float64                           `json:"slow_mean_ms,omitempty"`
}

type PlannedCompletionRange struct {
	Start, End int64
	Owner      PlannedReplica
}

type PlannedReplica struct {
	Alias    string `json:"alias"`
	Endpoint string `json:"endpoint"`
	Rank     int    `json:"rank"`
}

type PlannedReadRaceRange struct {
	Start, End int64
	Target     int
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
