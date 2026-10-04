package config

import (
	"fmt"
	"net"
	"sort"
	"strconv"
)

// Membership snapshots configuration order and advertised identities. Build it
// after address mapping; it never sorts voter IDs or selects protocol routes.
// Endpoint duplication among clients is allowed (co-located clients/clones).
type Membership struct {
	Replicas        []PlannedReplica
	Clients         []string
	replicaIDs      map[string]int
	clientAddrs     map[string]string
	clientEndpoints map[string]struct{}
	port            int
}

func (c *Config) Membership() (*Membership, error) {
	if len(c.ReplicaAliases) != len(c.ReplicaAddrs) {
		return nil, fmt.Errorf("replica aliases and addresses differ in count")
	}
	m := &Membership{replicaIDs: make(map[string]int), clientAddrs: make(map[string]string), clientEndpoints: make(map[string]struct{}), port: c.Port}
	for id, alias := range c.ReplicaAliases {
		endpoint, ok := c.ReplicaAddrs[alias]
		if alias == "" || !ok || endpoint == "" {
			return nil, fmt.Errorf("replica %q has no configured endpoint", alias)
		}
		if _, exists := m.replicaIDs[alias]; exists {
			return nil, fmt.Errorf("duplicate replica alias %q", alias)
		}
		m.replicaIDs[alias] = id
		m.Replicas = append(m.Replicas, PlannedReplica{Alias: alias, Endpoint: endpoint, Rank: id})
	}
	for alias, endpoint := range c.ClientAddrs {
		if alias == "" || endpoint == "" {
			return nil, fmt.Errorf("client %q has no configured endpoint", alias)
		}
		m.Clients = append(m.Clients, alias)
		m.clientAddrs[alias] = endpoint
		m.clientEndpoints[endpoint] = struct{}{}
	}
	sort.Strings(m.Clients)
	return m, nil
}

func (m *Membership) ReplicaID(alias string) (int, bool) {
	id, ok := m.replicaIDs[alias]
	return id, ok
}
func (m *Membership) ClientEndpoint(alias string) string { return m.clientAddrs[alias] }
func (m *Membership) HasClientEndpoint(endpoint string) bool {
	_, ok := m.clientEndpoints[endpoint]
	return ok
}

func (m *Membership) CheckReplica(alias string, id, count int) error {
	expected, ok := m.ReplicaID(alias)
	if !ok || id != expected || count != len(m.Replicas) {
		return fmt.Errorf("replica %q ID %d/membership %d differs from configured order", alias, id, count)
	}
	return nil
}

func (m *Membership) RegistrationID(alias, endpoint string) (int, error) {
	id, ok := m.ReplicaID(alias)
	if !ok {
		return 0, fmt.Errorf("unknown replica alias %q", alias)
	}
	expected := m.Replicas[id].Endpoint
	if _, _, err := net.SplitHostPort(expected); err != nil {
		expected = net.JoinHostPort(expected, strconv.Itoa(m.port))
	}
	if endpoint != expected {
		return 0, fmt.Errorf("replica %q registered endpoint %q, expected %q", alias, endpoint, expected)
	}
	return id, nil
}
