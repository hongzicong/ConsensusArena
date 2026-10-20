// Package heatprofile handles immutable empirical deployment inputs.
package heatprofile

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
)

type KeyCount struct {
	Key    int    `json:"key"`
	Reads  uint64 `json:"reads"`
	Writes uint64 `json:"writes"`
}

// ClientCounts preserves request origin without exposing generator parameters.
// Deployment topology, rather than the sampler, maps a client to a region.
type ClientCounts struct {
	Client   string     `json:"client"`
	Requests uint64     `json:"requests"`
	Keys     []KeyCount `json:"keys"`
}

type File struct {
	Schema     int            `json:"schema"`
	Source     string         `json:"source"`
	KeyCount   int            `json:"key_count"`
	Requests   uint64         `json:"requests"`
	SampleSeed int64          `json:"sample_seed"`
	Keys       []KeyCount     `json:"keys"`
	Clients    []ClientCounts `json:"clients,omitempty"`
}

// Arrays are immutable after loading; clones in one process share them.
type Profile struct {
	File          File
	SHA256        string
	Ranks         []int32
	Probabilities []float64
}

func CanonicalKeys(keys []KeyCount) {
	sort.Slice(keys, func(i, j int) bool {
		a, b := keys[i].Reads+keys[i].Writes, keys[j].Reads+keys[j].Writes
		if a != b {
			return a > b
		}
		return keys[i].Key < keys[j].Key
	})
}

func Load(path, expected string, keyCount int) (*Profile, error) {
	return load(path, expected, keyCount, 0)
}

// LoadRegional requires a complete origin-preserving sample for this membership.
// Legacy global-only profiles remain usable by Load, but cannot infer locality.
func LoadRegional(path, expected string, keyCount int, clients []string) (*Profile, error) {
	p, err := load(path, expected, keyCount, len(clients))
	if err != nil {
		return nil, err
	}
	if p.File.Schema != 2 || len(clients) == 0 || len(p.File.Clients) != len(clients) {
		return nil, fmt.Errorf("regional heat profile schema 2 covering all clients is required; regenerate the independent sample")
	}
	names := append([]string(nil), clients...)
	sort.Strings(names)
	for i, name := range names {
		if p.File.Clients[i].Client != name {
			return nil, fmt.Errorf("regional heat profile client membership mismatch")
		}
	}
	return p, nil
}

func load(path, expected string, keyCount, clientCount int) (*Profile, error) {
	if keyCount <= 0 || int64(keyCount) > int64(1<<31-1) {
		return nil, fmt.Errorf("invalid heat profile key domain")
	}
	if path == "" || len(expected) != 64 {
		return nil, fmt.Errorf("heat profile path and 64-character SHA256 are required")
	}
	if _, err := hex.DecodeString(expected); err != nil {
		return nil, fmt.Errorf("invalid heat profile SHA256: %w", err)
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	// The schema is sparse; bound input by domain size without accepting trailing data.
	limit := int64(keyCount)*100*(1+int64(clientCount)) + 4096 + int64(clientCount)*512
	data, err := io.ReadAll(io.LimitReader(f, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > limit {
		return nil, fmt.Errorf("heat profile exceeds size bound")
	}
	digest := fmt.Sprintf("%x", sha256.Sum256(data))
	if digest != expected {
		return nil, fmt.Errorf("heat profile SHA256 mismatch: got %s", digest)
	}
	var input File
	if err = json.Unmarshal(data, &input); err != nil {
		return nil, err
	}
	if (input.Schema != 1 && input.Schema != 2) || input.KeyCount != keyCount || input.Requests == 0 || input.Source != "independent-offline-offered-requests" || input.SampleSeed < 0 {
		return nil, fmt.Errorf("invalid heat profile schema, domain, provenance or sample count")
	}
	if err := validateCounts(input.Keys, input.Requests, keyCount); err != nil {
		return nil, err
	}
	if input.Schema == 1 && len(input.Clients) != 0 {
		return nil, fmt.Errorf("schema 1 cannot contain regional counts")
	}
	if input.Schema == 2 {
		if err := validateClients(input); err != nil {
			return nil, err
		}
	}
	p := &Profile{File: input, SHA256: digest, Ranks: make([]int32, keyCount), Probabilities: make([]float64, keyCount)}
	for key := range p.Ranks {
		p.Ranks[key] = -1
	}
	for rank, row := range input.Keys {
		count := row.Reads + row.Writes
		p.Ranks[row.Key] = int32(rank)
		p.Probabilities[rank] = float64(count) / float64(input.Requests)
	}
	return p, nil
}

func validateCounts(keys []KeyCount, requests uint64, keyCount int) error {
	seen := make(map[int]bool, len(keys))
	var total uint64
	for rank, row := range keys {
		if row.Key < 0 || row.Key >= keyCount || seen[row.Key] || row.Reads > requests || row.Writes > requests-row.Reads {
			return fmt.Errorf("invalid or duplicate sampled key at rank %d", rank)
		}
		seen[row.Key] = true
		count := row.Reads + row.Writes
		if count == 0 || count > requests-total {
			return fmt.Errorf("invalid sample count at rank %d", rank)
		}
		if rank > 0 {
			previous := keys[rank-1]
			old := previous.Reads + previous.Writes
			if count > old || (count == old && row.Key <= previous.Key) {
				return fmt.Errorf("non-canonical sample rank %d", rank)
			}
		}
		total += count
	}
	if total != requests {
		return fmt.Errorf("heat sample total mismatch: %d != %d", total, requests)
	}
	return nil
}

func validateClients(input File) error {
	if len(input.Clients) == 0 {
		return fmt.Errorf("regional heat profile is missing client observations")
	}
	aggregate := make(map[int]KeyCount, len(input.Keys))
	for _, row := range input.Keys {
		aggregate[row.Key] = row
	}
	var requests uint64
	for i, client := range input.Clients {
		if strings.TrimSpace(client.Client) == "" || (i > 0 && client.Client <= input.Clients[i-1].Client) || client.Requests > input.Requests-requests {
			return fmt.Errorf("invalid, duplicate or non-canonical sampled client")
		}
		if err := validateCounts(client.Keys, client.Requests, input.KeyCount); err != nil {
			return fmt.Errorf("sampled client %q: %w", client.Client, err)
		}
		requests += client.Requests
		for _, row := range client.Keys {
			remaining, ok := aggregate[row.Key]
			if !ok || row.Reads > remaining.Reads || row.Writes > remaining.Writes {
				return fmt.Errorf("regional/global counts disagree for key %d", row.Key)
			}
			remaining.Reads -= row.Reads
			remaining.Writes -= row.Writes
			aggregate[row.Key] = remaining
		}
	}
	if requests != input.Requests {
		return fmt.Errorf("regional/global request counts disagree")
	}
	for key, row := range aggregate {
		if row.Reads != 0 || row.Writes != 0 {
			return fmt.Errorf("regional/global counts disagree for key %d", key)
		}
	}
	return nil
}

func Write(path string, input File) (string, error) {
	data, err := json.Marshal(input)
	if err != nil {
		return "", err
	}
	data = append(data, '\n')
	if err = os.WriteFile(path, data, 0644); err != nil {
		return "", err
	}
	return fmt.Sprintf("%x", sha256.Sum256(data)), nil
}
