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
)

type KeyCount struct {
	Key    int    `json:"key"`
	Reads  uint64 `json:"reads"`
	Writes uint64 `json:"writes"`
}

type File struct {
	Schema     int        `json:"schema"`
	Source     string     `json:"source"`
	KeyCount   int        `json:"key_count"`
	Requests   uint64     `json:"requests"`
	SampleSeed int64      `json:"sample_seed"`
	Keys       []KeyCount `json:"keys"`
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
	limit := int64(keyCount)*100 + 4096
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
	if input.Schema != 1 || input.KeyCount != keyCount || keyCount <= 0 || int64(keyCount) > int64(1<<31-1) || input.Requests == 0 || input.Source != "independent-offline-offered-requests" {
		return nil, fmt.Errorf("invalid heat profile schema, domain, provenance or sample count")
	}
	p := &Profile{File: input, SHA256: digest, Ranks: make([]int32, keyCount), Probabilities: make([]float64, keyCount)}
	for key := range p.Ranks {
		p.Ranks[key] = -1
	}
	var total uint64
	for rank, row := range input.Keys {
		if row.Key < 0 || row.Key >= keyCount || p.Ranks[row.Key] >= 0 || row.Reads > input.Requests || row.Writes > input.Requests-row.Reads {
			return nil, fmt.Errorf("invalid or duplicate sampled key at rank %d", rank)
		}
		count := row.Reads + row.Writes
		if count == 0 || count > input.Requests-total {
			return nil, fmt.Errorf("invalid sample count at rank %d", rank)
		}
		if rank > 0 {
			previous := input.Keys[rank-1]
			old := previous.Reads + previous.Writes
			if count > old || (count == old && row.Key <= previous.Key) {
				return nil, fmt.Errorf("non-canonical sample rank %d", rank)
			}
		}
		total += count
		p.Ranks[row.Key] = int32(rank)
		p.Probabilities[rank] = float64(count) / float64(input.Requests)
	}
	if total != input.Requests {
		return nil, fmt.Errorf("heat sample total mismatch: %d != %d", total, input.Requests)
	}
	return p, nil
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
