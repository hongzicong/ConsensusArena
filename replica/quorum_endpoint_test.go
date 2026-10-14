package replica

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/imdea-software/swiftpaxos/config"
)

func TestConfiguredQuorumDistinguishesColocatedReplicas(t *testing.T) {
	r := &Replica{N: 5, Config: &config.Config{ReplicaAddrs: map[string]string{
		"a": "10.0.0.1:7070", "b": "10.0.0.1:7071", "c": "10.0.0.2:7072",
	}}, PeerAddrList: []string{"10.0.0.1:7071", "10.0.0.2:7072", "10.0.0.1:7070", "10.0.0.3:7073", "10.0.0.3:7074"}}
	path := filepath.Join(t.TempDir(), "quorum.conf")
	if err := os.WriteFile(path, []byte("l a\nb\nc\n"), 0600); err != nil {
		t.Fatal(err)
	}
	quorums, leaders, err := NewQuorumsFromFile(path, r)
	if err != nil || len(quorums) != 1 || quorums[0].Size() != 3 || leaders[0] != 2 {
		t.Fatalf("quorum/leader collapsed by host or registration order: %v %v %v", quorums, leaders, err)
	}
	for _, id := range []int32{0, 1, 2} {
		if !quorums[0].Contains(id) {
			t.Fatalf("missing intended voter %d", id)
		}
	}
}

func TestLegacyHostQuorumRemainsSupported(t *testing.T) {
	r := &Replica{N: 3, Config: &config.Config{ReplicaAddrs: map[string]string{
		"a": "10.0.0.1", "b": "10.0.0.2",
	}}, PeerAddrList: []string{"10.0.0.2:7070", "10.0.0.1:7070", "10.0.0.3:7070"}}
	path := filepath.Join(t.TempDir(), "quorum.conf")
	if err := os.WriteFile(path, []byte("l a\nb\n"), 0600); err != nil {
		t.Fatal(err)
	}
	quorums, leaders, err := NewQuorumsFromFile(path, r)
	if err != nil || quorums[0].Size() != 2 || leaders[0] != 1 {
		t.Fatalf("legacy bare-host configuration changed: %v %v %v", quorums, leaders, err)
	}
}
