package defs

import (
	"os"
	"path/filepath"
	"testing"
)

func TestDialMapPreservesIdentityAndRejectsAmbiguity(t *testing.T) {
	previous := benchmarkDialMap
	defer func() { benchmarkDialMap = previous }()
	path := filepath.Join(t.TempDir(), "routes.txt")
	if err := os.WriteFile(path, []byte("10.0.0.1:7070 127.0.0.1:20000\n10.0.0.1:7071 127.0.0.1:20001\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := ConfigureDialMap(path); err != nil {
		t.Fatal(err)
	}
	if DialAddress("10.0.0.1:7071") != "127.0.0.1:20001" || DialAddress("10.0.0.2:7087") != "10.0.0.2:7087" {
		t.Fatal("routes collapsed distinct replicas or rewrote an unrelated control endpoint")
	}
	for _, invalid := range []string{"", "bare-host 127.0.0.1:20000\n", "10.0.0.1:7070 127.0.0.1:20000\n10.0.0.1:7070 127.0.0.1:20001\n"} {
		if err := os.WriteFile(path, []byte(invalid), 0600); err != nil {
			t.Fatal(err)
		}
		if ConfigureDialMap(path) == nil {
			t.Fatal("accepted empty, malformed, or ambiguous routing")
		}
		if DialAddress("10.0.0.1:7071") != "127.0.0.1:20001" {
			t.Fatal("failed configuration partially replaced live routes")
		}
	}
}
