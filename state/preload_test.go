package state

import "testing"

func TestPreloadDeterminismAndEmptyState(t *testing.T) {
	a, b, c := InitState(), InitState(), InitState()
	first, err := a.Preload(100, 1000, 1)
	if err != nil {
		t.Fatal(err)
	}
	second, _ := b.Preload(100, 1000, 1)
	third, _ := c.Preload(100, 1000, 2)
	if first != second || first == third || a.Store.Size() != 100 {
		t.Fatal("invalid preload identity")
	}
	command := Command{Op: GET, K: 50}
	if len(command.Execute(a)) != 1000 {
		t.Fatal("preloaded READ did not return a full value")
	}
	if _, err := a.Preload(100, 1000, 1); err == nil {
		t.Fatal("nonempty state accepted preload")
	}
}
