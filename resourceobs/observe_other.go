//go:build !linux

package resourceobs

// Cluster resource measurements require Linux /proc and getrusage.
func Start(run, alias string) {}
