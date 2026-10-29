// Package resourceobs measures process-local data-plane work without protocol state.
package resourceobs

import "sync/atomic"

var enabled atomic.Bool
var sentFrames, sentBytes, receivedFrames, receivedBytes atomic.Uint64

func Sent(frames, bytes int) {
	if enabled.Load() {
		sentFrames.Add(uint64(frames))
		sentBytes.Add(uint64(bytes))
	}
}

// Received counts one complete protobuf payload plus its framing and RPC tag.
func Received(bytes int) {
	if enabled.Load() {
		receivedFrames.Add(1)
		receivedBytes.Add(uint64(bytes))
	}
}
