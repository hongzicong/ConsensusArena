package resourceobs

import "testing"

func TestCountersOptInAndDirection(t *testing.T) {
	enabled.Store(false)
	sentFrames.Store(0)
	sentBytes.Store(0)
	receivedFrames.Store(0)
	receivedBytes.Store(0)
	Sent(2, 90)
	Received(40)
	if sentBytes.Load() != 0 || receivedBytes.Load() != 0 {
		t.Fatal("disabled observer counted traffic")
	}
	enabled.Store(true)
	defer enabled.Store(false)
	Sent(2, 90)
	Received(40)
	if sentFrames.Load() != 2 || sentBytes.Load() != 90 || receivedFrames.Load() != 1 || receivedBytes.Load() != 40 {
		t.Fatal("directional frame/byte counters differ")
	}
}
