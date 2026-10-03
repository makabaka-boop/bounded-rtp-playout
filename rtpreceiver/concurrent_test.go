package rtpreceiver

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestConcurrentSourcesAndTicks exercises HandlePacket from multiple
// goroutines against the real wall clock's timer callbacks (race detector).
func TestConcurrentSourcesAndTicks(t *testing.T) {
	const sources = 4
	const packets = 12
	var emitted int64

	rec := NewRecorder()
	rcv := NewReceiver(Config{
		Clock:  WallClock(),
		Sink:   func(Frame) { atomic.AddInt64(&emitted, 1) },
		Window: 64,
	}, rec)

	var wg sync.WaitGroup
	for s := 0; s < sources; s++ {
		wg.Add(1)
		go func(ssrc uint32) {
			defer wg.Done()
			for i := 0; i < packets; i++ {
				b := buildPacket(t, ssrc, uint16(i), uint32(i)*SamplesPerPacket, int16(i))
				if err := rcv.HandlePacket(b); err != nil {
					t.Errorf("handle: %v", err)
					return
				}
			}
		}(uint32(0x1000 + s))
	}
	wg.Wait()

	// First frame is at 60 ms; 300 ms later every real packet has played.
	time.Sleep(320 * time.Millisecond)
	rcv.Close()

	if got := atomic.LoadInt64(&emitted); got < int64(sources*packets) {
		t.Fatalf("emitted=%d want at least %d", got, sources*packets)
	}
	stats := rcv.StatsSnapshot()
	if len(stats) != sources {
		t.Fatalf("stats generations=%d want %d", len(stats), sources)
	}
}
