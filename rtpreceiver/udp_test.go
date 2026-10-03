package rtpreceiver

import (
	"net"
	"testing"
	"time"
)

// TestUDPEndToEnd drives ServeUDP over a loopback socket and verifies the
// received audio is emitted and recorded.
func TestUDPEndToEnd(t *testing.T) {
	conn, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 0})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	rec := NewRecorder()
	rcv := NewReceiver(Config{
		Clock:  WallClock(),
		Window: 64,
	}, rec)

	serveDone := make(chan error, 1)
	go func() { serveDone <- rcv.ServeUDP(conn) }()

	client, err := net.Dial("udp4", conn.LocalAddr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	const ssrc = uint32(0xabcdef01)
	for i := 0; i < 6; i++ {
		pkt := buildPacket(t, ssrc, uint16(i), uint32(i)*SamplesPerPacket, int16(500+i))
		if _, err := client.Write(pkt); err != nil {
			t.Fatal(err)
		}
	}

	// First frame at +60 ms; six frames finish at +160 ms.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if len(rec.Frames(1)) == 6 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	frames := rec.Frames(1)
	if len(frames) != 6 {
		t.Fatalf("emitted frames=%d want 6", len(frames))
	}
	for i, f := range frames {
		if f.Missing || int(f.Samples[0]) != 500+i {
			t.Fatalf("frame %d wrong: missing=%v sample=%d", i, f.Missing, f.Samples[0])
		}
	}

	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}
	if err := <-serveDone; err == nil {
		t.Fatal("ServeUDP should return a read error after close")
	}
}
