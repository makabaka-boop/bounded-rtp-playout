package rtpreceiver

import (
	"encoding/binary"
	"testing"
)

func buildPacket(t *testing.T, ssrc uint32, seq uint16, ts uint32, sample int16) []byte {
	t.Helper()
	b := make([]byte, FixedHeaderLen+PayloadLen)
	b[0] = RTPVersion << 6
	b[1] = PayloadType
	binary.BigEndian.PutUint16(b[2:4], seq)
	binary.BigEndian.PutUint32(b[4:8], ts)
	binary.BigEndian.PutUint32(b[8:12], ssrc)
	for i := 0; i < SamplesPerPacket; i++ {
		binary.BigEndian.PutUint16(b[FixedHeaderLen+2*i:], uint16(sample))
	}
	return b
}

func TestParsePacketValid(t *testing.T) {
	b := buildPacket(t, 0xdeadbeef, 1234, 5678, -1234)
	p, err := ParsePacket(b)
	if err != nil {
		t.Fatalf("ParsePacket: %v", err)
	}
	if p.SSRC != 0xdeadbeef || p.Seq != 1234 || p.Timestamp != 5678 {
		t.Fatalf("header mismatch: %+v", p)
	}
	if p.Samples[0] != -1234 || p.Samples[SamplesPerPacket-1] != -1234 {
		t.Fatalf("payload mismatch: %d", p.Samples[0])
	}
	// The packet must own a copy: mutating the input must not change it.
	b[FixedHeaderLen] = 0
	b[FixedHeaderLen+1] = 0
	if p.Samples[0] != -1234 {
		t.Fatalf("ParsePacket aliased input buffer")
	}
}

func TestParsePacketRejections(t *testing.T) {
	good := buildPacket(t, 1, 0, 0, 1)
	cases := []struct {
		name string
		edit func(b []byte) []byte
		want error
	}{
		{"short", func(b []byte) []byte { return b[:6] }, ErrShortPacket},
		{"version", func(b []byte) []byte { b[0] = (RTPVersion - 1) << 6; return b }, ErrBadVersion},
		{"extension", func(b []byte) []byte { b[0] |= 0x10; return b }, ErrExtension},
		{"csrc", func(b []byte) []byte { b[0] |= 0x01; return b }, ErrCSRC},
		{"padding", func(b []byte) []byte { b[0] |= 0x20; return b }, ErrPadding},
		{"marker", func(b []byte) []byte { b[1] |= 0x80; return b }, ErrBadMarker},
		{"payload-type", func(b []byte) []byte { b[1] = 95; return b }, ErrBadPayloadType},
		{"length", func(b []byte) []byte { return append(b, 0) }, ErrBadLength},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			b := tc.edit(append([]byte(nil), good...))
			if _, err := ParsePacket(b); err != tc.want {
				t.Fatalf("got %v, want %v", err, tc.want)
			}
		})
	}
}

func TestSeqUnwrapperDoubleWraparound(t *testing.T) {
	var u seqUnwrapper

	// Anchor near the 16-bit edge; run through two full wraparounds.
	const start = uint16(65530)
	const count = 65536*2 + 12
	var frontier uint16
	for i := 0; i < count; i++ {
		raw := uint16(uint32(start) + uint32(i))
		ext, kind := u.unwrap(raw)
		want := uint64(uint32(start)) + uint64(i)
		if i == 0 {
			if kind != seqFirst {
				t.Fatalf("i=%d: kind=%d want seqFirst", i, kind)
			}
		} else if kind != seqForward {
			t.Fatalf("i=%d raw=%d: kind=%d want seqForward", i, raw, kind)
		}
		if ext != want {
			t.Fatalf("raw=%d: ext=%d want %d", raw, ext, want)
		}
		u.commit(raw, ext)
		frontier = raw
	}
	lastExt := uint64(uint32(start)) + uint64(count-1)
	if wantCycles := lastExt - uint64(frontier); u.cycles != wantCycles {
		t.Fatalf("cycles=%d want %d", u.cycles, wantCycles)
	}

	// A reordered packet from the previous 16-bit cycle (raw above the
	// small post-wrap frontier) must unwrap behind the frontier.
	lateRaw := uint16(frontier - 5) // belongs to the just-finished cycle
	ext, kind := u.unwrap(lateRaw)
	if kind != seqReorder {
		t.Fatalf("kind=%d want seqReorder", kind)
	}
	if ext >= u.cycles+uint64(frontier) {
		t.Fatalf("late ext=%d not behind frontier %d", ext, u.cycles+uint64(frontier))
	}

	// A far jump is classified seqJump and leaves state untouched.
	before := u
	if _, kind := u.unwrap(uint16(frontier + 4000)); kind != seqJump {
		t.Fatalf("far jump kind=%d want seqJump", kind)
	}
	if u != before {
		t.Fatalf("seqJump mutated unwrapper state")
	}
}

func TestSeqUnwrapperReorderAcrossWrap(t *testing.T) {
	var u seqUnwrapper
	// anchor 65534, then 0 crosses the wrap and commits, then 65535
	// arrives late from the previous cycle.
	e, k := u.unwrap(65534)
	if k != seqFirst {
		t.Fatalf("want first, got %d", k)
	}
	u.commit(65534, e)

	e0, k0 := u.unwrap(0)
	if k0 != seqForward || e0 != 65536 {
		t.Fatalf("wrap: ext=%d kind=%d", e0, k0)
	}
	u.commit(0, e0)

	eLate, kLate := u.unwrap(65535)
	if kLate != seqReorder || eLate != 65535 {
		t.Fatalf("late across wrap: ext=%d kind=%d", eLate, kLate)
	}
}

func TestExtendTimestampWraparound(t *testing.T) {
	// Anchor frame 0 one frame below the 32-bit edge. Frames per one
	// timestamp cycle = 2^32 / 160. For each subsequent packet the
	// receiver's "expected" value is the previous frame's expected value;
	// the sender sends ext mod 2^32. We check key frames crossing the edge
	// once and twice.
	const ext0 = uint64(1<<32 - SamplesPerPacket)
	framesPerCycle := uint64(1<<32) / SamplesPerPacket

	keyFrames := []uint64{1, 2,
		framesPerCycle - 1, framesPerCycle, framesPerCycle + 1,
		2 * framesPerCycle, 2*framesPerCycle + 3}
	for _, i := range keyFrames {
		// Resolve the packet against the previous frame's expected value
		// (the playout frontier), exactly as the receiver does on arrival.
		expected := ext0 + i*SamplesPerPacket
		prev := ext0 + (i-1)*SamplesPerPacket
		raw := uint32(expected)
		if got := ExtendTimestamp(prev, raw); got != expected {
			t.Fatalf("frame %d: got %d want %d (raw=%d)", i, got, expected, raw)
		}
	}

	// Direct boundary checks.
	if got := ExtendTimestamp(1<<32, 0); got != 1<<32 {
		t.Fatalf("at wrap: got %d", got)
	}
	if got := ExtendTimestamp(1<<32, 160); got != 1<<32+160 {
		t.Fatalf("after wrap: got %d", got)
	}
	if got := ExtendTimestamp(2*(1<<32), 0); got != 2*(1<<32) {
		t.Fatalf("second wrap: got %d", got)
	}
	// A reordered packet just behind the edge resolves backwards.
	if got := ExtendTimestamp(1<<32, uint32(1<<32-160)); got != 1<<32-160 {
		t.Fatalf("pre-edge reorder: got %d", got)
	}
}
