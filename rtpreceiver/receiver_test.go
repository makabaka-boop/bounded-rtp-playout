package rtpreceiver

import (
	"bytes"
	"encoding/binary"
	"sync"
	"testing"
	"time"
)

type sinkCollector struct {
	mu     sync.Mutex
	frames []Frame
}

func (c *sinkCollector) add(f Frame) {
	c.mu.Lock()
	c.frames = append(c.frames, f)
	c.mu.Unlock()
}

func (c *sinkCollector) get() []Frame {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]Frame, len(c.frames))
	copy(out, c.frames)
	return out
}

type harness struct {
	t     *testing.T
	clock *VirtualClock
	rec   *Recorder
	rcv   *Receiver
	sink  *sinkCollector
	ssrc  uint32
}

func newHarness(t *testing.T, window int) *harness {
	t.Helper()
	clk := NewVirtualClock(time.Unix(1000, 0))
	rec := NewRecorder()
	col := &sinkCollector{}
	rcv := NewReceiver(Config{Clock: clk, Sink: col.add, Window: window}, rec)
	return &harness{t: t, clock: clk, rec: rec, rcv: rcv, sink: col, ssrc: 0x11223344}
}

func (h *harness) send(seq uint16, ts uint32, sample int16) {
	h.t.Helper()
	if err := h.rcv.HandlePacket(buildPacket(h.t, h.ssrc, seq, ts, sample)); err != nil {
		h.t.Fatalf("HandlePacket seq=%d: %v", seq, err)
	}
}

func (h *harness) advance(d time.Duration) { h.clock.Advance(d) }

// zero checks that every sample of f is zero.
func allZero(f Frame) bool {
	for _, v := range f.Samples {
		if v != 0 {
			return false
		}
	}
	return true
}

func TestStartDelayAndSteadyPlayout(t *testing.T) {
	h := newHarness(t, 0)
	t0 := h.clock.Now()
	h.send(0, 1000, 10)
	h.send(1, 1160, 11)
	h.send(2, 1320, 12)

	// Nothing may come out before the 60 ms deadline.
	h.advance(59 * time.Millisecond)
	if got := h.sink.get(); len(got) != 0 {
		t.Fatalf("emitted %d frames before 60ms", len(got))
	}
	h.advance(1 * time.Millisecond) // exactly 60 ms
	frames := h.sink.get()
	if len(frames) != 1 {
		t.Fatalf("at 60ms: %d frames, want 1", len(frames))
	}
	if !frames[0].EmittedAt.Equal(t0.Add(60 * time.Millisecond)) {
		t.Fatalf("first frame at %v, want %v", frames[0].EmittedAt, t0.Add(60*time.Millisecond))
	}
	if frames[0].Samples[0] != 10 || frames[0].Missing {
		t.Fatalf("frame 0 wrong: %+v", frames[0])
	}

	// One more frame every 20 ms.
	h.advance(20 * time.Millisecond)
	h.advance(20 * time.Millisecond)
	frames = h.sink.get()
	if len(frames) != 3 {
		t.Fatalf("3 frames expected, got %d", len(frames))
	}
	for i, f := range frames {
		if f.Index != uint64(i) || f.Seq != uint64(i) {
			t.Fatalf("frame %d position wrong: %+v", i, f)
		}
		if int(f.Samples[0]) != 10+i {
			t.Fatalf("frame %d sample = %d, want %d", i, f.Samples[0], 10+i)
		}
		if want := uint64(1000) + uint64(i)*SamplesPerPacket; f.Timestamp != want {
			t.Fatalf("frame %d timestamp = %d, want %d", i, f.Timestamp, want)
		}
		if f.Missing {
			t.Fatalf("frame %d unexpectedly missing", i)
		}
	}
}

func TestReorderWithinWindowByPosition(t *testing.T) {
	h := newHarness(t, 0)
	// Arrive shuffled: 0,3,1,2 — the ring must place each by sequence
	// position, not arrival order.
	h.send(0, 0, 100)
	h.send(3, 480, 103)
	h.send(1, 160, 101)
	h.send(2, 320, 102)

	h.advance(60 * time.Millisecond)
	h.advance(60 * time.Millisecond)
	frames := h.sink.get()
	if len(frames) != 4 {
		t.Fatalf("frames=%d, want 4", len(frames))
	}
	for i, f := range frames {
		if f.Missing || int(f.Samples[0]) != 100+i {
			t.Fatalf("frame %d: missing=%v sample=%d", i, f.Missing, f.Samples[0])
		}
	}
}

func TestDuplicateDedup(t *testing.T) {
	h := newHarness(t, 0)
	h.send(5, 0, 7)
	h.send(5, 0, 8) // identical position, different payload: first wins
	h.advance(60 * time.Millisecond)
	frames := h.sink.get()
	if len(frames) != 1 || frames[0].Samples[0] != 7 {
		t.Fatalf("duplicate before playout changed data: %+v", frames)
	}

	// Duplicate after the frame was already emitted must be dropped late,
	// never rewriting output.
	h.send(5, 0, 9)
	if got := h.rcv.StatsSnapshot(); true {
		st := got[frames[0].Generation]
		if st.Duplicates != 1 || st.LateDropped != 1 {
			t.Fatalf("stats: dup=%d late=%d", st.Duplicates, st.LateDropped)
		}
	}
	out := h.rec.Frames(frames[0].Generation)
	if out[0].Samples[0] != 7 {
		t.Fatal("late duplicate rewrote emitted samples")
	}
}

func TestLossZeroFillEvidenceAndLatePacket(t *testing.T) {
	h := newHarness(t, 0)
	h.send(0, 0, 200)
	h.send(2, 320, 202) // seq 1 lost

	h.advance(60 * time.Millisecond) // frame 0
	h.advance(20 * time.Millisecond) // frame 1 -> missing, zero fill
	frames := h.sink.get()
	if len(frames) != 2 || !frames[1].Missing || !allZero(frames[1]) {
		t.Fatalf("frame 1 not zero-filled: %+v", frames)
	}

	// Original packet arrives after its frame position was emitted.
	h.send(1, 160, 201)
	stats := h.rcv.StatsSnapshot()[frames[0].Generation]
	if stats.LateDropped != 1 || stats.FramesMissing != 1 {
		t.Fatalf("stats: late=%d missing=%d", stats.LateDropped, stats.FramesMissing)
	}

	h.advance(20 * time.Millisecond) // frame 2 from the buffer
	out := h.rec.Frames(frames[0].Generation)
	if len(out) != 3 {
		t.Fatalf("output length %d", len(out))
	}
	if !out[1].Missing || !allZero(out[1]) {
		t.Fatal("late packet rewrote the zero-filled frame")
	}
	if out[2].Missing || out[2].Samples[0] != 202 {
		t.Fatalf("frame 2 wrong: %+v", out[2])
	}

	// The report is generated from actual output frames plus evidence.
	rep := BuildMissingReport(out, stats, h.rcv.Evidence(frames[0].Generation), h.clock.Now())
	if len(rep.MissingRuns) != 1 {
		t.Fatalf("runs=%d want 1", len(rep.MissingRuns))
	}
	run := rep.MissingRuns[0]
	if run.StartFrame != 1 || run.EndFrame != 1 || run.Count != 1 {
		t.Fatalf("run bounds wrong: %+v", run)
	}
	if run.StartSeq != 1 || run.StartTimestamp != 160 {
		t.Fatalf("run identity wrong: %+v", run)
	}
	foundLate := false
	for _, e := range run.Evidence {
		if e.Kind == "late" && e.RawSeq == 1 {
			foundLate = true
		}
	}
	if !foundLate {
		t.Fatalf("late-packet evidence not attached to run: %+v", run.Evidence)
	}
}

func TestSequenceWraparoundAndReorderAcrossWrap(t *testing.T) {
	h := newHarness(t, 0)
	h.send(65535, 100000, 50)
	h.send(0, 100160, 51) // crosses the 16-bit edge
	h.send(65534, 99840, 49)
	// 65534 is before the anchor (65535): it is a late/before-start packet
	// and must not occupy window storage.
	stats := h.rcv.StatsSnapshot()
	var g GenStats
	for _, st := range stats {
		g = st
	}
	if g.LateDropped != 1 {
		t.Fatalf("pre-anchor packet accepted, late=%d", g.LateDropped)
	}

	// Reorder the other way: anchor, cross edge, then the last packet of
	// the old cycle arrives while still inside the window.
	h2 := newHarness(t, 0)
	h2.send(65535, 0, 60)
	h2.send(1, 320, 62)
	h2.send(0, 160, 61) // reordered packet from the new cycle, after wrap
	h2.advance(100 * time.Millisecond)
	frames := h2.sink.get()
	if len(frames) != 3 {
		t.Fatalf("frames=%d want 3", len(frames))
	}
	wantSeq := []uint64{65535, 65536, 65537}
	wantSamp := []int{60, 61, 62}
	for i := range wantSeq {
		if frames[i].Seq != wantSeq[i] || frames[i].Missing {
			t.Fatalf("frame %d: seq=%d missing=%v", i, frames[i].Seq, frames[i].Missing)
		}
		if int(frames[i].Samples[0]) != wantSamp[i] {
			t.Fatalf("frame %d sample=%d want %d", i, frames[i].Samples[0], wantSamp[i])
		}
	}
}

func TestJustInTimePacketBeforeDeadline(t *testing.T) {
	h := newHarness(t, 0)
	h.send(0, 0, 300)
	h.send(2, 320, 302)
	// Seq 1 shows up 20 ms before its own frame deadline, after the
	// receiver already knows about seq 2 (true out-of-order arrival).
	h.advance(40 * time.Millisecond)
	h.send(1, 160, 301)
	h.advance(60 * time.Millisecond)
	frames := h.sink.get()
	if len(frames) != 3 {
		t.Fatalf("frames=%d want 3", len(frames))
	}
	for i, f := range frames {
		if f.Missing || int(f.Samples[0]) != 300+i {
			t.Fatalf("frame %d: missing=%v sample=%d", i, f.Missing, f.Samples[0])
		}
	}
}

func TestJustMissedDeadlineZeroFills(t *testing.T) {
	h := newHarness(t, 0)
	h.send(0, 0, 300)
	h.send(2, 320, 302)
	h.advance(60 * time.Millisecond) // emits frame 0; frame 1 still empty
	if fs := h.sink.get(); len(fs) != 1 {
		t.Fatalf("frames=%d want 1", len(fs))
	}
	// The frame-1 deadline (80 ms) passes with no packet: zero fill.
	h.advance(20 * time.Millisecond)
	frames := h.sink.get()
	if len(frames) != 2 || !frames[1].Missing || !allZero(frames[1]) {
		t.Fatalf("frame 1 must be zero-filled, got %+v", frames)
	}
	// Seq 1 reaches us only after the frame-1 tick already passed it.
	h.send(1, 160, 301)
	h.advance(20 * time.Millisecond) // frame 2
	out := h.rec.Frames(frames[0].Generation)
	if len(out) != 3 {
		t.Fatalf("frames=%d want 3", len(out))
	}
	if !out[1].Missing || !allZero(out[1]) {
		t.Fatal("late packet rewrote the zero-filled frame")
	}
	if out[2].Missing || out[2].Samples[0] != 302 {
		t.Fatalf("frame 2 wrong: %+v", out[2])
	}
	st := h.rcv.StatsSnapshot()[frames[0].Generation]
	if st.LateDropped != 1 || st.FramesMissing != 1 {
		t.Fatalf("stats: late=%d missing=%d", st.LateDropped, st.FramesMissing)
	}
}

func TestTimestampWraparound(t *testing.T) {
	h := newHarness(t, 0)
	const edge = uint32(1<<32 - SamplesPerPacket)
	h.send(0, edge, 1)
	h.send(1, 0, 2) // raw timestamp wraps to 0
	h.send(2, SamplesPerPacket, 3)
	h.advance(100 * time.Millisecond)
	frames := h.sink.get()
	if len(frames) != 3 {
		t.Fatalf("frames=%d", len(frames))
	}
	wantTS := []uint64{1<<32 - SamplesPerPacket, 1 << 32, 1<<32 + SamplesPerPacket}
	for i, want := range wantTS {
		if frames[i].Timestamp != want {
			t.Fatalf("frame %d ts=%d want %d", i, frames[i].Timestamp, want)
		}
	}
}

func TestDoubleSequenceWraparound(t *testing.T) {
	h := newHarness(t, 32)
	startVal := uint16(65530)
	const count = 65536*2 + 12

	// First packet anchors the generation; afterwards feed one packet per
	// 20 ms tick so the bounded window never overflows and every frame is
	// played on time.
	h.send(startVal, 0, int16(uint16(startVal)))
	for i := 1; i < count; i++ {
		raw := uint16(uint32(startVal) + uint32(i))
		ts := uint32(uint64(i) * SamplesPerPacket)
		h.send(raw, ts, int16(i))
		h.advance(FrameDuration)
	}
	// The last packet is three frames after the last tick's horizon
	// (start delay alignment); drain the remaining playout ticks.
	h.advance(3 * FrameDuration)

	frames := h.rec.Frames(1)
	if len(frames) != count {
		t.Fatalf("frames=%d want %d", len(frames), count)
	}
	// Spot-check positions at both wraparound boundaries.
	for _, idx := range []int{0, 5, 6, 7, 65541, 65542, 65543, count - 1} {
		f := frames[idx]
		wantSeq := uint64(uint32(startVal)) + uint64(idx)
		if f.Seq != wantSeq || f.Missing {
			t.Fatalf("frame %d: seq=%d want %d missing=%v", idx, f.Seq, wantSeq, f.Missing)
		}
		if wantTS := uint64(idx) * SamplesPerPacket; f.Timestamp != wantTS {
			t.Fatalf("frame %d ts=%d want %d", idx, f.Timestamp, wantTS)
		}
	}
	st := h.rcv.StatsSnapshot()[1]
	if st.FramesMissing != 0 || st.LateDropped != 0 || st.AheadRejected != 0 ||
		st.Duplicates != 0 || st.PacketsAccepted != count {
		t.Fatalf("stats after double wrap: %+v", st)
	}
}

func TestReceiveWindowBound(t *testing.T) {
	h := newHarness(t, 4) // positions cursor..cursor+3 are valid
	h.send(0, 0, 1)
	h.send(4, 640, 5) // exactly one beyond the window
	h.send(3, 480, 4)

	stats := h.rcv.StatsSnapshot()
	var g GenStats
	for _, st := range stats {
		g = st
	}
	if g.AheadRejected != 1 || g.PacketsAccepted != 2 {
		t.Fatalf("window stats: %+v", g)
	}

	h.advance(120 * time.Millisecond)
	frames := h.sink.get()
	if len(frames) != 4 {
		t.Fatalf("frames=%d", len(frames))
	}
	if frames[0].Missing || frames[3].Missing {
		t.Fatal("edge packets should be present")
	}
	if !frames[1].Missing || !frames[2].Missing {
		t.Fatal("gap packets should be zero-filled")
	}
}

func TestStopLateDataRestartNewGeneration(t *testing.T) {
	h := newHarness(t, 0)
	h.send(0, 0, 1)
	h.send(1, 160, 2)
	h.advance(80 * time.Millisecond) // frames 0 and 1
	before := h.sink.get()
	if len(before) != 2 {
		t.Fatalf("frames before stop: %d", len(before))
	}

	h.rcv.StopSource(h.ssrc)
	// Late data after stop: recorded as evidence, never emitted.
	h.send(1, 160, 99)
	h.send(2, 320, 3)
	h.advance(100 * time.Millisecond)
	if got := h.sink.get(); len(got) != 2 {
		t.Fatalf("frames emitted after stop: %d", len(got))
	}
	stats := h.rcv.StatsSnapshot()
	if stats[1].AfterStopDropped != 2 || stats[1].StoppedAt.IsZero() {
		t.Fatalf("stop stats: %+v", stats[1])
	}

	// Reopen: a new capture generation with independent state.
	if err := h.rcv.RestartSource(h.ssrc); err != nil {
		t.Fatal(err)
	}
	h.send(100, 5000, 70) // arbitrary seq/ts: fresh unwrap baseline
	h.advance(60 * time.Millisecond)
	frames := h.sink.get()
	if len(frames) != 3 {
		t.Fatalf("frames after restart: %d", len(frames))
	}
	if frames[2].Generation != 2 || frames[2].Seq != 100 || frames[2].Samples[0] != 70 {
		t.Fatalf("new generation frame wrong: %+v", frames[2])
	}
	if frames[2].Timestamp != 5000 {
		t.Fatalf("new gen ts=%d want 5000", frames[2].Timestamp)
	}

	// Both generations remain in the recorder and in statistics.
	g1 := h.rec.Frames(1)
	g2 := h.rec.Frames(2)
	if len(g1) != 2 || len(g2) != 1 {
		t.Fatalf("recorder generations: %d/%d", len(g1), len(g2))
	}
	all := h.rcv.StatsSnapshot()
	if all[1].Generation != 1 || all[2].Generation != 2 {
		t.Fatalf("stats lost across restart: %+v", all)
	}
}

func TestUnknownSSRCAfterStopIsRejected(t *testing.T) {
	h := newHarness(t, 0)
	h.send(0, 0, 1)
	h.advance(60 * time.Millisecond)
	h.rcv.StopSource(h.ssrc)
	h.send(0, 0, 2) // stopped source, not a restart
	st := h.rcv.StatsSnapshot()[1]
	if st.AfterStopDropped != 1 {
		t.Fatalf("after-stop drop not counted: %+v", st)
	}
}

func TestInvalidPacketsAndMaxSources(t *testing.T) {
	clk := NewVirtualClock(time.Unix(0, 0))
	rcv := NewReceiver(Config{Clock: clk, MaxSources: 1}, NewRecorder())

	if err := rcv.HandlePacket([]byte("short")); err != ErrShortPacket {
		t.Fatalf("err=%v", err)
	}
	if rcv.InvalidPackets() != 1 {
		t.Fatalf("invalid=%d", rcv.InvalidPackets())
	}
	if err := rcv.HandlePacket(buildPacket(t, 1, 0, 0, 0)); err != nil {
		t.Fatal(err)
	}
	if err := rcv.HandlePacket(buildPacket(t, 2, 0, 0, 0)); err != ErrTooManySources {
		t.Fatalf("second source err=%v", err)
	}
}

func TestCloseRejectsAndStopsTimers(t *testing.T) {
	h := newHarness(t, 0)
	h.send(0, 0, 1)
	if h.clock.PendingTimers() != 1 {
		t.Fatalf("timers pending=%d want 1", h.clock.PendingTimers())
	}
	h.rcv.Close()
	if h.clock.PendingTimers() != 0 {
		t.Fatalf("timers after close=%d", h.clock.PendingTimers())
	}
	if err := h.rcv.HandlePacket(buildPacket(t, h.ssrc, 1, 160, 2)); err != ErrReceiverClosed {
		t.Fatalf("post-close err=%v", err)
	}
}

func TestWAVRoundTrip(t *testing.T) {
	frames := []Frame{
		{Samples: mkSamples(0)},
		{Samples: mkSamples(-12345)},
		{Samples: mkSamples(30000)},
		{Missing: true},
	}
	var buf bytes.Buffer
	if err := WriteWAV(&buf, frames); err != nil {
		t.Fatal(err)
	}
	raw := buf.Bytes()
	if string(raw[0:4]) != "RIFF" || string(raw[8:12]) != "WAVE" || string(raw[36:40]) != "data" {
		t.Fatal("WAV magic wrong")
	}
	if binary.LittleEndian.Uint16(raw[22:24]) != 1 {
		t.Fatal("not mono")
	}
	if rate := binary.LittleEndian.Uint32(raw[24:28]); rate != ClockRateHz {
		t.Fatalf("rate=%d", rate)
	}
	if bits := binary.LittleEndian.Uint16(raw[34:36]); bits != 16 {
		t.Fatalf("bits=%d", bits)
	}
	if dataLen := binary.LittleEndian.Uint32(raw[40:44]); dataLen != uint32(len(frames)*PayloadLen) {
		t.Fatalf("dataLen=%d", dataLen)
	}
	if buf.Len() != 44+len(frames)*PayloadLen {
		t.Fatalf("total size=%d", buf.Len())
	}
	// PCM16 samples are little-endian on disk even though RTP is big-endian.
	first := int16(binary.LittleEndian.Uint16(raw[44:46]))
	second := int16(binary.LittleEndian.Uint16(raw[44+PayloadLen : 44+PayloadLen+2]))
	if first != 0 || second != -12345 {
		t.Fatalf("payload samples wrong: %d %d", first, second)
	}
	// Zero-filled frame is present verbatim.
	off := 44 + 3*PayloadLen
	for i := 0; i < PayloadLen; i++ {
		if raw[off+i] != 0 {
			t.Fatalf("missing frame not silence at byte %d", i)
		}
	}
}

func mkSamples(v int16) [SamplesPerPacket]int16 {
	var s [SamplesPerPacket]int16
	for i := range s {
		s[i] = v
	}
	return s
}

func TestReportExtraEvidence(t *testing.T) {
	now := time.Unix(1000, 0)
	frames := []Frame{
		{Generation: 7, Index: 0, Seq: 10, Missing: false, EmittedAt: now},
		{Generation: 7, Index: 1, Seq: 11, Missing: true, EmittedAt: now.Add(20 * time.Millisecond)},
	}
	stats := GenStats{SSRC: 0xab, Generation: 7, FramesEmitted: 2, FramesMissing: 1, AfterStopDropped: 1}
	ev := []Evidence{
		{At: now.Add(20 * time.Millisecond), Kind: "missing", FrameIndex: 1, Seq: 11, Timestamp: 1760},
		{At: now.Add(9 * time.Second), Kind: "after_stop", FrameIndex: -1, RawSeq: 42},
	}
	rep := BuildMissingReport(frames, stats, ev, now)
	if len(rep.MissingRuns) != 1 {
		t.Fatalf("runs=%d", len(rep.MissingRuns))
	}
	if len(rep.ExtraEvidence) != 1 || rep.ExtraEvidence[0].Kind != "after_stop" {
		t.Fatalf("extra evidence wrong: %+v", rep.ExtraEvidence)
	}
	if rep.SSRC != "0x000000ab" {
		t.Fatalf("ssrc text=%q", rep.SSRC)
	}
}
