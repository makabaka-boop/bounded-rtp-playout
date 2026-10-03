package rtpreceiver

import (
	"errors"
	"net"
	"sync"
	"time"
)

// FrameDuration is the wall-clock duration of one 160-sample frame at 8 kHz:
// exactly 20 ms.
const FrameDuration = time.Duration(SamplesPerPacket) * time.Second / ClockRateHz

// DefaultStartDelay: output for a source begins 60 ms (three frames) after
// the first packet of its generation.
const DefaultStartDelay = 3 * FrameDuration

// Config configures a Receiver.
type Config struct {
	Clock Clock
	Sink  FrameSink
	// Window bounds the per-source reorder queue in packets. A packet more
	// than Window-1 frames ahead of the current playout position is rejected.
	// Zero selects 32; values below 4 are raised to 4.
	Window int
	// StartDelay is the interval between a generation's first packet and its
	// first output frame. Zero selects DefaultStartDelay (60 ms).
	StartDelay time.Duration
	// MaxSources bounds the number of distinct SSRCs accepted. Zero means
	// unlimited.
	MaxSources int
}

func (c *Config) withDefaults() Config {
	cfg := *c
	if cfg.Clock == nil {
		cfg.Clock = WallClock()
	}
	if cfg.Window < 4 {
		if cfg.Window == 0 {
			cfg.Window = 32
		} else {
			cfg.Window = 4
		}
	}
	if cfg.StartDelay == 0 {
		cfg.StartDelay = DefaultStartDelay
	}
	return cfg
}

// Receiver accepts RTP datagrams, reorders them per SSRC and emits frames
// from a controllable clock. All methods are safe for concurrent use.
type Receiver struct {
	cfg      Config
	mu       sync.Mutex
	sources  map[uint32]*source
	retired  map[uint32][]*source // generations replaced via RestartSource
	nextGen  uint64
	recorder *Recorder

	invalid   int64
	invalidMu sync.Mutex
	closed    bool
}

// NewReceiver builds a receiver. Every emitted frame is stored in recorder
// (the source of truth for WAV files and missing reports) and additionally
// passed to sink when non-nil.
func NewReceiver(cfg Config, recorder *Recorder) *Receiver {
	if recorder == nil {
		recorder = NewRecorder()
	}
	return &Receiver{
		cfg:      cfg.withDefaults(),
		sources:  make(map[uint32]*source),
		retired:  make(map[uint32][]*source),
		nextGen:  1,
		recorder: recorder,
	}
}

// ErrReceiverClosed is returned by packet handling after Close.
var ErrReceiverClosed = errors.New("rtpreceiver: receiver closed")

// ErrTooManySources is returned when a new SSRC would exceed MaxSources.
var ErrTooManySources = errors.New("rtpreceiver: source table full")

// HandlePacket parses one raw datagram and runs it through the source state
// machine. Parse errors are counted via InvalidPackets.
func (r *Receiver) HandlePacket(b []byte) error {
	pkt, err := ParsePacket(b)
	if err != nil {
		r.invalidMu.Lock()
		r.invalid++
		r.invalidMu.Unlock()
		return err
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return ErrReceiverClosed
	}
	s, ok := r.sources[pkt.SSRC]
	if !ok {
		if r.cfg.MaxSources > 0 && len(r.sources) >= r.cfg.MaxSources {
			return ErrTooManySources
		}
		// First packet from an unknown SSRC opens a new capture generation.
		s = newSource(pkt.SSRC, r.allocGenLocked(), r.cfg.Window)
		r.sources[pkt.SSRC] = s
	}
	s.deliver(r, pkt)
	return nil
}

func (r *Receiver) allocGenLocked() uint64 {
	g := r.nextGen
	r.nextGen++
	return g
}

// RestartSource forces a source to reopen with a fresh capture generation:
// new sequence/timestamp unwrap state, an empty reorder window, reset
// timers and statistics. The next packet from ssrc starts it. Frames of
// previous generations remain available in the recorder.
func (r *Receiver) RestartSource(ssrc uint32) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return ErrReceiverClosed
	}
	if old, ok := r.sources[ssrc]; ok {
		old.finish(r)
		r.retired[ssrc] = append(r.retired[ssrc], old)
	}
	s := newSource(ssrc, r.allocGenLocked(), r.cfg.Window)
	s.awaiting = true
	r.sources[ssrc] = s
	return nil
}

// StopSource ends the source's current generation. Frames already emitted
// remain available; later packets are dropped with evidence until the source
// is restarted.
func (r *Receiver) StopSource(ssrc uint32) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if s, ok := r.sources[ssrc]; ok {
		s.finish(r)
	}
}

// Close stops every source. Subsequent packets return ErrReceiverClosed.
func (r *Receiver) Close() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return
	}
	r.closed = true
	for _, s := range r.sources {
		s.cancelTimer()
	}
}

// InvalidPackets reports how many datagrams failed ParsePacket.
func (r *Receiver) InvalidPackets() int64 {
	r.invalidMu.Lock()
	defer r.invalidMu.Unlock()
	return r.invalid
}

// StatsSnapshot returns per-generation statistics keyed by generation id,
// including generations that were later restarted away.
func (r *Receiver) StatsSnapshot() map[uint64]GenStats {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make(map[uint64]GenStats)
	for _, list := range r.retired {
		for _, s := range list {
			out[s.generation] = s.finalStats()
		}
	}
	for _, s := range r.sources {
		out[s.generation] = s.finalStats()
	}
	return out
}

// Sources returns the SSRCs currently known to the receiver.
func (r *Receiver) Sources() []uint32 {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]uint32, 0, len(r.sources))
	for ssrc := range r.sources {
		out = append(out, ssrc)
	}
	return out
}

// Recorder returns the recorder holding every emitted frame.
func (r *Receiver) Recorder() *Recorder { return r.recorder }

// evidenceFor returns the recorded packet/frame evidence for a generation.
func (r *Receiver) evidenceFor(generation uint64) []dropEvidence {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, list := range r.retired {
		for _, s := range list {
			if e := s.evidence[generation]; e != nil {
				return e
			}
		}
	}
	for _, s := range r.sources {
		if e := s.evidence[generation]; e != nil {
			return e
		}
	}
	return nil
}

// ServeUDP feeds datagrams read from conn to the receiver until a read error
// occurs (typically because conn was closed).
func (r *Receiver) ServeUDP(conn *net.UDPConn) error {
	buf := make([]byte, 65535)
	for {
		n, _, err := conn.ReadFromUDP(buf)
		if err != nil {
			return err
		}
		// ParsePacket copies the bytes; the read buffer is reused.
		_ = r.HandlePacket(buf[:n])
	}
}

// source is the per-SSRC playout state machine. Receiver.mu guards all
// fields; clock callbacks re-acquire that lock.
type source struct {
	ssrc       uint32
	generation uint64
	window     int

	// awaiting is set by RestartSource until the first packet arrives.
	awaiting bool
	// stopped is set by StopSource; later packets are recorded and dropped.
	stopped bool
	timer   Timer

	unwrap    seqUnwrapper
	firstExt  uint64 // extended seq of the generation's first packet
	firstTS   uint32 // raw RTP timestamp of the generation's first packet
	nextFrame uint64 // frame index (relative to firstExt) to emit next

	ring    []*slot // indexed by frame index modulo window
	startAt time.Time

	stats    GenStats
	evidence map[uint64][]dropEvidence

	firstPacketSeen bool
}

// slot is one occupied position in the reorder window.
type slot struct {
	seq       uint64 // extended sequence number (= frame position)
	timestamp uint64 // extended sender RTP timestamp
	samples   [SamplesPerPacket]int16
}

func newSource(ssrc uint32, generation uint64, window int) *source {
	return &source{
		ssrc:       ssrc,
		generation: generation,
		window:     window,
		evidence:   make(map[uint64][]dropEvidence),
	}
}

// deliver processes one parsed packet. Receiver.mu is held.
func (s *source) deliver(r *Receiver, pkt *Packet) {
	now := r.cfg.Clock.Now()

	if s.stopped {
		s.stats.AfterStopDropped++
		s.evidence[s.generation] = append(s.evidence[s.generation], dropEvidence{
			At: now, Kind: "after_stop", FrameIndex: -1,
			RawSeq: pkt.Seq, RawTS: pkt.Timestamp,
		})
		return
	}
	if s.awaiting || !s.firstPacketSeen {
		s.awaiting = false
		s.anchor(r, pkt, now)
		return
	}

	ext, kind := s.unwrap.unwrap(pkt.Seq)
	cursorExt := s.firstExt + s.nextFrame

	// A frame at or behind the cursor has already been emitted (or is being
	// emitted this instant). Late data must never rewrite output samples.
	if s.nextFrame > 0 && ext <= cursorExt-1 {
		s.dropLate(pkt, ext, now)
		return
	}

	// Before the first frame is due the cursor is still at the anchor; a
	// packet whose extended sequence is below the anchor can never belong to
	// this generation's output.
	if ext < s.firstExt {
		s.dropLate(pkt, ext, now)
		return
	}

	ahead := int64(ext - cursorExt) // >= 0 here; 0 is a duplicate of current
	if kind == seqJump || ahead >= int64(s.window) {
		// Beyond the bounded reorder window (or RFC 3550 jump guard).
		s.stats.AheadRejected++
		s.evidence[s.generation] = append(s.evidence[s.generation], dropEvidence{
			At: now, Kind: "ahead",
			FrameIndex: int64(ext - s.firstExt),
			Seq:        ext, RawSeq: pkt.Seq,
			Timestamp: s.expectedTS(ext), RawTS: pkt.Timestamp,
		})
		return
	}

	idx := (ext - s.firstExt) % uint64(s.window)
	if sl := s.ring[idx]; sl != nil && sl.seq == ext {
		// Same sequence number already buffered: duplicate, ignored.
		s.stats.Duplicates++
		return
	}

	sl := &slot{seq: ext, timestamp: ExtendTimestamp(s.expectedTS(ext), pkt.Timestamp)}
	sl.samples = pkt.Samples
	s.ring[idx] = sl
	s.stats.PacketsAccepted++
	if kind == seqForward {
		s.unwrap.commit(pkt.Seq, ext)
	}
}

// expectedTS returns the extended sender timestamp for an extended sequence.
func (s *source) expectedTS(ext uint64) uint64 {
	return uint64(s.firstTS) + (ext-s.firstExt)*SamplesPerPacket
}

// dropLate records a packet that can no longer influence output because its
// frame position precedes the anchor or the playout cursor.
func (s *source) dropLate(pkt *Packet, ext uint64, now time.Time) {
	s.stats.LateDropped++
	var ts uint64
	var fi int64
	if ext >= s.firstExt {
		ts = s.expectedTS(ext)
		fi = int64(ext - s.firstExt)
	} else {
		fi = -int64(s.firstExt - ext)
	}
	s.evidence[s.generation] = append(s.evidence[s.generation], dropEvidence{
		At: now, Kind: "late",
		FrameIndex: fi,
		Seq:        ext, RawSeq: pkt.Seq,
		Timestamp: ts, RawTS: pkt.Timestamp,
	})
}

// anchor opens the generation with its first packet and arms the 60 ms timer.
func (s *source) anchor(r *Receiver, pkt *Packet, now time.Time) {
	s.ring = make([]*slot, s.window)
	s.unwrap.reset()
	ext := uint64(pkt.Seq)
	s.unwrap.commit(pkt.Seq, ext)
	s.firstExt = ext
	s.firstTS = pkt.Timestamp
	s.nextFrame = 0
	s.firstPacketSeen = true

	s.stats = GenStats{
		SSRC:            s.ssrc,
		Generation:      s.generation,
		FirstPacketAt:   now,
		PacketsAccepted: 1,
	}

	sl := &slot{seq: ext, timestamp: uint64(pkt.Timestamp)}
	sl.samples = pkt.Samples
	s.ring[0] = sl

	s.startAt = now.Add(r.cfg.StartDelay)
	s.timer = r.cfg.Clock.AfterFunc(r.cfg.StartDelay, func() { s.tick(r) })
}

// tick runs on the clock thread. Receiver.mu must not be held.
func (s *source) tick(r *Receiver) {
	var out []Frame

	r.mu.Lock()
	if s.stopped || !s.firstPacketSeen {
		r.mu.Unlock()
		return
	}
	now := r.cfg.Clock.Now()
	for !s.frameTime(s.nextFrame).After(now) {
		out = append(out, s.emitFrameLocked())
	}
	wait := s.frameTime(s.nextFrame).Sub(now)
	if wait < 0 {
		wait = 0
	}
	s.timer = r.cfg.Clock.AfterFunc(wait, func() { s.tick(r) })
	r.mu.Unlock()

	for _, f := range out {
		s.dispatch(r, f)
	}
}

// frameTime is the scheduled emission time of frame index i.
func (s *source) frameTime(i uint64) time.Time {
	return s.startAt.Add(time.Duration(i) * FrameDuration)
}

// emitFrameLocked produces the frame at the playout cursor, zero-filling and
// recording evidence when its window slot is empty. Receiver.mu is held.
func (s *source) emitFrameLocked() Frame {
	ext := s.firstExt + s.nextFrame
	f := Frame{
		SSRC:       s.ssrc,
		Generation: s.generation,
		Index:      s.nextFrame,
		Seq:        ext,
		Timestamp:  s.expectedTS(ext),
		EmittedAt:  s.frameTime(s.nextFrame),
	}

	idx := s.nextFrame % uint64(s.window)
	if sl := s.ring[idx]; sl != nil && sl.seq == ext {
		f.Samples = sl.samples
		f.Timestamp = sl.timestamp
		s.ring[idx] = nil
	} else {
		f.Missing = true
		s.stats.FramesMissing++
		s.evidence[s.generation] = append(s.evidence[s.generation], dropEvidence{
			At: f.EmittedAt, Kind: "missing",
			FrameIndex: int64(s.nextFrame),
			Seq:        ext, RawSeq: uint16(ext),
			Timestamp: f.Timestamp, RawTS: uint32(f.Timestamp),
		})
	}

	s.nextFrame++
	s.stats.FramesEmitted++
	if s.stats.FramesEmitted == 1 {
		s.stats.FirstFrameAt = f.EmittedAt
	}
	return f
}

func (s *source) dispatch(r *Receiver, f Frame) {
	r.recorder.AddFrame(f)
	if r.cfg.Sink != nil {
		r.cfg.Sink(f)
	}
}

// finish ends the active generation: cancel the tick timer and stamp the
// statistics.
func (s *source) finish(r *Receiver) {
	s.cancelTimer()
	if !s.stopped {
		s.stopped = true
		s.stats.StoppedAt = r.cfg.Clock.Now()
	}
}

func (s *source) cancelTimer() {
	if s.timer != nil {
		s.timer.Stop()
		s.timer = nil
	}
}

// finalStats returns the stats snapshot (stopped time filled when active).
func (s *source) finalStats() GenStats { return s.stats }
