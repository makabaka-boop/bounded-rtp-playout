package rtpreceiver

import (
	"sync"
	"time"
)

// Frame is one 20 ms / 160-sample output frame, as actually emitted by the
// playout clock. WAV output and missing reports are generated exclusively
// from emitted frames, never from the receive buffer.
type Frame struct {
	SSRC       uint32
	Generation uint64
	// Index is the frame number within the generation, starting at zero.
	Index uint64
	// Seq is the extended (unwrapped) sequence number of the frame.
	Seq uint64
	// Timestamp is the extended RTP timestamp: the sender's value when the
	// packet arrived, or the expected value for a zero-filled frame.
	Timestamp uint64
	Samples   [SamplesPerPacket]int16
	// Missing reports that the frame was zero-filled because no packet was
	// available at the playout deadline.
	Missing bool
	// EmittedAt is the controllable-clock time at which the frame came out.
	EmittedAt time.Time
}

// FrameSink receives emitted frames. Implementations must be safe for
// concurrent use if the receiver runs multiple sources.
type FrameSink func(Frame)

// Recorder stores the actual output stream per (SSRC, generation). It is the
// single source of truth for WAV files and missing reports.
type Recorder struct {
	mu     sync.Mutex
	frames map[uint64][]Frame // keyed by the globally unique generation id
}

// NewRecorder creates an empty recorder.
func NewRecorder() *Recorder {
	return &Recorder{frames: make(map[uint64][]Frame)}
}

// AddFrame appends an emitted frame.
func (rec *Recorder) AddFrame(f Frame) {
	rec.mu.Lock()
	rec.frames[f.Generation] = append(rec.frames[f.Generation], f)
	rec.mu.Unlock()
}

// Frames returns a copy of the emitted frames for one generation.
func (rec *Recorder) Frames(generation uint64) []Frame {
	rec.mu.Lock()
	defer rec.mu.Unlock()
	src := rec.frames[generation]
	out := make([]Frame, len(src))
	copy(out, src)
	return out
}

// Generations returns the generation ids with at least one emitted frame.
func (rec *Recorder) Generations() []uint64 {
	rec.mu.Lock()
	defer rec.mu.Unlock()
	out := make([]uint64, 0, len(rec.frames))
	for g := range rec.frames {
		out = append(out, g)
	}
	return out
}

// GenStats are the per-generation counters used by the missing report.
type GenStats struct {
	SSRC       uint32
	Generation uint64

	FirstPacketAt time.Time
	FirstFrameAt  time.Time
	StoppedAt     time.Time

	// PacketsAccepted counts unique packets that entered the reorder window.
	PacketsAccepted int64
	// Duplicates counts packets dropped because their slot was already
	// occupied by an identical sequence number.
	Duplicates int64
	// LateDropped counts packets whose frame position had already been
	// emitted (they can never alter produced samples).
	LateDropped int64
	// AheadRejected counts packets beyond the fixed receive window or the
	// RFC 3550 jump guard.
	AheadRejected int64
	// AfterStopDropped counts packets that arrived once the generation was
	// stopped.
	AfterStopDropped int64

	FramesEmitted int64
	FramesMissing int64
}

// dropEvidence records why a packet or frame position was unusable. It is
// the evidence trail attached to missing reports.
type dropEvidence struct {
	At         time.Time
	Kind       string // "missing", "late", "ahead", "after_stop"
	FrameIndex int64  // -1 when unknown
	Seq        uint64
	RawSeq     uint16
	Timestamp  uint64
	RawTS      uint32
}
