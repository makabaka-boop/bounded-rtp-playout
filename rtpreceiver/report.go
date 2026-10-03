package rtpreceiver

import (
	"encoding/json"
	"os"
	"time"
)

// MissingRun is one contiguous run of zero-filled frames in the actual
// output record.
type MissingRun struct {
	// StartFrame/EndFrame are frame indices within the generation, both
	// inclusive.
	StartFrame int64 `json:"startFrame"`
	EndFrame   int64 `json:"endFrame"`
	Count      int   `json:"count"`
	// StartSeq/EndSeq are extended (unwrapped) RTP sequence numbers.
	StartSeq uint64 `json:"startSeq"`
	EndSeq   uint64 `json:"endSeq"`
	// StartTimestamp is the extended RTP timestamp of the first missing
	// frame (firstTS + startFrame*160).
	StartTimestamp uint64 `json:"startTimestamp"`
	// EmittedAt is the playout-clock time of the first frame in the run.
	EmittedAt time.Time `json:"emittedAt"`
	// Evidence lists recorded packet events touching the run or its
	// surroundings (late packets, out-of-window rejects).
	Evidence []Evidence `json:"evidence"`
}

// Evidence is one recorded rejection or fill event.
type Evidence struct {
	At         time.Time `json:"at"`
	Kind       string    `json:"kind"`       // "missing", "late", "ahead", "after_stop"
	FrameIndex int64     `json:"frameIndex"` // -1 when unrelated to a frame
	Seq        uint64    `json:"seq"`
	RawSeq     uint16    `json:"rawSeq"`
	Timestamp  uint64    `json:"timestamp"`
	RawTS      uint32    `json:"rawTimestamp"`
}

// MissingReport describes one generation's output quality and is generated
// from the frames actually emitted, not from receive-buffer contents.
type MissingReport struct {
	SSRC          string     `json:"ssrc"`
	SSRCUint      uint32     `json:"ssrcUint"`
	Generation    uint64     `json:"generation"`
	GeneratedAt   time.Time  `json:"generatedAt"`
	FirstPacketAt time.Time  `json:"firstPacketAt"`
	FirstFrameAt  time.Time  `json:"firstFrameAt"`
	StoppedAt     *time.Time `json:"stoppedAt,omitempty"`

	FramesEmitted    int64 `json:"framesEmitted"`
	FramesMissing    int64 `json:"framesMissing"`
	PacketsAccepted  int64 `json:"packetsAccepted"`
	Duplicates       int64 `json:"duplicates"`
	LateDropped      int64 `json:"lateDropped"`
	AheadRejected    int64 `json:"aheadRejected"`
	AfterStopDropped int64 `json:"afterStopDropped"`

	MissingRuns []MissingRun `json:"missingRuns"`
	// ExtraEvidence holds packet events not attributable to a missing run
	// (after-stop data, rejects while everything else played fine).
	ExtraEvidence []Evidence `json:"extraEvidence,omitempty"`
}

// BuildMissingReport derives the report for a generation from its emitted
// frames, statistics and evidence trail (see Receiver.Evidence).
func BuildMissingReport(frames []Frame, stats GenStats, ev []Evidence, now time.Time) MissingReport {
	rep := MissingReport{
		SSRC:             ssrcHex(stats.SSRC),
		SSRCUint:         stats.SSRC,
		Generation:       stats.Generation,
		GeneratedAt:      now,
		FirstPacketAt:    stats.FirstPacketAt,
		FirstFrameAt:     stats.FirstFrameAt,
		FramesEmitted:    stats.FramesEmitted,
		FramesMissing:    stats.FramesMissing,
		PacketsAccepted:  stats.PacketsAccepted,
		Duplicates:       stats.Duplicates,
		LateDropped:      stats.LateDropped,
		AheadRejected:    stats.AheadRejected,
		AfterStopDropped: stats.AfterStopDropped,
	}
	if !stats.StoppedAt.IsZero() {
		t := stats.StoppedAt
		rep.StoppedAt = &t
	}

	// Derive runs from the actual output frames.
	var runs []MissingRun
	for i := 0; i < len(frames); {
		if !frames[i].Missing {
			i++
			continue
		}
		start := i
		for i < len(frames) && frames[i].Missing {
			i++
		}
		end := i - 1
		runs = append(runs, MissingRun{
			StartFrame:     int64(start),
			EndFrame:       int64(end),
			Count:          end - start + 1,
			StartSeq:       frames[start].Seq,
			EndSeq:         frames[end].Seq,
			StartTimestamp: frames[start].Timestamp,
			EmittedAt:      frames[start].EmittedAt,
		})
	}

	used := make([]bool, len(ev))
	for ri := range runs {
		lo, hi := runs[ri].StartFrame, runs[ri].EndFrame
		for ei, e := range ev {
			if e.Kind != "late" && e.Kind != "ahead" && e.Kind != "missing" {
				continue
			}
			// Attach events at or just inside the run boundaries; the
			// "missing" events are exactly the run itself.
			if e.FrameIndex >= lo-1 && e.FrameIndex <= hi {
				runs[ri].Evidence = append(runs[ri].Evidence, e)
				used[ei] = true
			}
		}
	}
	rep.MissingRuns = runs

	for ei, e := range ev {
		if !used[ei] {
			rep.ExtraEvidence = append(rep.ExtraEvidence, e)
		}
	}
	return rep
}

func toEvidence(e dropEvidence) Evidence {
	return Evidence{
		At: e.At, Kind: e.Kind, FrameIndex: e.FrameIndex,
		Seq: e.Seq, RawSeq: e.RawSeq, Timestamp: e.Timestamp, RawTS: e.RawTS,
	}
}

// Evidence returns the recorded packet/frame evidence for a generation.
func (r *Receiver) Evidence(generation uint64) []Evidence {
	raw := r.evidenceFor(generation)
	out := make([]Evidence, 0, len(raw))
	for _, e := range raw {
		out = append(out, toEvidence(e))
	}
	return out
}

func ssrcHex(ssrc uint32) string {
	const hex = "0123456789abcdef"
	out := make([]byte, 10)
	out[0], out[1] = '0', 'x'
	for i := 0; i < 8; i++ {
		out[2+i] = hex[byte(ssrc>>uint(28-4*i))&0xf]
	}
	return string(out)
}

// WriteMissingReportFile writes a report as indented JSON.
func WriteMissingReportFile(path string, rep MissingReport) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	if err := enc.Encode(rep); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}
