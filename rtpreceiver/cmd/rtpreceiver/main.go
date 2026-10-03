// Command rtpreceiver receives the limited-profile RTP audio described by
// the rtpreceiver package over UDP and, on shutdown, writes one WAV file and
// one JSON missing report per capture generation to an output directory.
//
// Usage:
//
//	rtpreceiver -addr :5004 -out ./recordings
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"rtpreceiver"
)

func main() {
	addr := flag.String("addr", ":5004", "UDP listen address")
	outDir := flag.String("out", "./recordings", "directory for WAV and report files")
	window := flag.Int("window", 32, "per-source reorder window in packets")
	startDelay := flag.Duration("start-delay", 60*time.Millisecond, "first-frame delay after first packet")
	maxSources := flag.Int("max-sources", 0, "maximum distinct SSRCs (0 = unlimited)")
	flag.Parse()

	if err := os.MkdirAll(*outDir, 0o755); err != nil {
		log.Fatalf("create output dir: %v", err)
	}

	udpAddr, err := net.ResolveUDPAddr("udp4", *addr)
	if err != nil {
		log.Fatalf("resolve %q: %v", *addr, err)
	}
	conn, err := net.ListenUDP("udp4", udpAddr)
	if err != nil {
		log.Fatalf("listen %q: %v", *addr, err)
	}

	rec := rtpreceiver.NewRecorder()
	rcv := rtpreceiver.NewReceiver(rtpreceiver.Config{
		Window:     *window,
		StartDelay: *startDelay,
		MaxSources: *maxSources,
	}, rec)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	serveErr := make(chan error, 1)
	go func() {
		log.Printf("listening on %s", *addr)
		serveErr <- rcv.ServeUDP(conn)
	}()

	select {
	case <-ctx.Done():
		log.Printf("signal received, shutting down")
	case err := <-serveErr:
		if err != nil && !errors.Is(err, net.ErrClosed) {
			log.Printf("udp read error: %v", err)
		}
	}

	// Stop receiving and freeze every generation before reading records.
	conn.Close()
	rcv.Close()

	if err := flushOutputs(*outDir, rcv, rec); err != nil {
		log.Fatalf("flush outputs: %v", err)
	}
	log.Printf("invalid packets: %d", rcv.InvalidPackets())
}

// flushOutputs writes one WAV and one report per generation from the actual
// frames in the recorder.
func flushOutputs(dir string, rcv *rtpreceiver.Receiver, rec *rtpreceiver.Recorder) error {
	stats := rcv.StatsSnapshot()
	gens := rec.Generations()
	now := time.Now()
	for _, g := range gens {
		frames := rec.Frames(g)
		st, ok := stats[g]
		if !ok {
			return fmt.Errorf("no statistics for generation %d", g)
		}
		base := fmt.Sprintf("ssrc_%08x_gen%d", st.SSRC, g)

		wavPath := filepath.Join(dir, base+".wav")
		if err := rtpreceiver.WriteWAVFile(wavPath, frames); err != nil {
			return fmt.Errorf("write %s: %w", wavPath, err)
		}

		report := rtpreceiver.BuildMissingReport(frames, st, rcv.Evidence(g), now)
		repPath := filepath.Join(dir, base+"_missing.json")
		if err := rtpreceiver.WriteMissingReportFile(repPath, report); err != nil {
			return fmt.Errorf("write %s: %w", repPath, err)
		}
		log.Printf("generation %d (ssrc %08x): %d frames, %d missing -> %s, %s",
			g, st.SSRC, len(frames), st.FramesMissing,
			filepath.Base(wavPath), filepath.Base(repPath))
	}
	return nil
}
