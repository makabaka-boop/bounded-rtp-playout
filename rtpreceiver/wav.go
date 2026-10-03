package rtpreceiver

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
)

// WriteWAV writes the given frames as a 8000 Hz, mono, 16-bit PCM WAV file.
// Frames are written in slice order, which must be emission order. Zero
// filled frames are included verbatim, so the file duration reflects the
// actual output record.
func WriteWAV(w io.Writer, frames []Frame) error {
	samples := len(frames) * SamplesPerPacket
	dataBytes := samples * 2
	riffLen := 36 + dataBytes

	hdr := make([]byte, 44)
	copy(hdr[0:4], "RIFF")
	binary.LittleEndian.PutUint32(hdr[4:8], uint32(riffLen))
	copy(hdr[8:12], "WAVE")
	copy(hdr[12:16], "fmt ")
	binary.LittleEndian.PutUint32(hdr[16:20], 16) // PCM fmt chunk size
	binary.LittleEndian.PutUint16(hdr[20:22], 1)  // PCM
	binary.LittleEndian.PutUint16(hdr[22:24], 1)  // mono
	binary.LittleEndian.PutUint32(hdr[24:28], ClockRateHz)
	binary.LittleEndian.PutUint32(hdr[28:32], ClockRateHz*1*2) // byte rate
	binary.LittleEndian.PutUint16(hdr[32:34], 2)               // block align
	binary.LittleEndian.PutUint16(hdr[34:36], 16)              // bits per sample
	copy(hdr[36:40], "data")
	binary.LittleEndian.PutUint32(hdr[40:44], uint32(dataBytes))
	if _, err := w.Write(hdr); err != nil {
		return err
	}

	buf := make([]byte, 0, SamplesPerPacket*2)
	for _, f := range frames {
		buf = buf[:0]
		var tmp [2]byte
		for _, v := range f.Samples {
			binary.LittleEndian.PutUint16(tmp[:], uint16(v))
			buf = append(buf, tmp[0], tmp[1])
		}
		if _, err := w.Write(buf); err != nil {
			return err
		}
	}
	return nil
}

// WriteWAVFile creates/overrides path with the WAV encoding of frames.
func WriteWAVFile(path string, frames []Frame) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	if err := WriteWAV(f, frames); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("wav close: %w", err)
	}
	return nil
}
