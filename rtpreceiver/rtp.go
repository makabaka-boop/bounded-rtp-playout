package rtpreceiver

import (
	"encoding/binary"
	"errors"
)

// Constraints of the limited RTP profile this receiver accepts.
const (
	RTPVersion       = 2
	FixedHeaderLen   = 12
	PayloadType      = 96
	SamplesPerPacket = 160
	ClockRateHz      = 8000
	// Payload is 16-bit PCM, 160 samples per packet.
	PayloadLen   = SamplesPerPacket * 2
	MinPacketLen = FixedHeaderLen
	MaxPacketLen = FixedHeaderLen + PayloadLen
)

// Errors returned by ParsePacket. They are sentinel values so callers can
// attribute rejected datagrams precisely.
var (
	ErrShortPacket    = errors.New("rtp: packet shorter than the fixed header")
	ErrBadVersion     = errors.New("rtp: unsupported RTP version")
	ErrExtension      = errors.New("rtp: extension header is not supported")
	ErrCSRC           = errors.New("rtp: CSRC count must be zero")
	ErrBadPayloadType = errors.New("rtp: dynamic payload type is not 96")
	ErrBadMarker      = errors.New("rtp: marker bit must be zero")
	ErrBadLength      = errors.New("rtp: packet must carry exactly 160 PCM16 samples")
	ErrPadding        = errors.New("rtp: padding is not supported")
)

// Packet is a parsed, fixed-header RTP datagram carrying one 20 ms PCM16
// frame at 8 kHz.
type Packet struct {
	Raw       []byte
	Seq       uint16
	Timestamp uint32
	SSRC      uint32
	Samples   [SamplesPerPacket]int16
}

// ParsePacket validates the fixed 12-byte header and the 320-byte PCM16
// payload. On success the returned Packet owns a copy of the datagram so the
// caller may reuse the input buffer immediately.
func ParsePacket(b []byte) (*Packet, error) {
	if len(b) < FixedHeaderLen {
		return nil, ErrShortPacket
	}
	if b[0]>>6 != RTPVersion {
		return nil, ErrBadVersion
	}
	if b[0]&0x10 != 0 { // X: extension header bit
		return nil, ErrExtension
	}
	if b[0]&0x0f != 0 { // CC: CSRC count
		return nil, ErrCSRC
	}
	if b[0]&0x20 != 0 { // P: padding bit
		return nil, ErrPadding
	}
	if b[1]&0x80 != 0 { // M: marker bit
		return nil, ErrBadMarker
	}
	if b[1]&0x7f != PayloadType {
		return nil, ErrBadPayloadType
	}
	if len(b) != FixedHeaderLen+PayloadLen {
		return nil, ErrBadLength
	}

	p := &Packet{
		Raw:       make([]byte, len(b)),
		Seq:       binary.BigEndian.Uint16(b[2:4]),
		Timestamp: binary.BigEndian.Uint32(b[4:8]),
		SSRC:      binary.BigEndian.Uint32(b[8:12]),
	}
	copy(p.Raw, b)
	for i := 0; i < SamplesPerPacket; i++ {
		p.Samples[i] = int16(binary.BigEndian.Uint16(b[FixedHeaderLen+2*i:]))
	}
	return p, nil
}

// RFC 3550 Appendix A.1 sequence-number unwrap thresholds.
const (
	maxDropout  = 3000
	maxMisorder = 100
)

type seqKind int

const (
	// seqFirst is the very first packet of a capture generation.
	seqFirst seqKind = iota
	// seqForward is an in-order or forward-gap packet (gap < maxDropout),
	// including a sequence-number wraparound. It advances the frontier.
	seqForward
	// seqReorder is a packet arriving slightly late after a wrap
	// (within maxMisorder); it must not move the frontier.
	seqReorder
	// seqJump is a packet more than maxDropout ahead and more than
	// maxMisorder away in the backward direction; it is rejected.
	seqJump
)

// seqUnwrapper converts raw 16-bit RTP sequence numbers into monotonically
// increasing 64-bit extended values, following RFC 3550 Appendix A.1. The
// probation/source-validation machinery is omitted because every sender
// start opens a fresh capture generation with a reset state.
type seqUnwrapper struct {
	maxRaw uint16
	cycles uint64
	set    bool
}

// unwrap classifies a raw sequence number and returns its extended value.
// State is mutated only for seqForward packets that the caller subsequently
// admits; callers must invoke commit in that case and must not call it for
// packets rejected as beyond the receive window.
func (u *seqUnwrapper) unwrap(seq uint16) (extended uint64, kind seqKind) {
	if !u.set {
		return uint64(seq), seqFirst
	}
	udelta := seq - u.maxRaw
	switch {
	case udelta < maxDropout:
		cycles := u.cycles
		if seq < u.maxRaw {
			cycles += 1 << 16 // permissible forward wraparound
		}
		return cycles + uint64(seq), seqForward
	case udelta <= (1<<16)-maxMisorder:
		// Very large jump: reject, state untouched.
		return u.cycles + uint64(seq), seqJump
	default:
		// Duplicate or reordered packet within maxMisorder of the
		// frontier. If the frontier already crossed a wrap (maxRaw is
		// small) a raw value above maxRaw belongs to the previous
		// 16-bit cycle; a raw value below it sits behind the frontier
		// in the current cycle.
		if seq > u.maxRaw && u.cycles > 0 {
			return u.cycles - (1 << 16) + uint64(seq), seqReorder
		}
		return u.cycles + uint64(seq), seqReorder
	}
}

// commit records that a seqForward packet was admitted, moving the frontier.
func (u *seqUnwrapper) commit(seq uint16, extended uint64) {
	u.maxRaw = seq
	u.cycles = extended - uint64(seq)
	u.set = true
}

// reset starts a fresh unwrap cycle for a new capture generation.
func (u *seqUnwrapper) reset() {
	u.maxRaw = 0
	u.cycles = 0
	u.set = false
}

// ExtendTimestamp unwraps a 32-bit RTP timestamp into a monotonically
// increasing 64-bit value. expected is the sender's expected timestamp
// (already extended, derived from the extended sequence number). The signed
// 32-bit difference picks the correct wrap, matching the sender's modulo-2^32
// arithmetic; the high bits come from expected itself, so an arbitrary
// number of timestamp wraparounds (including the double-wraparound case in
// long streams) are handled without extra state.
func ExtendTimestamp(expected uint64, ts uint32) uint64 {
	delta := int64(int32(uint32(ts) - uint32(expected%(1<<32))))
	return uint64(int64(expected) + delta)
}
