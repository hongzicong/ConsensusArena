package bodega

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/hongzicong/ConsensusArena/replica/defs"
	"github.com/hongzicong/ConsensusArena/rpc"
	"github.com/hongzicong/ConsensusArena/state"
)

const maxFrame = 64 << 20
const compactCommitFlag uint32 = 1 << 31
const compactCommitSize = 33
const wireVersion = 2

type encoder struct{ b []byte }

func (e *encoder) u8(v byte) { e.b = append(e.b, v) }
func (e *encoder) u32(v uint32) {
	var b [4]byte
	binary.LittleEndian.PutUint32(b[:], v)
	e.b = append(e.b, b[:]...)
}
func (e *encoder) u64(v uint64) {
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], v)
	e.b = append(e.b, b[:]...)
}
func (e *encoder) value(v []byte) {
	if v == nil {
		e.u32(^uint32(0))
		return
	}
	e.u32(uint32(len(v)))
	e.b = append(e.b, v...)
}
func (e *encoder) request(r request) {
	e.u8(byte(r.Origin + 1))
	e.u32(uint32(r.Proposal.ClientId))
	e.u32(uint32(r.Proposal.CommandId))
	e.u64(uint64(r.Proposal.Timestamp))
	e.u8(byte(r.Proposal.Command.Op))
	e.u64(uint64(r.Proposal.Command.K))
	e.value(r.Proposal.Command.V)
}
func (e *encoder) entry(v entry) {
	e.u64(v.Slot)
	e.u64(v.Ballot)
	if v.ReadFresh {
		e.u8(1)
	} else {
		e.u8(0)
	}
	rs := v.requests()
	e.u32(uint32(len(rs)))
	for _, r := range rs {
		e.request(r)
	}
}

func (*message) New() rpc.Serializable { return &message{} }
func (m *message) Marshal(w io.Writer) {
	if m.Kind == commit {
		var frame [4 + compactCommitSize]byte
		binary.LittleEndian.PutUint32(frame[:4], compactCommitFlag|compactCommitSize)
		frame[4] = byte(m.From)
		for i, value := range []uint64{m.Roster.Ballot, m.CommitPrefix, m.CommitSlot, m.ReadPrefix} {
			binary.LittleEndian.PutUint64(frame[5+8*i:], value)
		}
		_, _ = w.Write(frame[:])
		return
	}
	e := encoder{b: make([]byte, 4, 128)}
	e.u8(wireVersion)
	e.u8(byte(m.Kind))
	e.u8(byte(m.From))
	e.u64(m.Roster.Ballot)
	e.u64(m.ReadPrefix)
	switch m.Kind {
	case heartbeat:
		e.u8(byte(m.Roster.Leader))
		e.u64(m.Roster.Responders)
		e.u32(uint32(len(m.Roster.Ranges)))
		for _, span := range m.Roster.Ranges {
			e.u64(uint64(span.Start))
			e.u64(uint64(span.End))
			e.u64(span.Responders)
		}
		e.u64(m.Prefix)
		e.u64(m.CommitPrefix)
	case leaseRequest:
		e.u64(m.Sequence)
	case leaseReply:
		e.u64(m.Sequence)
		e.u64(m.Threshold)
	case prepare:
	case promise:
		e.u32(uint32(m.Part))
		e.u32(uint32(m.Parts))
		e.u32(uint32(len(m.Entries)))
		for _, v := range m.Entries {
			e.entry(v)
		}
	case accept:
		e.entry(m.Entry)
	case accepted:
		e.u64(m.Entry.Slot)
		e.u64(m.Prefix)
	case forward:
		e.request(m.Request)
	case result:
		// Replies need identity/ingress only; never echo a PUT payload.
		e.u8(byte(m.Request.Origin + 1))
		e.u32(uint32(m.Request.Proposal.ClientId))
		e.u32(uint32(m.Request.Proposal.CommandId))
		e.value(m.Value)
	case repairRequest:
		e.u64(m.RepairStart)
	default:
		panic("bodega: invalid outgoing message kind")
	}
	if len(e.b)-4 > maxFrame {
		panic("bodega: frame exceeds 64 MiB")
	}
	binary.LittleEndian.PutUint32(e.b[:4], uint32(len(e.b)-4))
	_, _ = w.Write(e.b)
}

type decoder struct {
	b   []byte
	err error
}

func (d *decoder) take(n int) []byte {
	if d.err != nil || n < 0 || n > len(d.b) {
		d.err = io.ErrUnexpectedEOF
		return nil
	}
	b := d.b[:n]
	d.b = d.b[n:]
	return b
}
func (d *decoder) u8() byte {
	if b := d.take(1); b != nil {
		return b[0]
	}
	return 0
}
func (d *decoder) u32() uint32 {
	if b := d.take(4); b != nil {
		return binary.LittleEndian.Uint32(b)
	}
	return 0
}
func (d *decoder) u64() uint64 {
	if b := d.take(8); b != nil {
		return binary.LittleEndian.Uint64(b)
	}
	return 0
}
func (d *decoder) count(minBytes int) int {
	n := uint64(d.u32())
	if n > uint64(len(d.b)/minBytes) {
		d.err = fmt.Errorf("bodega: invalid item count")
		return 0
	}
	return int(n)
}
func (d *decoder) value() []byte {
	n := d.u32()
	if n == ^uint32(0) {
		return nil
	}
	return d.take(int(n))
}
func (d *decoder) request() request {
	r := request{Origin: int(d.u8()) - 1}
	r.Proposal.ClientId = int32(d.u32())
	r.Proposal.CommandId = int32(d.u32())
	r.Proposal.Timestamp = int64(d.u64())
	r.Proposal.Command.Op = state.Operation(d.u8())
	r.Proposal.Command.K = state.Key(d.u64())
	r.Proposal.Command.V = d.value()
	if r.Origin < -1 || r.Origin >= 63 || r.Proposal.Command.Op > state.SCAN {
		d.err = fmt.Errorf("bodega: invalid request")
	}
	return r
}
func (d *decoder) entry() entry {
	v := entry{Slot: d.u64(), Ballot: d.u64()}
	fresh := d.u8()
	v.ReadFresh = fresh == 1
	n := d.count(30)
	if fresh > 1 || n == 0 || n > maxBatchCommands {
		d.err = fmt.Errorf("bodega: invalid batch")
		return v
	}
	if n == 1 {
		v.Request = d.request()
	} else {
		v.Batch = make([]request, n)
		for i := range v.Batch {
			v.Batch[i] = d.request()
		}
	}
	return v
}
func (m *message) Unmarshal(r io.Reader) error {
	var size [4]byte
	if _, err := io.ReadFull(r, size[:]); err != nil {
		return err
	}
	n := binary.LittleEndian.Uint32(size[:])
	if n&compactCommitFlag != 0 {
		if n&^compactCommitFlag != compactCommitSize {
			return fmt.Errorf("bodega: invalid compact commit size")
		}
		var b [compactCommitSize]byte
		if _, err := io.ReadFull(r, b[:]); err != nil {
			return err
		}
		*m = message{Kind: commit, From: int(b[0])}
		m.Roster.Ballot = binary.LittleEndian.Uint64(b[1:])
		m.CommitPrefix = binary.LittleEndian.Uint64(b[9:])
		m.CommitSlot = binary.LittleEndian.Uint64(b[17:])
		m.ReadPrefix = binary.LittleEndian.Uint64(b[25:])
		if m.From >= 63 {
			return fmt.Errorf("bodega: invalid sender")
		}
		return nil
	}
	if n == 0 || n > maxFrame {
		return fmt.Errorf("bodega: invalid frame size %d", n)
	}
	b := make([]byte, n)
	if _, err := io.ReadFull(r, b); err != nil {
		return err
	}
	d := decoder{b: b}
	if d.u8() != wireVersion {
		return fmt.Errorf("bodega: unsupported wire version")
	}
	*m = message{Kind: kind(d.u8()), From: int(d.u8())}
	m.Roster.Ballot = d.u64()
	m.ReadPrefix = d.u64()
	switch m.Kind {
	case heartbeat:
		m.Roster.Leader = int(d.u8())
		m.Roster.Responders = d.u64()
		count := d.count(24)
		if count > 0 {
			m.Roster.Ranges = make([]defs.BodegaResponderRange, count)
		}
		for i := range m.Roster.Ranges {
			m.Roster.Ranges[i] = defs.BodegaResponderRange{Start: int64(d.u64()), End: int64(d.u64()), Responders: d.u64()}
		}
		m.Prefix = d.u64()
		m.CommitPrefix = d.u64()
	case leaseRequest:
		m.Sequence = d.u64()
	case leaseReply:
		m.Sequence = d.u64()
		m.Threshold = d.u64()
	case prepare:
	case promise:
		m.Part = int(d.u32())
		m.Parts = int(d.u32())
		count := d.count(51)
		if count > 0 {
			m.Entries = make([]entry, count)
		}
		for i := range m.Entries {
			m.Entries[i] = d.entry()
		}
		if m.Parts < 1 || m.Part >= m.Parts {
			d.err = fmt.Errorf("bodega: invalid snapshot part")
		}
	case accept:
		m.Entry = d.entry()
	case accepted:
		m.Entry = entry{Slot: d.u64(), Ballot: m.Roster.Ballot}
		m.Prefix = d.u64()
	case forward:
		m.Request = d.request()
	case result:
		m.Request.Origin = int(d.u8()) - 1
		m.Request.Proposal.ClientId = int32(d.u32())
		m.Request.Proposal.CommandId = int32(d.u32())
		m.Value = d.value()
	case repairRequest:
		m.RepairStart = d.u64()
	default:
		return fmt.Errorf("bodega: invalid message kind")
	}
	if d.err != nil {
		return d.err
	}
	if len(d.b) != 0 || m.From >= 63 {
		return fmt.Errorf("bodega: trailing payload or invalid sender")
	}
	return nil
}
