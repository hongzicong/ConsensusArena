package fastpaxos

// Message factories, wire encoding, and wire decoding.
import (
	"encoding/binary"
	"fmt"
	"io"

	fastrpc "github.com/hongzicong/ConsensusArena/rpc"
	"github.com/hongzicong/ConsensusArena/state"
)

// Versioned bounded frames. Fast votes carry IDs; classic messages carry data.
type wireMessage struct{ message }

func (*wireMessage) New() fastrpc.Serializable { return &wireMessage{} }

func (m *wireMessage) Marshal(w io.Writer) {
	var h [31]byte
	h[0] = 1
	h[1] = m.Kind
	h[2] = byte(m.From)
	binary.LittleEndian.PutUint64(h[3:11], m.Epoch)
	for i, v := range []int{m.Start, m.High, m.Page, m.Pages, len(m.Entries)} {
		binary.LittleEndian.PutUint32(h[11+4*i:15+4*i], uint32(int32(v)))
	}
	w.Write(h[:])
	for _, e := range m.Entries {
		var b [29]byte
		binary.LittleEndian.PutUint32(b[:4], uint32(e.Slot))
		binary.LittleEndian.PutUint64(b[4:12], e.Epoch)
		binary.LittleEndian.PutUint64(b[12:20], e.Round)
		binary.LittleEndian.PutUint32(b[20:24], uint32(e.Value.ID.ClientId))
		binary.LittleEndian.PutUint32(b[24:28], uint32(e.Value.ID.SeqNum))
		if e.Payload {
			b[28] |= 1
		}
		if e.Decided {
			b[28] |= 2
		}
		if e.Value.Noop {
			b[28] |= 4
		}
		w.Write(b[:])
		if e.Payload && !e.Value.Noop {
			var p [13]byte
			p[0] = byte(e.Value.Command.Op)
			binary.LittleEndian.PutUint64(p[1:9], uint64(e.Value.Command.K))
			binary.LittleEndian.PutUint32(p[9:13], uint32(len(e.Value.Command.V)))
			w.Write(p[:])
			w.Write(e.Value.Command.V)
		}
	}
}

func (m *wireMessage) Unmarshal(r io.Reader) error {
	var h [31]byte
	if _, err := io.ReadFull(r, h[:]); err != nil {
		return err
	}
	if h[0] != 1 || h[1] > msgRecover {
		return fmt.Errorf("unsupported FastPaxos frame")
	}
	m.Kind = h[1]
	m.From = int(h[2])
	m.Epoch = binary.LittleEndian.Uint64(h[3:11])
	fields := []*int{&m.Start, &m.High, &m.Page, &m.Pages}
	for i, p := range fields {
		*p = int(int32(binary.LittleEndian.Uint32(h[11+4*i : 15+4*i])))
	}
	n := binary.LittleEndian.Uint32(h[27:31])
	if n > pageSize {
		return fmt.Errorf("oversized FastPaxos batch")
	}
	m.Entries = make([]entry, int(n))
	total := 0
	for i := range m.Entries {
		var b [29]byte
		if _, err := io.ReadFull(r, b[:]); err != nil {
			return err
		}
		e := &m.Entries[i]
		e.Slot = int(int32(binary.LittleEndian.Uint32(b[:4])))
		if e.Slot < 0 {
			return fmt.Errorf("negative FastPaxos slot")
		}
		e.Epoch = binary.LittleEndian.Uint64(b[4:12])
		e.Round = binary.LittleEndian.Uint64(b[12:20])
		e.Value.ID = CommandId{int32(binary.LittleEndian.Uint32(b[20:24])), int32(binary.LittleEndian.Uint32(b[24:28]))}
		e.Payload = b[28]&1 != 0
		e.Decided = b[28]&2 != 0
		e.Value.Noop = b[28]&4 != 0
		if e.Payload && !e.Value.Noop {
			var p [13]byte
			if _, err := io.ReadFull(r, p[:]); err != nil {
				return err
			}
			e.Value.Command.Op = state.Operation(p[0])
			e.Value.Command.K = state.Key(binary.LittleEndian.Uint64(p[1:9]))
			size := binary.LittleEndian.Uint32(p[9:13])
			total += int(size)
			if e.Value.Command.Op > state.SCAN || size > 16<<20 || total > 32<<20 {
				return fmt.Errorf("invalid FastPaxos payload")
			}
			e.Value.Command.V = make(state.Value, int(size))
			if _, err := io.ReadFull(r, e.Value.Command.V); err != nil {
				return err
			}
		}
	}
	return nil
}
