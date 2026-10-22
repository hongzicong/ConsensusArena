package epaxos

// Message factories, wire encoding, and wire decoding.
import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"

	fastrpc "github.com/hongzicong/ConsensusArena/rpc"
	"github.com/hongzicong/ConsensusArena/state"
)

func (*repairRequest) New() fastrpc.Serializable { return &repairRequest{} }

func (m *repairRequest) Marshal(w io.Writer) {
	binary.Write(w, binary.LittleEndian, m.Owner)
	binary.Write(w, binary.LittleEndian, m.Slot)
}

func (m *repairRequest) Unmarshal(rd io.Reader) error {
	if err := binary.Read(rd, binary.LittleEndian, &m.Owner); err != nil {
		return err
	}
	return binary.Read(rd, binary.LittleEndian, &m.Slot)
}

func writeCommandBatch(w io.Writer, commands []state.Command) {
	binary.Write(w, binary.LittleEndian, uint32(len(commands)))
	for i := range commands {
		commands[i].Marshal(w)
	}
}

func readCommandBatch(rd io.Reader) ([]state.Command, error) {
	var n uint32
	if err := binary.Read(rd, binary.LittleEndian, &n); err != nil {
		return nil, err
	}
	if n > MAX_BATCH {
		return nil, fmt.Errorf("oversized EPaxos command batch")
	}
	cs := make([]state.Command, n)
	for i := range cs {
		if err := cs[i].Unmarshal(rd); err != nil {
			return nil, err
		}
	}
	return cs, nil
}

type byteReader interface {
	io.Reader
	ReadByte() (c byte, err error)
}

func (m *PrepareReply) New() fastrpc.Serializable {
	return new(PrepareReply)
}

func (m *PrepareReply) BinarySize() (nbytes int, sizeKnown bool) {
	return 0, false
}

func (m *PrepareReply) Marshal(wire io.Writer) {
	var b [18]byte
	var bs []byte
	bs = b[:17]
	tmp32 := m.AcceptorId
	bs[0] = byte(tmp32)
	bs[1] = byte(tmp32 >> 8)
	bs[2] = byte(tmp32 >> 16)
	bs[3] = byte(tmp32 >> 24)
	tmp32 = m.Replica
	bs[4] = byte(tmp32)
	bs[5] = byte(tmp32 >> 8)
	bs[6] = byte(tmp32 >> 16)
	bs[7] = byte(tmp32 >> 24)
	tmp32 = m.Instance
	bs[8] = byte(tmp32)
	bs[9] = byte(tmp32 >> 8)
	bs[10] = byte(tmp32 >> 16)
	bs[11] = byte(tmp32 >> 24)
	tmp32 = m.Ballot
	bs[12] = byte(tmp32)
	bs[13] = byte(tmp32 >> 8)
	bs[14] = byte(tmp32 >> 16)
	bs[15] = byte(tmp32 >> 24)
	bs[16] = byte(m.Status)
	wire.Write(bs)
	bs = b[:]
	alen1 := int64(len(m.Command))
	if wlen := binary.PutVarint(bs, alen1); wlen >= 0 {
		wire.Write(b[0:wlen])
	}
	for i := int64(0); i < alen1; i++ {
		m.Command[i].Marshal(wire)
	}
	bs = b[:4]
	tmp32 = m.Seq
	bs[0] = byte(tmp32)
	bs[1] = byte(tmp32 >> 8)
	bs[2] = byte(tmp32 >> 16)
	bs[3] = byte(tmp32 >> 24)
	wire.Write(bs)
	bs = b[:]
	alen2 := int64(len(m.Deps))
	if wlen := binary.PutVarint(bs, alen2); wlen >= 0 {
		wire.Write(b[0:wlen])
	}
	for i := int64(0); i < alen2; i++ {
		bs = b[:4]
		tmp32 = m.Deps[i]
		bs[0] = byte(tmp32)
		bs[1] = byte(tmp32 >> 8)
		bs[2] = byte(tmp32 >> 16)
		bs[3] = byte(tmp32 >> 24)
		wire.Write(bs)
	}
}

func (m *PrepareReply) Unmarshal(rr io.Reader) error {
	var wire byteReader
	var ok bool
	if wire, ok = rr.(byteReader); !ok {
		wire = bufio.NewReader(rr)
	}
	var b [17]byte
	var bs []byte
	bs = b[:17]
	if _, err := io.ReadAtLeast(wire, bs, 17); err != nil {
		return err
	}
	m.AcceptorId = int32((uint32(bs[0]) | (uint32(bs[1]) << 8) | (uint32(bs[2]) << 16) | (uint32(bs[3]) << 24)))
	m.Replica = int32((uint32(bs[4]) | (uint32(bs[5]) << 8) | (uint32(bs[6]) << 16) | (uint32(bs[7]) << 24)))
	m.Instance = int32((uint32(bs[8]) | (uint32(bs[9]) << 8) | (uint32(bs[10]) << 16) | (uint32(bs[11]) << 24)))
	m.Ballot = int32((uint32(bs[12]) | (uint32(bs[13]) << 8) | (uint32(bs[14]) << 16) | (uint32(bs[15]) << 24)))
	m.Status = int8(bs[16])
	alen1, err := binary.ReadVarint(wire)
	if err != nil {
		return err
	}
	m.Command = make([]state.Command, alen1)
	for i := int64(0); i < alen1; i++ {
		if err := m.Command[i].Unmarshal(wire); err != nil {
			return err
		}
	}
	bs = b[:4]
	if _, err := io.ReadAtLeast(wire, bs, 4); err != nil {
		return err
	}
	m.Seq = int32((uint32(bs[0]) | (uint32(bs[1]) << 8) | (uint32(bs[2]) << 16) | (uint32(bs[3]) << 24)))
	alen2, err := binary.ReadVarint(wire)
	if err != nil {
		return err
	}
	m.Deps = make([]int32, alen2)
	for i := int64(0); i < alen2; i++ {
		if _, err := io.ReadAtLeast(wire, bs, 4); err != nil {
			return err
		}
		m.Deps[i] = int32((uint32(bs[0]) | (uint32(bs[1]) << 8) | (uint32(bs[2]) << 16) | (uint32(bs[3]) << 24)))
	}
	return nil
}

func (m *PreAccept) New() fastrpc.Serializable {
	return new(PreAccept)
}

func (m *PreAccept) BinarySize() (nbytes int, sizeKnown bool) {
	return 0, false
}

func (m *PreAccept) Marshal(wire io.Writer) {
	var b [16]byte
	var bs []byte
	bs = b[:16]
	tmp32 := m.LeaderId
	bs[0] = byte(tmp32)
	bs[1] = byte(tmp32 >> 8)
	bs[2] = byte(tmp32 >> 16)
	bs[3] = byte(tmp32 >> 24)
	tmp32 = m.Replica
	bs[4] = byte(tmp32)
	bs[5] = byte(tmp32 >> 8)
	bs[6] = byte(tmp32 >> 16)
	bs[7] = byte(tmp32 >> 24)
	tmp32 = m.Instance
	bs[8] = byte(tmp32)
	bs[9] = byte(tmp32 >> 8)
	bs[10] = byte(tmp32 >> 16)
	bs[11] = byte(tmp32 >> 24)
	tmp32 = m.Ballot
	bs[12] = byte(tmp32)
	bs[13] = byte(tmp32 >> 8)
	bs[14] = byte(tmp32 >> 16)
	bs[15] = byte(tmp32 >> 24)
	wire.Write(bs)
	bs = b[:]
	alen1 := int64(len(m.Command))
	if wlen := binary.PutVarint(bs, alen1); wlen >= 0 {
		wire.Write(b[0:wlen])
	}
	for i := int64(0); i < alen1; i++ {
		m.Command[i].Marshal(wire)
	}
	bs = b[:4]
	tmp32 = m.Seq
	bs[0] = byte(tmp32)
	bs[1] = byte(tmp32 >> 8)
	bs[2] = byte(tmp32 >> 16)
	bs[3] = byte(tmp32 >> 24)
	wire.Write(bs)
	bs = b[:]
	alen2 := int64(len(m.Deps))
	if wlen := binary.PutVarint(bs, alen2); wlen >= 0 {
		wire.Write(b[0:wlen])
	}
	for i := int64(0); i < alen2; i++ {
		bs = b[:4]
		tmp32 = m.Deps[i]
		bs[0] = byte(tmp32)
		bs[1] = byte(tmp32 >> 8)
		bs[2] = byte(tmp32 >> 16)
		bs[3] = byte(tmp32 >> 24)
		wire.Write(bs)
	}
}

func (m *PreAccept) Unmarshal(rr io.Reader) error {
	var wire byteReader
	var ok bool
	if wire, ok = rr.(byteReader); !ok {
		wire = bufio.NewReader(rr)
	}
	var b [16]byte
	var bs []byte
	bs = b[:16]
	if _, err := io.ReadAtLeast(wire, bs, 16); err != nil {
		return err
	}
	m.LeaderId = int32((uint32(bs[0]) | (uint32(bs[1]) << 8) | (uint32(bs[2]) << 16) | (uint32(bs[3]) << 24)))
	m.Replica = int32((uint32(bs[4]) | (uint32(bs[5]) << 8) | (uint32(bs[6]) << 16) | (uint32(bs[7]) << 24)))
	m.Instance = int32((uint32(bs[8]) | (uint32(bs[9]) << 8) | (uint32(bs[10]) << 16) | (uint32(bs[11]) << 24)))
	m.Ballot = int32((uint32(bs[12]) | (uint32(bs[13]) << 8) | (uint32(bs[14]) << 16) | (uint32(bs[15]) << 24)))
	alen1, err := binary.ReadVarint(wire)
	if err != nil {
		return err
	}
	m.Command = make([]state.Command, alen1)
	for i := int64(0); i < alen1; i++ {
		if err := m.Command[i].Unmarshal(wire); err != nil {
			return err
		}
	}
	bs = b[:4]
	if _, err := io.ReadAtLeast(wire, bs, 4); err != nil {
		return err
	}
	m.Seq = int32((uint32(bs[0]) | (uint32(bs[1]) << 8) | (uint32(bs[2]) << 16) | (uint32(bs[3]) << 24)))
	alen2, err := binary.ReadVarint(wire)
	if err != nil {
		return err
	}
	m.Deps = make([]int32, alen2)
	for i := int64(0); i < alen2; i++ {
		if _, err := io.ReadAtLeast(wire, bs, 4); err != nil {
			return err
		}
		m.Deps[i] = int32((uint32(bs[0]) | (uint32(bs[1]) << 8) | (uint32(bs[2]) << 16) | (uint32(bs[3]) << 24)))
	}
	return nil
}

func (m *Accept) New() fastrpc.Serializable {
	return new(Accept)
}

func (m *Accept) BinarySize() (nbytes int, sizeKnown bool) {
	return 0, false
}

func (m *Accept) Marshal(wire io.Writer) {
	writeCommandBatch(wire, m.Command)
	var b [20]byte
	var bs []byte
	bs = b[:20]
	tmp32 := m.LeaderId
	bs[0] = byte(tmp32)
	bs[1] = byte(tmp32 >> 8)
	bs[2] = byte(tmp32 >> 16)
	bs[3] = byte(tmp32 >> 24)
	tmp32 = m.Replica
	bs[4] = byte(tmp32)
	bs[5] = byte(tmp32 >> 8)
	bs[6] = byte(tmp32 >> 16)
	bs[7] = byte(tmp32 >> 24)
	tmp32 = m.Instance
	bs[8] = byte(tmp32)
	bs[9] = byte(tmp32 >> 8)
	bs[10] = byte(tmp32 >> 16)
	bs[11] = byte(tmp32 >> 24)
	tmp32 = m.Ballot
	bs[12] = byte(tmp32)
	bs[13] = byte(tmp32 >> 8)
	bs[14] = byte(tmp32 >> 16)
	bs[15] = byte(tmp32 >> 24)
	tmp32 = m.Seq
	bs[16] = byte(tmp32)
	bs[17] = byte(tmp32 >> 8)
	bs[18] = byte(tmp32 >> 16)
	bs[19] = byte(tmp32 >> 24)
	wire.Write(bs)
	bs = b[:]
	alen1 := int64(len(m.Deps))
	if wlen := binary.PutVarint(bs, alen1); wlen >= 0 {
		wire.Write(b[0:wlen])
	}
	for i := int64(0); i < alen1; i++ {
		bs = b[:4]
		tmp32 = m.Deps[i]
		bs[0] = byte(tmp32)
		bs[1] = byte(tmp32 >> 8)
		bs[2] = byte(tmp32 >> 16)
		bs[3] = byte(tmp32 >> 24)
		wire.Write(bs)
	}
}

func (m *Accept) Unmarshal(rr io.Reader) error {
	var batchErr error
	m.Command, batchErr = readCommandBatch(rr)
	if batchErr != nil {
		return batchErr
	}
	var wire byteReader
	var ok bool
	if wire, ok = rr.(byteReader); !ok {
		wire = bufio.NewReader(rr)
	}
	var b [20]byte
	var bs []byte
	bs = b[:20]
	if _, err := io.ReadAtLeast(wire, bs, 20); err != nil {
		return err
	}
	m.LeaderId = int32((uint32(bs[0]) | (uint32(bs[1]) << 8) | (uint32(bs[2]) << 16) | (uint32(bs[3]) << 24)))
	m.Replica = int32((uint32(bs[4]) | (uint32(bs[5]) << 8) | (uint32(bs[6]) << 16) | (uint32(bs[7]) << 24)))
	m.Instance = int32((uint32(bs[8]) | (uint32(bs[9]) << 8) | (uint32(bs[10]) << 16) | (uint32(bs[11]) << 24)))
	m.Ballot = int32((uint32(bs[12]) | (uint32(bs[13]) << 8) | (uint32(bs[14]) << 16) | (uint32(bs[15]) << 24)))
	m.Seq = int32((uint32(bs[16]) | (uint32(bs[17]) << 8) | (uint32(bs[18]) << 16) | (uint32(bs[19]) << 24)))
	alen1, err := binary.ReadVarint(wire)
	if err != nil {
		return err
	}
	m.Deps = make([]int32, alen1)
	for i := int64(0); i < alen1; i++ {
		bs = b[:4]
		if _, err := io.ReadAtLeast(wire, bs, 4); err != nil {
			return err
		}
		m.Deps[i] = int32((uint32(bs[0]) | (uint32(bs[1]) << 8) | (uint32(bs[2]) << 16) | (uint32(bs[3]) << 24)))
	}
	return nil
}

func (m *AcceptReply) New() fastrpc.Serializable {
	return new(AcceptReply)
}

func (m *AcceptReply) BinarySize() (nbytes int, sizeKnown bool) {
	return 13, true
}

func (m *AcceptReply) Marshal(wire io.Writer) {
	_ = binary.Write(wire, binary.LittleEndian, m.AcceptorId)
	var b [12]byte
	var bs []byte
	bs = b[:12]
	tmp32 := m.Replica
	bs[0] = byte(tmp32)
	bs[1] = byte(tmp32 >> 8)
	bs[2] = byte(tmp32 >> 16)
	bs[3] = byte(tmp32 >> 24)
	tmp32 = m.Instance
	bs[4] = byte(tmp32)
	bs[5] = byte(tmp32 >> 8)
	bs[6] = byte(tmp32 >> 16)
	bs[7] = byte(tmp32 >> 24)
	tmp32 = m.Ballot
	bs[8] = byte(tmp32)
	bs[9] = byte(tmp32 >> 8)
	bs[10] = byte(tmp32 >> 16)
	bs[11] = byte(tmp32 >> 24)
	wire.Write(bs)
}

func (m *AcceptReply) Unmarshal(wire io.Reader) error {
	if err := binary.Read(wire, binary.LittleEndian, &m.AcceptorId); err != nil {
		return err
	}
	var b [12]byte
	var bs []byte
	bs = b[:12]
	if _, err := io.ReadAtLeast(wire, bs, 12); err != nil {
		return err
	}
	m.Replica = int32((uint32(bs[0]) | (uint32(bs[1]) << 8) | (uint32(bs[2]) << 16) | (uint32(bs[3]) << 24)))
	m.Instance = int32((uint32(bs[4]) | (uint32(bs[5]) << 8) | (uint32(bs[6]) << 16) | (uint32(bs[7]) << 24)))
	m.Ballot = int32((uint32(bs[8]) | (uint32(bs[9]) << 8) | (uint32(bs[10]) << 16) | (uint32(bs[11]) << 24)))
	return nil
}

func (m *TryPreAccept) New() fastrpc.Serializable {
	return new(TryPreAccept)
}

func (m *TryPreAccept) BinarySize() (nbytes int, sizeKnown bool) {
	return 0, false
}

func (m *TryPreAccept) Marshal(wire io.Writer) {
	var b [16]byte
	var bs []byte
	bs = b[:16]
	tmp32 := m.LeaderId
	bs[0] = byte(tmp32)
	bs[1] = byte(tmp32 >> 8)
	bs[2] = byte(tmp32 >> 16)
	bs[3] = byte(tmp32 >> 24)
	tmp32 = m.Replica
	bs[4] = byte(tmp32)
	bs[5] = byte(tmp32 >> 8)
	bs[6] = byte(tmp32 >> 16)
	bs[7] = byte(tmp32 >> 24)
	tmp32 = m.Instance
	bs[8] = byte(tmp32)
	bs[9] = byte(tmp32 >> 8)
	bs[10] = byte(tmp32 >> 16)
	bs[11] = byte(tmp32 >> 24)
	tmp32 = m.Ballot
	bs[12] = byte(tmp32)
	bs[13] = byte(tmp32 >> 8)
	bs[14] = byte(tmp32 >> 16)
	bs[15] = byte(tmp32 >> 24)
	wire.Write(bs)
	bs = b[:]
	alen1 := int64(len(m.Command))
	if wlen := binary.PutVarint(bs, alen1); wlen >= 0 {
		wire.Write(b[0:wlen])
	}
	for i := int64(0); i < alen1; i++ {
		m.Command[i].Marshal(wire)
	}
	bs = b[:4]
	tmp32 = m.Seq
	bs[0] = byte(tmp32)
	bs[1] = byte(tmp32 >> 8)
	bs[2] = byte(tmp32 >> 16)
	bs[3] = byte(tmp32 >> 24)
	wire.Write(bs)
	bs = b[:]
	alen2 := int64(len(m.Deps))
	if wlen := binary.PutVarint(bs, alen2); wlen >= 0 {
		wire.Write(b[0:wlen])
	}
	for i := int64(0); i < alen2; i++ {
		bs = b[:4]
		tmp32 = m.Deps[i]
		bs[0] = byte(tmp32)
		bs[1] = byte(tmp32 >> 8)
		bs[2] = byte(tmp32 >> 16)
		bs[3] = byte(tmp32 >> 24)
		wire.Write(bs)
	}
}

func (m *TryPreAccept) Unmarshal(rr io.Reader) error {
	var wire byteReader
	var ok bool
	if wire, ok = rr.(byteReader); !ok {
		wire = bufio.NewReader(rr)
	}
	var b [16]byte
	var bs []byte
	bs = b[:16]
	if _, err := io.ReadAtLeast(wire, bs, 16); err != nil {
		return err
	}
	m.LeaderId = int32((uint32(bs[0]) | (uint32(bs[1]) << 8) | (uint32(bs[2]) << 16) | (uint32(bs[3]) << 24)))
	m.Replica = int32((uint32(bs[4]) | (uint32(bs[5]) << 8) | (uint32(bs[6]) << 16) | (uint32(bs[7]) << 24)))
	m.Instance = int32((uint32(bs[8]) | (uint32(bs[9]) << 8) | (uint32(bs[10]) << 16) | (uint32(bs[11]) << 24)))
	m.Ballot = int32((uint32(bs[12]) | (uint32(bs[13]) << 8) | (uint32(bs[14]) << 16) | (uint32(bs[15]) << 24)))
	alen1, err := binary.ReadVarint(wire)
	if err != nil {
		return err
	}
	m.Command = make([]state.Command, alen1)
	for i := int64(0); i < alen1; i++ {
		if err := m.Command[i].Unmarshal(wire); err != nil {
			return err
		}
	}
	bs = b[:4]
	if _, err := io.ReadAtLeast(wire, bs, 4); err != nil {
		return err
	}
	m.Seq = int32((uint32(bs[0]) | (uint32(bs[1]) << 8) | (uint32(bs[2]) << 16) | (uint32(bs[3]) << 24)))
	alen2, err := binary.ReadVarint(wire)
	if err != nil {
		return err
	}
	m.Deps = make([]int32, alen2)
	for i := int64(0); i < alen2; i++ {
		if _, err := io.ReadAtLeast(wire, bs, 4); err != nil {
			return err
		}
		m.Deps[i] = int32((uint32(bs[0]) | (uint32(bs[1]) << 8) | (uint32(bs[2]) << 16) | (uint32(bs[3]) << 24)))
	}
	return nil
}

func (m *Prepare) New() fastrpc.Serializable {
	return new(Prepare)
}

func (m *Prepare) BinarySize() (nbytes int, sizeKnown bool) {
	return 16, true
}

func (m *Prepare) Marshal(wire io.Writer) {
	var b [16]byte
	var bs []byte
	bs = b[:16]
	tmp32 := m.LeaderId
	bs[0] = byte(tmp32)
	bs[1] = byte(tmp32 >> 8)
	bs[2] = byte(tmp32 >> 16)
	bs[3] = byte(tmp32 >> 24)
	tmp32 = m.Replica
	bs[4] = byte(tmp32)
	bs[5] = byte(tmp32 >> 8)
	bs[6] = byte(tmp32 >> 16)
	bs[7] = byte(tmp32 >> 24)
	tmp32 = m.Instance
	bs[8] = byte(tmp32)
	bs[9] = byte(tmp32 >> 8)
	bs[10] = byte(tmp32 >> 16)
	bs[11] = byte(tmp32 >> 24)
	tmp32 = m.Ballot
	bs[12] = byte(tmp32)
	bs[13] = byte(tmp32 >> 8)
	bs[14] = byte(tmp32 >> 16)
	bs[15] = byte(tmp32 >> 24)
	wire.Write(bs)
}

func (m *Prepare) Unmarshal(wire io.Reader) error {
	var b [16]byte
	var bs []byte
	bs = b[:16]
	if _, err := io.ReadAtLeast(wire, bs, 16); err != nil {
		return err
	}
	m.LeaderId = int32((uint32(bs[0]) | (uint32(bs[1]) << 8) | (uint32(bs[2]) << 16) | (uint32(bs[3]) << 24)))
	m.Replica = int32((uint32(bs[4]) | (uint32(bs[5]) << 8) | (uint32(bs[6]) << 16) | (uint32(bs[7]) << 24)))
	m.Instance = int32((uint32(bs[8]) | (uint32(bs[9]) << 8) | (uint32(bs[10]) << 16) | (uint32(bs[11]) << 24)))
	m.Ballot = int32((uint32(bs[12]) | (uint32(bs[13]) << 8) | (uint32(bs[14]) << 16) | (uint32(bs[15]) << 24)))
	return nil
}

func (m *PreAcceptReply) New() fastrpc.Serializable {
	return new(PreAcceptReply)
}

func (m *PreAcceptReply) BinarySize() (nbytes int, sizeKnown bool) {
	return 0, false
}

func (m *PreAcceptReply) Marshal(wire io.Writer) {
	_ = binary.Write(wire, binary.LittleEndian, m.AcceptorId)
	var b [21]byte
	var bs []byte
	bs = b[:21]
	tmp32 := m.Replica
	bs[0] = byte(tmp32)
	bs[1] = byte(tmp32 >> 8)
	bs[2] = byte(tmp32 >> 16)
	bs[3] = byte(tmp32 >> 24)
	tmp32 = m.Instance
	bs[4] = byte(tmp32)
	bs[5] = byte(tmp32 >> 8)
	bs[6] = byte(tmp32 >> 16)
	bs[7] = byte(tmp32 >> 24)
	tmp32 = m.Ballot
	bs[8] = byte(tmp32)
	bs[9] = byte(tmp32 >> 8)
	bs[10] = byte(tmp32 >> 16)
	bs[11] = byte(tmp32 >> 24)
	tmp32 = m.VBallot
	bs[12] = byte(tmp32)
	bs[13] = byte(tmp32 >> 8)
	bs[14] = byte(tmp32 >> 16)
	bs[15] = byte(tmp32 >> 24)
	tmp32 = m.Seq
	bs[16] = byte(tmp32)
	bs[17] = byte(tmp32 >> 8)
	bs[18] = byte(tmp32 >> 16)
	bs[19] = byte(tmp32 >> 24)
	bs[20] = byte(m.Status)
	wire.Write(bs)
	bs = b[:]
	alen1 := int64(len(m.Deps))
	if wlen := binary.PutVarint(bs, alen1); wlen >= 0 {
		wire.Write(b[0:wlen])
	}
	for i := int64(0); i < alen1; i++ {
		bs = b[:4]
		tmp32 = m.Deps[i]
		bs[0] = byte(tmp32)
		bs[1] = byte(tmp32 >> 8)
		bs[2] = byte(tmp32 >> 16)
		bs[3] = byte(tmp32 >> 24)
		wire.Write(bs)
	}
	bs = b[:]
	alen2 := int64(len(m.CommittedDeps))
	if wlen := binary.PutVarint(bs, alen2); wlen >= 0 {
		wire.Write(b[0:wlen])
	}
	for i := int64(0); i < alen2; i++ {
		bs = b[:4]
		tmp32 = m.CommittedDeps[i]
		bs[0] = byte(tmp32)
		bs[1] = byte(tmp32 >> 8)
		bs[2] = byte(tmp32 >> 16)
		bs[3] = byte(tmp32 >> 24)
		wire.Write(bs)
	}
}

func (m *PreAcceptReply) Unmarshal(rr io.Reader) error {
	if err := binary.Read(rr, binary.LittleEndian, &m.AcceptorId); err != nil {
		return err
	}
	var wire byteReader
	var ok bool
	if wire, ok = rr.(byteReader); !ok {
		wire = bufio.NewReader(rr)
	}
	var b [21]byte
	var bs []byte
	bs = b[:21]
	if _, err := io.ReadAtLeast(wire, bs, 21); err != nil {
		return err
	}
	m.Replica = int32((uint32(bs[0]) | (uint32(bs[1]) << 8) | (uint32(bs[2]) << 16) | (uint32(bs[3]) << 24)))
	m.Instance = int32((uint32(bs[4]) | (uint32(bs[5]) << 8) | (uint32(bs[6]) << 16) | (uint32(bs[7]) << 24)))
	m.Ballot = int32((uint32(bs[8]) | (uint32(bs[9]) << 8) | (uint32(bs[10]) << 16) | (uint32(bs[11]) << 24)))
	m.VBallot = int32((uint32(bs[12]) | (uint32(bs[13]) << 8) | (uint32(bs[14]) << 16) | (uint32(bs[15]) << 24)))
	m.Seq = int32((uint32(bs[16]) | (uint32(bs[17]) << 8) | (uint32(bs[18]) << 16) | (uint32(bs[19]) << 24)))
	m.Status = int8(bs[20])
	alen1, err := binary.ReadVarint(wire)
	if err != nil {
		return err
	}
	m.Deps = make([]int32, alen1)
	for i := int64(0); i < alen1; i++ {
		bs = b[:4]
		if _, err := io.ReadAtLeast(wire, bs, 4); err != nil {
			return err
		}
		m.Deps[i] = int32((uint32(bs[0]) | (uint32(bs[1]) << 8) | (uint32(bs[2]) << 16) | (uint32(bs[3]) << 24)))
	}
	alen2, err := binary.ReadVarint(wire)
	if err != nil {
		return err
	}
	m.CommittedDeps = make([]int32, alen2)
	for i := int64(0); i < alen2; i++ {
		if _, err := io.ReadAtLeast(wire, bs, 4); err != nil {
			return err
		}
		m.CommittedDeps[i] = int32((uint32(bs[0]) | (uint32(bs[1]) << 8) | (uint32(bs[2]) << 16) | (uint32(bs[3]) << 24)))
	}
	return nil
}

func (m *PreAcceptOK) New() fastrpc.Serializable {
	return new(PreAcceptOK)
}

func (m *PreAcceptOK) BinarySize() (nbytes int, sizeKnown bool) {
	return 4, true
}

func (m *PreAcceptOK) Marshal(wire io.Writer) {
	var b [4]byte
	var bs []byte
	bs = b[:4]
	tmp32 := m.Instance
	bs[0] = byte(tmp32)
	bs[1] = byte(tmp32 >> 8)
	bs[2] = byte(tmp32 >> 16)
	bs[3] = byte(tmp32 >> 24)
	wire.Write(bs)
}

func (m *PreAcceptOK) Unmarshal(wire io.Reader) error {
	var b [4]byte
	var bs []byte
	bs = b[:4]
	if _, err := io.ReadAtLeast(wire, bs, 4); err != nil {
		return err
	}
	m.Instance = int32((uint32(bs[0]) | (uint32(bs[1]) << 8) | (uint32(bs[2]) << 16) | (uint32(bs[3]) << 24)))
	return nil
}

func (m *Commit) New() fastrpc.Serializable {
	return new(Commit)
}

func (m *Commit) BinarySize() (nbytes int, sizeKnown bool) {
	return 0, false
}

func (m *Commit) Marshal(wire io.Writer) {
	var b [16]byte
	var bs []byte
	bs = b[:16]
	tmp32 := m.LeaderId
	bs[0] = byte(tmp32)
	bs[1] = byte(tmp32 >> 8)
	bs[2] = byte(tmp32 >> 16)
	bs[3] = byte(tmp32 >> 24)
	tmp32 = m.Replica
	bs[4] = byte(tmp32)
	bs[5] = byte(tmp32 >> 8)
	bs[6] = byte(tmp32 >> 16)
	bs[7] = byte(tmp32 >> 24)
	tmp32 = m.Instance
	bs[8] = byte(tmp32)
	bs[9] = byte(tmp32 >> 8)
	bs[10] = byte(tmp32 >> 16)
	bs[11] = byte(tmp32 >> 24)
	tmp32 = m.Ballot
	bs[12] = byte(tmp32)
	bs[13] = byte(tmp32 >> 8)
	bs[14] = byte(tmp32 >> 16)
	bs[15] = byte(tmp32 >> 24)
	wire.Write(bs)
	bs = b[:]
	alen1 := int64(len(m.Command))
	if wlen := binary.PutVarint(bs, alen1); wlen >= 0 {
		wire.Write(b[0:wlen])
	}
	for i := int64(0); i < alen1; i++ {
		m.Command[i].Marshal(wire)
	}
	bs = b[:4]
	tmp32 = m.Seq
	bs[0] = byte(tmp32)
	bs[1] = byte(tmp32 >> 8)
	bs[2] = byte(tmp32 >> 16)
	bs[3] = byte(tmp32 >> 24)
	wire.Write(bs)
	bs = b[:]
	alen2 := int64(len(m.Deps))
	if wlen := binary.PutVarint(bs, alen2); wlen >= 0 {
		wire.Write(b[0:wlen])
	}
	for i := int64(0); i < alen2; i++ {
		bs = b[:4]
		tmp32 = m.Deps[i]
		bs[0] = byte(tmp32)
		bs[1] = byte(tmp32 >> 8)
		bs[2] = byte(tmp32 >> 16)
		bs[3] = byte(tmp32 >> 24)
		wire.Write(bs)
	}
}

func (m *Commit) Unmarshal(rr io.Reader) error {
	var wire byteReader
	var ok bool
	if wire, ok = rr.(byteReader); !ok {
		wire = bufio.NewReader(rr)
	}
	var b [16]byte
	var bs []byte
	bs = b[:16]
	if _, err := io.ReadAtLeast(wire, bs, 16); err != nil {
		return err
	}
	m.LeaderId = int32((uint32(bs[0]) | (uint32(bs[1]) << 8) | (uint32(bs[2]) << 16) | (uint32(bs[3]) << 24)))
	m.Replica = int32((uint32(bs[4]) | (uint32(bs[5]) << 8) | (uint32(bs[6]) << 16) | (uint32(bs[7]) << 24)))
	m.Instance = int32((uint32(bs[8]) | (uint32(bs[9]) << 8) | (uint32(bs[10]) << 16) | (uint32(bs[11]) << 24)))
	m.Ballot = int32((uint32(bs[12]) | (uint32(bs[13]) << 8) | (uint32(bs[14]) << 16) | (uint32(bs[15]) << 24)))
	alen1, err := binary.ReadVarint(wire)
	if err != nil {
		return err
	}
	m.Command = make([]state.Command, alen1)
	for i := int64(0); i < alen1; i++ {
		if err := m.Command[i].Unmarshal(wire); err != nil {
			return err
		}
	}
	bs = b[:4]
	if _, err := io.ReadAtLeast(wire, bs, 4); err != nil {
		return err
	}
	m.Seq = int32((uint32(bs[0]) | (uint32(bs[1]) << 8) | (uint32(bs[2]) << 16) | (uint32(bs[3]) << 24)))
	alen2, err := binary.ReadVarint(wire)
	if err != nil {
		return err
	}
	m.Deps = make([]int32, alen2)
	for i := int64(0); i < alen2; i++ {
		if _, err := io.ReadAtLeast(wire, bs, 4); err != nil {
			return err
		}
		m.Deps[i] = int32((uint32(bs[0]) | (uint32(bs[1]) << 8) | (uint32(bs[2]) << 16) | (uint32(bs[3]) << 24)))
	}
	return nil
}

func (m *TryPreAcceptReply) New() fastrpc.Serializable {
	return new(TryPreAcceptReply)
}

func (m *TryPreAcceptReply) BinarySize() (nbytes int, sizeKnown bool) {
	return 26, true
}

func (m *TryPreAcceptReply) Marshal(wire io.Writer) {
	var b [29]byte
	var bs []byte
	bs = b[:29]
	tmp32 := m.AcceptorId
	bs[0] = byte(tmp32)
	bs[1] = byte(tmp32 >> 8)
	bs[2] = byte(tmp32 >> 16)
	bs[3] = byte(tmp32 >> 24)
	tmp32 = m.Replica
	bs[4] = byte(tmp32)
	bs[5] = byte(tmp32 >> 8)
	bs[6] = byte(tmp32 >> 16)
	bs[7] = byte(tmp32 >> 24)
	tmp32 = m.Instance
	bs[8] = byte(tmp32)
	bs[9] = byte(tmp32 >> 8)
	bs[10] = byte(tmp32 >> 16)
	bs[11] = byte(tmp32 >> 24)
	tmp32 = m.Ballot
	bs[12] = byte(tmp32)
	bs[13] = byte(tmp32 >> 8)
	bs[14] = byte(tmp32 >> 16)
	bs[15] = byte(tmp32 >> 24)
	tmp32 = m.VBallot
	bs[16] = byte(tmp32)
	bs[17] = byte(tmp32 >> 8)
	bs[18] = byte(tmp32 >> 16)
	bs[19] = byte(tmp32 >> 24)
	tmp32 = m.ConflictReplica
	bs[20] = byte(tmp32)
	bs[21] = byte(tmp32 >> 8)
	bs[22] = byte(tmp32 >> 16)
	bs[23] = byte(tmp32 >> 24)
	tmp32 = m.ConflictInstance
	bs[24] = byte(tmp32)
	bs[25] = byte(tmp32 >> 8)
	bs[26] = byte(tmp32 >> 16)
	bs[27] = byte(tmp32 >> 24)
	bs[28] = byte(m.ConflictStatus)
	wire.Write(bs)
}

func (m *TryPreAcceptReply) Unmarshal(wire io.Reader) error {
	var b [29]byte
	var bs []byte
	bs = b[:29]
	if _, err := io.ReadAtLeast(wire, bs, 29); err != nil {
		return err
	}
	m.AcceptorId = int32((uint32(bs[0]) | (uint32(bs[1]) << 8) | (uint32(bs[2]) << 16) | (uint32(bs[3]) << 24)))
	m.Replica = int32((uint32(bs[4]) | (uint32(bs[5]) << 8) | (uint32(bs[6]) << 16) | (uint32(bs[7]) << 24)))
	m.Instance = int32((uint32(bs[8]) | (uint32(bs[9]) << 8) | (uint32(bs[10]) << 16) | (uint32(bs[11]) << 24)))
	m.Ballot = int32((uint32(bs[12]) | (uint32(bs[13]) << 8) | (uint32(bs[14]) << 16) | (uint32(bs[15]) << 24)))
	m.VBallot = int32((uint32(bs[16]) | (uint32(bs[17]) << 8) | (uint32(bs[18]) << 16) | (uint32(bs[19]) << 24)))
	m.ConflictReplica = int32((uint32(bs[20]) | (uint32(bs[21]) << 8) | (uint32(bs[22]) << 16) | (uint32(bs[23]) << 24)))
	m.ConflictInstance = int32((uint32(bs[24]) | (uint32(bs[25]) << 8) | (uint32(bs[26]) << 16) | (uint32(bs[27]) << 24)))
	m.ConflictStatus = int8(bs[28])
	return nil
}
