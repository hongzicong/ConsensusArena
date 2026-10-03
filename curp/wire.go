package curp

// Message factories, wire encoding, and wire decoding.
import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"io"

	fastrpc "github.com/hongzicong/ConsensusArena/rpc"
)

func (m *MReply) New() fastrpc.Serializable {
	return new(MReply)
}

func (m *MAccept) New() fastrpc.Serializable {
	return new(MAccept)
}

func (m *MAcceptAck) New() fastrpc.Serializable {
	return new(MAcceptAck)
}

func (m *MAAcks) New() fastrpc.Serializable {
	return new(MAAcks)
}

func (m *MRecordAck) New() fastrpc.Serializable {
	return new(MRecordAck)
}

func (m *MCommit) New() fastrpc.Serializable {
	return new(MCommit)
}

func (m *MSync) New() fastrpc.Serializable {
	return new(MSync)
}

func (m *MSyncReply) New() fastrpc.Serializable {
	return new(MSyncReply)
}

type byteReader interface {
	io.Reader
	ReadByte() (c byte, err error)
}

func (t *MCommit) BinarySize() (nbytes int, sizeKnown bool) {
	return 16, true
}

func (t *MCommit) Marshal(wire io.Writer) {
	var b [16]byte
	var bs []byte
	bs = b[:16]
	tmp32 := t.Replica
	bs[0] = byte(tmp32)
	bs[1] = byte(tmp32 >> 8)
	bs[2] = byte(tmp32 >> 16)
	bs[3] = byte(tmp32 >> 24)
	tmp32 = t.Ballot
	bs[4] = byte(tmp32)
	bs[5] = byte(tmp32 >> 8)
	bs[6] = byte(tmp32 >> 16)
	bs[7] = byte(tmp32 >> 24)
	tmp64 := t.CmdSlot
	bs[8] = byte(tmp64)
	bs[9] = byte(tmp64 >> 8)
	bs[10] = byte(tmp64 >> 16)
	bs[11] = byte(tmp64 >> 24)
	bs[12] = byte(tmp64 >> 32)
	bs[13] = byte(tmp64 >> 40)
	bs[14] = byte(tmp64 >> 48)
	bs[15] = byte(tmp64 >> 56)
	wire.Write(bs)
}

func (t *MCommit) Unmarshal(wire io.Reader) error {
	var b [16]byte
	var bs []byte
	bs = b[:16]
	if _, err := io.ReadAtLeast(wire, bs, 16); err != nil {
		return err
	}
	t.Replica = int32((uint32(bs[0]) | (uint32(bs[1]) << 8) | (uint32(bs[2]) << 16) | (uint32(bs[3]) << 24)))
	t.Ballot = int32((uint32(bs[4]) | (uint32(bs[5]) << 8) | (uint32(bs[6]) << 16) | (uint32(bs[7]) << 24)))
	t.CmdSlot = int((uint64(bs[8]) | (uint64(bs[9]) << 8) | (uint64(bs[10]) << 16) | (uint64(bs[11]) << 24) | (uint64(bs[12]) << 32) | (uint64(bs[13]) << 40) | (uint64(bs[14]) << 48) | (uint64(bs[15]) << 56)))
	return nil
}

func (t *MSyncReply) BinarySize() (nbytes int, sizeKnown bool) {
	return 0, false
}

func (t *MSyncReply) Marshal(wire io.Writer) {
	var b [16]byte
	var bs []byte
	bs = b[:16]
	tmp32 := t.Replica
	bs[0] = byte(tmp32)
	bs[1] = byte(tmp32 >> 8)
	bs[2] = byte(tmp32 >> 16)
	bs[3] = byte(tmp32 >> 24)
	tmp32 = t.Ballot
	bs[4] = byte(tmp32)
	bs[5] = byte(tmp32 >> 8)
	bs[6] = byte(tmp32 >> 16)
	bs[7] = byte(tmp32 >> 24)
	tmp32 = t.CmdId.Client
	bs[8] = byte(tmp32)
	bs[9] = byte(tmp32 >> 8)
	bs[10] = byte(tmp32 >> 16)
	bs[11] = byte(tmp32 >> 24)
	tmp32 = t.CmdId.Sequence
	bs[12] = byte(tmp32)
	bs[13] = byte(tmp32 >> 8)
	bs[14] = byte(tmp32 >> 16)
	bs[15] = byte(tmp32 >> 24)
	wire.Write(bs)
	bs = b[:]
	alen1 := int64(len(t.Rep))
	if wlen := binary.PutVarint(bs, alen1); wlen >= 0 {
		wire.Write(b[0:wlen])
	}
	for i := int64(0); i < alen1; i++ {
		bs = b[:1]
		bs[0] = byte(t.Rep[i])
		wire.Write(bs)
	}
}

func (t *MSyncReply) Unmarshal(rr io.Reader) error {
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
	t.Replica = int32((uint32(bs[0]) | (uint32(bs[1]) << 8) | (uint32(bs[2]) << 16) | (uint32(bs[3]) << 24)))
	t.Ballot = int32((uint32(bs[4]) | (uint32(bs[5]) << 8) | (uint32(bs[6]) << 16) | (uint32(bs[7]) << 24)))
	t.CmdId.Client = int32((uint32(bs[8]) | (uint32(bs[9]) << 8) | (uint32(bs[10]) << 16) | (uint32(bs[11]) << 24)))
	t.CmdId.Sequence = int32((uint32(bs[12]) | (uint32(bs[13]) << 8) | (uint32(bs[14]) << 16) | (uint32(bs[15]) << 24)))
	alen1, err := binary.ReadVarint(wire)
	if err != nil {
		return err
	}
	t.Rep = make([]byte, alen1)
	for i := int64(0); i < alen1; i++ {
		bs = b[:1]
		if _, err := io.ReadAtLeast(wire, bs, 1); err != nil {
			return err
		}
		t.Rep[i] = byte(bs[0])
	}
	return nil
}

func (t *MAccept) BinarySize() (nbytes int, sizeKnown bool) {
	return 0, false
}

func (t *MAccept) Marshal(wire io.Writer) {
	var b [16]byte
	var bs []byte
	bs = b[:8]
	tmp32 := t.Replica
	bs[0] = byte(tmp32)
	bs[1] = byte(tmp32 >> 8)
	bs[2] = byte(tmp32 >> 16)
	bs[3] = byte(tmp32 >> 24)
	tmp32 = t.Ballot
	bs[4] = byte(tmp32)
	bs[5] = byte(tmp32 >> 8)
	bs[6] = byte(tmp32 >> 16)
	bs[7] = byte(tmp32 >> 24)
	wire.Write(bs)
	t.Cmd.Marshal(wire)
	bs = b[:16]
	tmp32 = t.CmdId.Client
	bs[0] = byte(tmp32)
	bs[1] = byte(tmp32 >> 8)
	bs[2] = byte(tmp32 >> 16)
	bs[3] = byte(tmp32 >> 24)
	tmp32 = t.CmdId.Sequence
	bs[4] = byte(tmp32)
	bs[5] = byte(tmp32 >> 8)
	bs[6] = byte(tmp32 >> 16)
	bs[7] = byte(tmp32 >> 24)
	tmp64 := t.CmdSlot
	bs[8] = byte(tmp64)
	bs[9] = byte(tmp64 >> 8)
	bs[10] = byte(tmp64 >> 16)
	bs[11] = byte(tmp64 >> 24)
	bs[12] = byte(tmp64 >> 32)
	bs[13] = byte(tmp64 >> 40)
	bs[14] = byte(tmp64 >> 48)
	bs[15] = byte(tmp64 >> 56)
	wire.Write(bs)
}

func (t *MAccept) Unmarshal(wire io.Reader) error {
	var b [16]byte
	var bs []byte
	bs = b[:8]
	if _, err := io.ReadAtLeast(wire, bs, 8); err != nil {
		return err
	}
	t.Replica = int32((uint32(bs[0]) | (uint32(bs[1]) << 8) | (uint32(bs[2]) << 16) | (uint32(bs[3]) << 24)))
	t.Ballot = int32((uint32(bs[4]) | (uint32(bs[5]) << 8) | (uint32(bs[6]) << 16) | (uint32(bs[7]) << 24)))
	t.Cmd.Unmarshal(wire)
	bs = b[:16]
	if _, err := io.ReadAtLeast(wire, bs, 16); err != nil {
		return err
	}
	t.CmdId.Client = int32((uint32(bs[0]) | (uint32(bs[1]) << 8) | (uint32(bs[2]) << 16) | (uint32(bs[3]) << 24)))
	t.CmdId.Sequence = int32((uint32(bs[4]) | (uint32(bs[5]) << 8) | (uint32(bs[6]) << 16) | (uint32(bs[7]) << 24)))
	t.CmdSlot = int((uint64(bs[8]) | (uint64(bs[9]) << 8) | (uint64(bs[10]) << 16) | (uint64(bs[11]) << 24) | (uint64(bs[12]) << 32) | (uint64(bs[13]) << 40) | (uint64(bs[14]) << 48) | (uint64(bs[15]) << 56)))
	return nil
}

func (t *MReply) BinarySize() (nbytes int, sizeKnown bool) {
	return 0, false
}

func (t *MReply) Marshal(wire io.Writer) {
	var b [16]byte
	var bs []byte
	bs = b[:16]
	tmp32 := t.Replica
	bs[0] = byte(tmp32)
	bs[1] = byte(tmp32 >> 8)
	bs[2] = byte(tmp32 >> 16)
	bs[3] = byte(tmp32 >> 24)
	tmp32 = t.Ballot
	bs[4] = byte(tmp32)
	bs[5] = byte(tmp32 >> 8)
	bs[6] = byte(tmp32 >> 16)
	bs[7] = byte(tmp32 >> 24)
	tmp32 = t.CmdId.Client
	bs[8] = byte(tmp32)
	bs[9] = byte(tmp32 >> 8)
	bs[10] = byte(tmp32 >> 16)
	bs[11] = byte(tmp32 >> 24)
	tmp32 = t.CmdId.Sequence
	bs[12] = byte(tmp32)
	bs[13] = byte(tmp32 >> 8)
	bs[14] = byte(tmp32 >> 16)
	bs[15] = byte(tmp32 >> 24)
	wire.Write(bs)
	bs = b[:]
	alen1 := int64(len(t.Rep))
	if wlen := binary.PutVarint(bs, alen1); wlen >= 0 {
		wire.Write(b[0:wlen])
	}
	for i := int64(0); i < alen1; i++ {
		bs = b[:1]
		bs[0] = byte(t.Rep[i])
		wire.Write(bs)
	}
	bs = b[:1]
	bs[0] = byte(t.Ok)
	wire.Write(bs)
}

func (t *MReply) Unmarshal(rr io.Reader) error {
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
	t.Replica = int32((uint32(bs[0]) | (uint32(bs[1]) << 8) | (uint32(bs[2]) << 16) | (uint32(bs[3]) << 24)))
	t.Ballot = int32((uint32(bs[4]) | (uint32(bs[5]) << 8) | (uint32(bs[6]) << 16) | (uint32(bs[7]) << 24)))
	t.CmdId.Client = int32((uint32(bs[8]) | (uint32(bs[9]) << 8) | (uint32(bs[10]) << 16) | (uint32(bs[11]) << 24)))
	t.CmdId.Sequence = int32((uint32(bs[12]) | (uint32(bs[13]) << 8) | (uint32(bs[14]) << 16) | (uint32(bs[15]) << 24)))
	alen1, err := binary.ReadVarint(wire)
	if err != nil {
		return err
	}
	if alen1 < 0 {
		return fmt.Errorf("invalid MReply payload length %d", alen1)
	}
	t.Rep = make([]byte, alen1)
	for i := int64(0); i < alen1; i++ {
		bs = b[:1]
		if _, err := io.ReadAtLeast(wire, bs, 1); err != nil {
			return err
		}
		t.Rep[i] = byte(bs[0])
	}
	bs = b[:1]
	if _, err := io.ReadFull(wire, bs); err != nil {
		return err
	}
	t.Ok = uint8(bs[0])
	return nil
}

func (t *MAcceptAck) BinarySize() (nbytes int, sizeKnown bool) {
	return 16, true
}

func (t *MAcceptAck) Marshal(wire io.Writer) {
	var b [16]byte
	var bs []byte
	bs = b[:16]
	tmp32 := t.Replica
	bs[0] = byte(tmp32)
	bs[1] = byte(tmp32 >> 8)
	bs[2] = byte(tmp32 >> 16)
	bs[3] = byte(tmp32 >> 24)
	tmp32 = t.Ballot
	bs[4] = byte(tmp32)
	bs[5] = byte(tmp32 >> 8)
	bs[6] = byte(tmp32 >> 16)
	bs[7] = byte(tmp32 >> 24)
	tmp64 := t.CmdSlot
	bs[8] = byte(tmp64)
	bs[9] = byte(tmp64 >> 8)
	bs[10] = byte(tmp64 >> 16)
	bs[11] = byte(tmp64 >> 24)
	bs[12] = byte(tmp64 >> 32)
	bs[13] = byte(tmp64 >> 40)
	bs[14] = byte(tmp64 >> 48)
	bs[15] = byte(tmp64 >> 56)
	wire.Write(bs)
}

func (t *MAcceptAck) Unmarshal(wire io.Reader) error {
	var b [16]byte
	var bs []byte
	bs = b[:16]
	if _, err := io.ReadAtLeast(wire, bs, 16); err != nil {
		return err
	}
	t.Replica = int32((uint32(bs[0]) | (uint32(bs[1]) << 8) | (uint32(bs[2]) << 16) | (uint32(bs[3]) << 24)))
	t.Ballot = int32((uint32(bs[4]) | (uint32(bs[5]) << 8) | (uint32(bs[6]) << 16) | (uint32(bs[7]) << 24)))
	t.CmdSlot = int((uint64(bs[8]) | (uint64(bs[9]) << 8) | (uint64(bs[10]) << 16) | (uint64(bs[11]) << 24) | (uint64(bs[12]) << 32) | (uint64(bs[13]) << 40) | (uint64(bs[14]) << 48) | (uint64(bs[15]) << 56)))
	return nil
}

func (t *MAAcks) BinarySize() (nbytes int, sizeKnown bool) {
	return 0, false
}

func (t *MAAcks) Marshal(wire io.Writer) {
	var b [10]byte
	var bs []byte
	bs = b[:]
	alen1 := int64(len(t.Acks))
	if wlen := binary.PutVarint(bs, alen1); wlen >= 0 {
		wire.Write(b[0:wlen])
	}
	for i := int64(0); i < alen1; i++ {
		bs = b[:4]
		tmp32 := t.Acks[i].Replica
		bs[0] = byte(tmp32)
		bs[1] = byte(tmp32 >> 8)
		bs[2] = byte(tmp32 >> 16)
		bs[3] = byte(tmp32 >> 24)
		wire.Write(bs)
		tmp32 = t.Acks[i].Ballot
		bs[0] = byte(tmp32)
		bs[1] = byte(tmp32 >> 8)
		bs[2] = byte(tmp32 >> 16)
		bs[3] = byte(tmp32 >> 24)
		wire.Write(bs)
		bs = b[:8]
		tmp64 := t.Acks[i].CmdSlot
		bs[0] = byte(tmp64)
		bs[1] = byte(tmp64 >> 8)
		bs[2] = byte(tmp64 >> 16)
		bs[3] = byte(tmp64 >> 24)
		bs[4] = byte(tmp64 >> 32)
		bs[5] = byte(tmp64 >> 40)
		bs[6] = byte(tmp64 >> 48)
		bs[7] = byte(tmp64 >> 56)
		wire.Write(bs)
	}
	bs = b[:]
	alen2 := int64(len(t.Accepts))
	if wlen := binary.PutVarint(bs, alen2); wlen >= 0 {
		wire.Write(b[0:wlen])
	}
	for i := int64(0); i < alen2; i++ {
		t.Accepts[i].Marshal(wire)
	}
}

func (t *MAAcks) Unmarshal(rr io.Reader) error {
	var wire byteReader
	var ok bool
	if wire, ok = rr.(byteReader); !ok {
		wire = bufio.NewReader(rr)
	}
	var b [10]byte
	var bs []byte
	alen1, err := binary.ReadVarint(wire)
	if err != nil {
		return err
	}
	t.Acks = make([]MAcceptAck, alen1)
	for i := int64(0); i < alen1; i++ {
		bs = b[:4]
		if _, err := io.ReadAtLeast(wire, bs, 4); err != nil {
			return err
		}
		t.Acks[i].Replica = int32((uint32(bs[0]) | (uint32(bs[1]) << 8) | (uint32(bs[2]) << 16) | (uint32(bs[3]) << 24)))
		if _, err := io.ReadAtLeast(wire, bs, 4); err != nil {
			return err
		}
		t.Acks[i].Ballot = int32((uint32(bs[0]) | (uint32(bs[1]) << 8) | (uint32(bs[2]) << 16) | (uint32(bs[3]) << 24)))
		bs = b[:8]
		if _, err := io.ReadAtLeast(wire, bs, 8); err != nil {
			return err
		}
		t.Acks[i].CmdSlot = int((uint64(bs[0]) | (uint64(bs[1]) << 8) | (uint64(bs[2]) << 16) | (uint64(bs[3]) << 24) | (uint64(bs[4]) << 32) | (uint64(bs[5]) << 40) | (uint64(bs[6]) << 48) | (uint64(bs[7]) << 56)))
	}
	alen2, err := binary.ReadVarint(wire)
	if err != nil {
		return err
	}
	t.Accepts = make([]MAccept, alen2)
	for i := int64(0); i < alen2; i++ {
		t.Accepts[i].Unmarshal(wire)
	}
	return nil
}

func (t *MRecordAck) BinarySize() (nbytes int, sizeKnown bool) {
	return 17, true
}

func (t *MRecordAck) Marshal(wire io.Writer) {
	var b [17]byte
	var bs []byte
	bs = b[:17]
	tmp32 := t.Replica
	bs[0] = byte(tmp32)
	bs[1] = byte(tmp32 >> 8)
	bs[2] = byte(tmp32 >> 16)
	bs[3] = byte(tmp32 >> 24)
	tmp32 = t.Ballot
	bs[4] = byte(tmp32)
	bs[5] = byte(tmp32 >> 8)
	bs[6] = byte(tmp32 >> 16)
	bs[7] = byte(tmp32 >> 24)
	tmp32 = t.CmdId.Client
	bs[8] = byte(tmp32)
	bs[9] = byte(tmp32 >> 8)
	bs[10] = byte(tmp32 >> 16)
	bs[11] = byte(tmp32 >> 24)
	tmp32 = t.CmdId.Sequence
	bs[12] = byte(tmp32)
	bs[13] = byte(tmp32 >> 8)
	bs[14] = byte(tmp32 >> 16)
	bs[15] = byte(tmp32 >> 24)
	bs[16] = byte(t.Ok)
	wire.Write(bs)
}

func (t *MRecordAck) Unmarshal(wire io.Reader) error {
	var b [17]byte
	var bs []byte
	bs = b[:17]
	if _, err := io.ReadAtLeast(wire, bs, 17); err != nil {
		return err
	}
	t.Replica = int32((uint32(bs[0]) | (uint32(bs[1]) << 8) | (uint32(bs[2]) << 16) | (uint32(bs[3]) << 24)))
	t.Ballot = int32((uint32(bs[4]) | (uint32(bs[5]) << 8) | (uint32(bs[6]) << 16) | (uint32(bs[7]) << 24)))
	t.CmdId.Client = int32((uint32(bs[8]) | (uint32(bs[9]) << 8) | (uint32(bs[10]) << 16) | (uint32(bs[11]) << 24)))
	t.CmdId.Sequence = int32((uint32(bs[12]) | (uint32(bs[13]) << 8) | (uint32(bs[14]) << 16) | (uint32(bs[15]) << 24)))
	t.Ok = uint8(bs[16])
	return nil
}

func (t *MSync) BinarySize() (nbytes int, sizeKnown bool) {
	return 8, true
}

func (t *MSync) Marshal(wire io.Writer) {
	var b [8]byte
	var bs []byte
	bs = b[:8]
	tmp32 := t.CmdId.Client
	bs[0] = byte(tmp32)
	bs[1] = byte(tmp32 >> 8)
	bs[2] = byte(tmp32 >> 16)
	bs[3] = byte(tmp32 >> 24)
	tmp32 = t.CmdId.Sequence
	bs[4] = byte(tmp32)
	bs[5] = byte(tmp32 >> 8)
	bs[6] = byte(tmp32 >> 16)
	bs[7] = byte(tmp32 >> 24)
	wire.Write(bs)
}

func (t *MSync) Unmarshal(wire io.Reader) error {
	var b [8]byte
	var bs []byte
	bs = b[:8]
	if _, err := io.ReadAtLeast(wire, bs, 8); err != nil {
		return err
	}
	t.CmdId.Client = int32((uint32(bs[0]) | (uint32(bs[1]) << 8) | (uint32(bs[2]) << 16) | (uint32(bs[3]) << 24)))
	t.CmdId.Sequence = int32((uint32(bs[4]) | (uint32(bs[5]) << 8) | (uint32(bs[6]) << 16) | (uint32(bs[7]) << 24)))
	return nil
}

func (*Packet) New() fastrpc.Serializable { return &Packet{} }

func (p *Packet) Marshal(w io.Writer) {
	var b bytes.Buffer
	if err := gob.NewEncoder(&b).Encode(p); err != nil {
		panic(err)
	}
	binary.Write(w, binary.LittleEndian, uint32(b.Len()))
	w.Write(b.Bytes())
}

func (p *Packet) Unmarshal(r io.Reader) error {
	var n uint32
	if err := binary.Read(r, binary.LittleEndian, &n); err != nil {
		return err
	}
	if n > 16<<20 {
		return fmt.Errorf("oversized recovery frame: %d", n)
	}
	b := make([]byte, n)
	if _, err := io.ReadFull(r, b); err != nil {
		return err
	}
	return gob.NewDecoder(bytes.NewReader(b)).Decode(p)
}
