package swift

// Message factories, wire encoding, and wire decoding.
// Integers remain little-endian (including 64-bit wire values for Go int),
// slice lengths remain signed varints, and MSync retains its gob encoding.
import (
	"bufio"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"io"
	"log"

	"github.com/hongzicong/ConsensusArena/replica/defs"
	fastrpc "github.com/hongzicong/ConsensusArena/rpc"
	"github.com/hongzicong/ConsensusArena/state"
)

// FastAck allocation and copying. Messages use ordinary Go allocation.
func newFastAck() *MFastAck {
	return &MFastAck{}
}

func copyFastAck(fa *MFastAck) *MFastAck {
	fa2 := newFastAck()
	fa2.Replica = fa.Replica
	fa2.Ballot = fa.Ballot
	fa2.CmdId = fa.CmdId
	fa2.Dep = fa.Dep
	fa2.Checksum = fa.Checksum
	fa2.Seqnum = fa.Seqnum
	return fa2
}

func (m *MFastAck) New() fastrpc.Serializable {
	return newFastAck()
}

func (m *MFastAckClient) New() fastrpc.Serializable {
	return new(MFastAckClient)
}

func (m *MSlowAck) New() fastrpc.Serializable {
	return new(MSlowAck)
}

func (m *MLightSlowAck) New() fastrpc.Serializable {
	return new(MLightSlowAck)
}

func (m *MAcks) New() fastrpc.Serializable {
	return new(MAcks)
}

func (m *MOptAcks) New() fastrpc.Serializable {
	return new(MOptAcks)
}

func (m *MReply) New() fastrpc.Serializable {
	return new(MReply)
}

func (m *MNewLeader) New() fastrpc.Serializable {
	return new(MNewLeader)
}

func (m *MNewLeaderAck) New() fastrpc.Serializable {
	return new(MNewLeaderAck)
}

func (m *MNewLeaderAckN) New() fastrpc.Serializable {
	return new(MNewLeaderAckN)
}

func (m *MShareState) New() fastrpc.Serializable {
	return new(MShareState)
}

func (m *MSync) New() fastrpc.Serializable {
	return new(MSync)
}

func (m *MSync) Marshal(w io.Writer) {
	e := gob.NewEncoder(w)

	err := e.Encode(m)
	if err != nil {
		log.Fatal("Don't know what to do")
	}
}

func (m *MSync) Unmarshal(r io.Reader) error {
	e := gob.NewDecoder(r)

	return e.Decode(m)
}

func (m *MLightSync) New() fastrpc.Serializable {
	return new(MLightSync)
}

func (m *MCollect) New() fastrpc.Serializable {
	return new(MCollect)
}

func (m *MAccept) New() fastrpc.Serializable {
	return new(MAccept)
}

func (m *MPing) New() fastrpc.Serializable {
	return new(MPing)
}

func (m *MPingRep) New() fastrpc.Serializable {
	return new(MPingRep)
}

type byteReader interface {
	io.Reader
	ReadByte() (c byte, err error)
}

func (t *SHash) BinarySize() (nbytes int, sizeKnown bool) {
	return 32, true
}

func (t *SHash) Marshal(wire io.Writer) {
	wire.Write(t.H[:])
}

func (t *SHash) Unmarshal(wire io.Reader) error {
	var hash [32]byte
	if _, err := io.ReadFull(wire, hash[:]); err != nil {
		return err
	}
	t.H = hash
	return nil
}

func (t *Ack) BinarySize() (nbytes int, sizeKnown bool) {
	return 0, false
}

func (t *Ack) Marshal(wire io.Writer) {
	var b [10]byte
	var bs []byte
	bs = b[:8]
	binary.LittleEndian.PutUint32(bs[0:], uint32(t.CmdId.Client))
	binary.LittleEndian.PutUint32(bs[4:], uint32(t.CmdId.Sequence))
	wire.Write(bs)
	writeCommandIds(wire, b[:], t.Dep)
	writeHashes(wire, b[:], t.Checksum)
	bs = b[:8]
	binary.LittleEndian.PutUint64(bs[0:], uint64(t.Seqnum))
	wire.Write(bs)
}

func (t *Ack) Unmarshal(rr io.Reader) error {
	wire := asByteReader(rr)
	var b [10]byte
	var bs []byte
	bs = b[:8]
	if _, err := io.ReadAtLeast(wire, bs, 8); err != nil {
		return err
	}
	t.CmdId.Client = int32(binary.LittleEndian.Uint32(bs[0:]))
	t.CmdId.Sequence = int32(binary.LittleEndian.Uint32(bs[4:]))
	dep, err := readCommandIds(wire, b[:])
	if err != nil {
		return err
	}
	t.Dep = dep
	checksum, err := readHashes(wire)
	if err != nil {
		return err
	}
	t.Checksum = checksum
	bs = b[:8]
	if _, err := io.ReadAtLeast(wire, bs, 8); err != nil {
		return err
	}
	t.Seqnum = int(binary.LittleEndian.Uint64(bs[0:]))
	return nil
}

func (t *MNewLeaderAck) BinarySize() (nbytes int, sizeKnown bool) {
	return 12, true
}

func (t *MNewLeaderAck) Marshal(wire io.Writer) {
	var b [12]byte
	var bs []byte
	bs = b[:12]
	binary.LittleEndian.PutUint32(bs[0:], uint32(t.Replica))
	binary.LittleEndian.PutUint32(bs[4:], uint32(t.Ballot))
	binary.LittleEndian.PutUint32(bs[8:], uint32(t.Cballot))
	wire.Write(bs)
}

func (t *MNewLeaderAck) Unmarshal(wire io.Reader) error {
	var b [12]byte
	var bs []byte
	bs = b[:12]
	if _, err := io.ReadAtLeast(wire, bs, 12); err != nil {
		return err
	}
	t.Replica = int32(binary.LittleEndian.Uint32(bs[0:]))
	t.Ballot = int32(binary.LittleEndian.Uint32(bs[4:]))
	t.Cballot = int32(binary.LittleEndian.Uint32(bs[8:]))
	return nil
}

func (t *MLightSync) BinarySize() (nbytes int, sizeKnown bool) {
	return 8, true
}

func (t *MLightSync) Marshal(wire io.Writer) {
	var b [8]byte
	var bs []byte
	bs = b[:8]
	binary.LittleEndian.PutUint32(bs[0:], uint32(t.Replica))
	binary.LittleEndian.PutUint32(bs[4:], uint32(t.Ballot))
	wire.Write(bs)
}

func (t *MLightSync) Unmarshal(wire io.Reader) error {
	var b [8]byte
	var bs []byte
	bs = b[:8]
	if _, err := io.ReadAtLeast(wire, bs, 8); err != nil {
		return err
	}
	t.Replica = int32(binary.LittleEndian.Uint32(bs[0:]))
	t.Ballot = int32(binary.LittleEndian.Uint32(bs[4:]))
	return nil
}

func (t *MFastAck) BinarySize() (nbytes int, sizeKnown bool) {
	return 0, false
}

func (t *MFastAck) Marshal(wire io.Writer) {
	var b [16]byte
	var bs []byte
	bs = b[:16]
	binary.LittleEndian.PutUint32(bs[0:], uint32(t.Replica))
	binary.LittleEndian.PutUint32(bs[4:], uint32(t.Ballot))
	binary.LittleEndian.PutUint32(bs[8:], uint32(t.CmdId.Client))
	binary.LittleEndian.PutUint32(bs[12:], uint32(t.CmdId.Sequence))
	wire.Write(bs)
	writeCommandIds(wire, b[:], t.Dep)
	writeHashes(wire, b[:], t.Checksum)
	bs = b[:8]
	binary.LittleEndian.PutUint64(bs[0:], uint64(t.Seqnum))
	wire.Write(bs)
}

func (t *MFastAck) Unmarshal(rr io.Reader) error {
	wire := asByteReader(rr)
	var b [16]byte
	var bs []byte
	bs = b[:16]
	if _, err := io.ReadAtLeast(wire, bs, 16); err != nil {
		return err
	}
	t.Replica = int32(binary.LittleEndian.Uint32(bs[0:]))
	t.Ballot = int32(binary.LittleEndian.Uint32(bs[4:]))
	t.CmdId.Client = int32(binary.LittleEndian.Uint32(bs[8:]))
	t.CmdId.Sequence = int32(binary.LittleEndian.Uint32(bs[12:]))
	dep, err := readCommandIds(wire, b[:])
	if err != nil {
		return err
	}
	t.Dep = dep
	checksum, err := readHashes(wire)
	if err != nil {
		return err
	}
	t.Checksum = checksum
	bs = b[:8]
	if _, err := io.ReadAtLeast(wire, bs, 8); err != nil {
		return err
	}
	t.Seqnum = int(binary.LittleEndian.Uint64(bs[0:]))
	return nil
}

func (t *MFastAckClient) BinarySize() (nbytes int, sizeKnown bool) {
	return 0, false
}

func (t *MFastAckClient) Marshal(wire io.Writer) {
	var b [16]byte
	var bs []byte
	bs = b[:16]
	binary.LittleEndian.PutUint32(bs[0:], uint32(t.Replica))
	binary.LittleEndian.PutUint32(bs[4:], uint32(t.Ballot))
	binary.LittleEndian.PutUint32(bs[8:], uint32(t.CmdId.Client))
	binary.LittleEndian.PutUint32(bs[12:], uint32(t.CmdId.Sequence))
	wire.Write(bs)
	writeHashes(wire, b[:], t.Checksum)
}

func (t *MFastAckClient) Unmarshal(rr io.Reader) error {
	wire := asByteReader(rr)
	var b [16]byte
	var bs []byte
	bs = b[:16]
	if _, err := io.ReadAtLeast(wire, bs, 16); err != nil {
		return err
	}
	t.Replica = int32(binary.LittleEndian.Uint32(bs[0:]))
	t.Ballot = int32(binary.LittleEndian.Uint32(bs[4:]))
	t.CmdId.Client = int32(binary.LittleEndian.Uint32(bs[8:]))
	t.CmdId.Sequence = int32(binary.LittleEndian.Uint32(bs[12:]))
	checksum, err := readHashes(wire)
	if err != nil {
		return err
	}
	t.Checksum = checksum
	return nil
}

func (t *MLightSlowAck) BinarySize() (nbytes int, sizeKnown bool) {
	return 16, true
}

func (t *MLightSlowAck) Marshal(wire io.Writer) {
	var b [16]byte
	var bs []byte
	bs = b[:16]
	binary.LittleEndian.PutUint32(bs[0:], uint32(t.Replica))
	binary.LittleEndian.PutUint32(bs[4:], uint32(t.Ballot))
	binary.LittleEndian.PutUint32(bs[8:], uint32(t.CmdId.Client))
	binary.LittleEndian.PutUint32(bs[12:], uint32(t.CmdId.Sequence))
	wire.Write(bs)
}

func (t *MLightSlowAck) Unmarshal(wire io.Reader) error {
	var b [16]byte
	var bs []byte
	bs = b[:16]
	if _, err := io.ReadAtLeast(wire, bs, 16); err != nil {
		return err
	}
	t.Replica = int32(binary.LittleEndian.Uint32(bs[0:]))
	t.Ballot = int32(binary.LittleEndian.Uint32(bs[4:]))
	t.CmdId.Client = int32(binary.LittleEndian.Uint32(bs[8:]))
	t.CmdId.Sequence = int32(binary.LittleEndian.Uint32(bs[12:]))
	return nil
}

func (t *MAccept) BinarySize() (nbytes int, sizeKnown bool) {
	return 0, false
}

func (t *MAccept) Marshal(wire io.Writer) {
	var b [16]byte
	var bs []byte
	bs = b[:16]
	binary.LittleEndian.PutUint32(bs[0:], uint32(t.Replica))
	binary.LittleEndian.PutUint32(bs[4:], uint32(t.Ballot))
	binary.LittleEndian.PutUint32(bs[8:], uint32(t.CmdId.Client))
	binary.LittleEndian.PutUint32(bs[12:], uint32(t.CmdId.Sequence))
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

func (t *MAccept) Unmarshal(rr io.Reader) error {
	wire := asByteReader(rr)
	var b [16]byte
	var bs []byte
	bs = b[:16]
	if _, err := io.ReadAtLeast(wire, bs, 16); err != nil {
		return err
	}
	t.Replica = int32(binary.LittleEndian.Uint32(bs[0:]))
	t.Ballot = int32(binary.LittleEndian.Uint32(bs[4:]))
	t.CmdId.Client = int32(binary.LittleEndian.Uint32(bs[8:]))
	t.CmdId.Sequence = int32(binary.LittleEndian.Uint32(bs[12:]))
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

func (t *MNewLeader) BinarySize() (nbytes int, sizeKnown bool) {
	return 8, true
}

func (t *MNewLeader) Marshal(wire io.Writer) {
	var b [8]byte
	var bs []byte
	bs = b[:8]
	binary.LittleEndian.PutUint32(bs[0:], uint32(t.Replica))
	binary.LittleEndian.PutUint32(bs[4:], uint32(t.Ballot))
	wire.Write(bs)
}

func (t *MNewLeader) Unmarshal(wire io.Reader) error {
	var b [8]byte
	var bs []byte
	bs = b[:8]
	if _, err := io.ReadAtLeast(wire, bs, 8); err != nil {
		return err
	}
	t.Replica = int32(binary.LittleEndian.Uint32(bs[0:]))
	t.Ballot = int32(binary.LittleEndian.Uint32(bs[4:]))
	return nil
}

func (t *SDep) BinarySize() (nbytes int, sizeKnown bool) {
	return 0, false
}

func (t *SDep) Marshal(wire io.Writer) {
	var b [10]byte
	writeCommandIds(wire, b[:], t.Dep)
}

func (t *SDep) Unmarshal(rr io.Reader) error {
	var b [8]byte
	wire := asByteReader(rr)
	dep, err := readCommandIds(wire, b[:])
	if err != nil {
		return err
	}
	t.Dep = dep
	return nil
}

func (t *MShareState) BinarySize() (nbytes int, sizeKnown bool) {
	return 8, true
}

func (t *MShareState) Marshal(wire io.Writer) {
	var b [8]byte
	var bs []byte
	bs = b[:8]
	binary.LittleEndian.PutUint32(bs[0:], uint32(t.Replica))
	binary.LittleEndian.PutUint32(bs[4:], uint32(t.Ballot))
	wire.Write(bs)
}

func (t *MShareState) Unmarshal(wire io.Reader) error {
	var b [8]byte
	var bs []byte
	bs = b[:8]
	if _, err := io.ReadAtLeast(wire, bs, 8); err != nil {
		return err
	}
	t.Replica = int32(binary.LittleEndian.Uint32(bs[0:]))
	t.Ballot = int32(binary.LittleEndian.Uint32(bs[4:]))
	return nil
}

func (t *MOptAcks) BinarySize() (nbytes int, sizeKnown bool) {
	return 0, false
}

func (t *MOptAcks) Marshal(wire io.Writer) {
	var b [10]byte
	var bs []byte
	bs = b[:8]
	binary.LittleEndian.PutUint32(bs[0:], uint32(t.Replica))
	binary.LittleEndian.PutUint32(bs[4:], uint32(t.Ballot))
	wire.Write(bs)
	bs = b[:]
	alen1 := int64(len(t.Acks))
	if wlen := binary.PutVarint(bs, alen1); wlen >= 0 {
		wire.Write(b[0:wlen])
	}
	for i := int64(0); i < alen1; i++ {
		t.Acks[i].Marshal(wire)
	}
}

func (t *MOptAcks) Unmarshal(rr io.Reader) error {
	wire := asByteReader(rr)
	var b [10]byte
	var bs []byte
	bs = b[:8]
	if _, err := io.ReadAtLeast(wire, bs, 8); err != nil {
		return err
	}
	t.Replica = int32(binary.LittleEndian.Uint32(bs[0:]))
	t.Ballot = int32(binary.LittleEndian.Uint32(bs[4:]))
	alen1, err := binary.ReadVarint(wire)
	if err != nil {
		return err
	}
	t.Acks = make([]Ack, alen1)
	for i := int64(0); i < alen1; i++ {
		t.Acks[i].Unmarshal(wire)
	}
	return nil
}

func (t *MPing) BinarySize() (nbytes int, sizeKnown bool) {
	return 8, true
}

func (t *MPing) Marshal(wire io.Writer) {
	var b [8]byte
	var bs []byte
	bs = b[:8]
	binary.LittleEndian.PutUint32(bs[0:], uint32(t.Replica))
	binary.LittleEndian.PutUint32(bs[4:], uint32(t.Ballot))
	wire.Write(bs)
}

func (t *MPing) Unmarshal(wire io.Reader) error {
	var b [8]byte
	var bs []byte
	bs = b[:8]
	if _, err := io.ReadAtLeast(wire, bs, 8); err != nil {
		return err
	}
	t.Replica = int32(binary.LittleEndian.Uint32(bs[0:]))
	t.Ballot = int32(binary.LittleEndian.Uint32(bs[4:]))
	return nil
}

func (t *MSlowAck) BinarySize() (nbytes int, sizeKnown bool) {
	return 0, false
}

func (t *MSlowAck) Marshal(wire io.Writer) {
	var b [16]byte
	var bs []byte
	bs = b[:16]
	binary.LittleEndian.PutUint32(bs[0:], uint32(t.Replica))
	binary.LittleEndian.PutUint32(bs[4:], uint32(t.Ballot))
	binary.LittleEndian.PutUint32(bs[8:], uint32(t.CmdId.Client))
	binary.LittleEndian.PutUint32(bs[12:], uint32(t.CmdId.Sequence))
	wire.Write(bs)
	writeCommandIds(wire, b[:], t.Dep)
	writeHashes(wire, b[:], t.Checksum)
}

func (t *MSlowAck) Unmarshal(rr io.Reader) error {
	wire := asByteReader(rr)
	var b [16]byte
	var bs []byte
	bs = b[:16]
	if _, err := io.ReadAtLeast(wire, bs, 16); err != nil {
		return err
	}
	t.Replica = int32(binary.LittleEndian.Uint32(bs[0:]))
	t.Ballot = int32(binary.LittleEndian.Uint32(bs[4:]))
	t.CmdId.Client = int32(binary.LittleEndian.Uint32(bs[8:]))
	t.CmdId.Sequence = int32(binary.LittleEndian.Uint32(bs[12:]))
	dep, err := readCommandIds(wire, b[:])
	if err != nil {
		return err
	}
	t.Dep = dep
	checksum, err := readHashes(wire)
	if err != nil {
		return err
	}
	t.Checksum = checksum
	return nil
}

func (t *MAcks) BinarySize() (nbytes int, sizeKnown bool) {
	return 0, false
}

func (t *MAcks) Marshal(wire io.Writer) {
	var b [10]byte
	var bs []byte
	bs = b[:]
	alen1 := int64(len(t.FastAcks))
	if wlen := binary.PutVarint(bs, alen1); wlen >= 0 {
		wire.Write(b[0:wlen])
	}
	for i := int64(0); i < alen1; i++ {
		t.FastAcks[i].Marshal(wire)
	}
	bs = b[:]
	alen2 := int64(len(t.LightSlowAcks))
	if wlen := binary.PutVarint(bs, alen2); wlen >= 0 {
		wire.Write(b[0:wlen])
	}
	for i := int64(0); i < alen2; i++ {
		bs = b[:4]
		binary.LittleEndian.PutUint32(bs[0:], uint32(t.LightSlowAcks[i].Replica))
		wire.Write(bs)
		binary.LittleEndian.PutUint32(bs[0:], uint32(t.LightSlowAcks[i].Ballot))
		wire.Write(bs)
		binary.LittleEndian.PutUint32(bs[0:], uint32(t.LightSlowAcks[i].CmdId.Client))
		wire.Write(bs)
		binary.LittleEndian.PutUint32(bs[0:], uint32(t.LightSlowAcks[i].CmdId.Sequence))
		wire.Write(bs)
	}
}

func (t *MAcks) Unmarshal(rr io.Reader) error {
	wire := asByteReader(rr)
	var b [10]byte
	var bs []byte
	alen1, err := binary.ReadVarint(wire)
	if err != nil {
		return err
	}
	t.FastAcks = make([]MFastAck, alen1)
	for i := int64(0); i < alen1; i++ {
		t.FastAcks[i].Unmarshal(wire)
	}
	alen2, err := binary.ReadVarint(wire)
	if err != nil {
		return err
	}
	t.LightSlowAcks = make([]MLightSlowAck, alen2)
	for i := int64(0); i < alen2; i++ {
		bs = b[:4]
		if _, err := io.ReadAtLeast(wire, bs, 4); err != nil {
			return err
		}
		t.LightSlowAcks[i].Replica = int32(binary.LittleEndian.Uint32(bs[0:]))
		if _, err := io.ReadAtLeast(wire, bs, 4); err != nil {
			return err
		}
		t.LightSlowAcks[i].Ballot = int32(binary.LittleEndian.Uint32(bs[0:]))
		if _, err := io.ReadAtLeast(wire, bs, 4); err != nil {
			return err
		}
		t.LightSlowAcks[i].CmdId.Client = int32(binary.LittleEndian.Uint32(bs[0:]))
		if _, err := io.ReadAtLeast(wire, bs, 4); err != nil {
			return err
		}
		t.LightSlowAcks[i].CmdId.Sequence = int32(binary.LittleEndian.Uint32(bs[0:]))
	}
	return nil
}

func (t *MReply) BinarySize() (nbytes int, sizeKnown bool) {
	return 0, false
}

func (t *MReply) Marshal(wire io.Writer) {
	var b [16]byte
	var bs []byte
	bs = b[:16]
	binary.LittleEndian.PutUint32(bs[0:], uint32(t.Replica))
	binary.LittleEndian.PutUint32(bs[4:], uint32(t.Ballot))
	binary.LittleEndian.PutUint32(bs[8:], uint32(t.CmdId.Client))
	binary.LittleEndian.PutUint32(bs[12:], uint32(t.CmdId.Sequence))
	wire.Write(bs)
	writeHashes(wire, b[:], t.Checksum)
	bs = b[:]
	alen3 := int64(len(t.Rep))
	if wlen := binary.PutVarint(bs, alen3); wlen >= 0 {
		wire.Write(b[0:wlen])
	}
	for i := int64(0); i < alen3; i++ {
		bs = b[:1]
		bs[0] = byte(t.Rep[i])
		wire.Write(bs)
	}
}

func (t *MReply) Unmarshal(rr io.Reader) error {
	wire := asByteReader(rr)
	var b [16]byte
	var bs []byte
	bs = b[:16]
	if _, err := io.ReadAtLeast(wire, bs, 16); err != nil {
		return err
	}
	t.Replica = int32(binary.LittleEndian.Uint32(bs[0:]))
	t.Ballot = int32(binary.LittleEndian.Uint32(bs[4:]))
	t.CmdId.Client = int32(binary.LittleEndian.Uint32(bs[8:]))
	t.CmdId.Sequence = int32(binary.LittleEndian.Uint32(bs[12:]))
	checksum, err := readHashes(wire)
	if err != nil {
		return err
	}
	t.Checksum = checksum
	alen3, err := binary.ReadVarint(wire)
	if err != nil {
		return err
	}
	if alen3 < 0 {
		return fmt.Errorf("negative reply value length: %d", alen3)
	}
	t.Rep = make([]byte, alen3)
	// With an empty Checksum, bs still spans the 16-byte header. Reading
	// "at least 1" into it consumed bytes from following RPCs during recovery.
	_, err = io.ReadFull(wire, t.Rep)
	return err
}

func (t *MNewLeaderAckN) BinarySize() (nbytes int, sizeKnown bool) {
	return 0, false
}

func (t *MNewLeaderAckN) Marshal(wire io.Writer) {
	var b [12]byte
	var bs []byte
	bs = b[:12]
	binary.LittleEndian.PutUint32(bs[0:], uint32(t.Replica))
	binary.LittleEndian.PutUint32(bs[4:], uint32(t.Ballot))
	binary.LittleEndian.PutUint32(bs[8:], uint32(t.Cballot))
	wire.Write(bs)
	writeCommandIds(wire, b[:], t.CmdIds)
	bs = b[:]
	alen2 := int64(len(t.Phases))
	if wlen := binary.PutVarint(bs, alen2); wlen >= 0 {
		wire.Write(b[0:wlen])
	}
	for i := int64(0); i < alen2; i++ {
		bs = b[:8]
		binary.LittleEndian.PutUint64(bs[0:], uint64(t.Phases[i]))
		wire.Write(bs)
	}
	bs = b[:]
	alen3 := int64(len(t.Cmds))
	if wlen := binary.PutVarint(bs, alen3); wlen >= 0 {
		wire.Write(b[0:wlen])
	}
	for i := int64(0); i < alen3; i++ {
		t.Cmds[i].Marshal(wire)
	}
	bs = b[:]
	alen4 := int64(len(t.Deps))
	if wlen := binary.PutVarint(bs, alen4); wlen >= 0 {
		wire.Write(b[0:wlen])
	}
	for i := int64(0); i < alen4; i++ {
		t.Deps[i].Marshal(wire)
	}
}

func (t *MNewLeaderAckN) Unmarshal(rr io.Reader) error {
	wire := asByteReader(rr)
	var b [12]byte
	var bs []byte
	bs = b[:12]
	if _, err := io.ReadAtLeast(wire, bs, 12); err != nil {
		return err
	}
	t.Replica = int32(binary.LittleEndian.Uint32(bs[0:]))
	t.Ballot = int32(binary.LittleEndian.Uint32(bs[4:]))
	t.Cballot = int32(binary.LittleEndian.Uint32(bs[8:]))
	cmdIds, err := readCommandIds(wire, b[:])
	if err != nil {
		return err
	}
	t.CmdIds = cmdIds
	alen2, err := binary.ReadVarint(wire)
	if err != nil {
		return err
	}
	t.Phases = make([]int, alen2)
	for i := int64(0); i < alen2; i++ {
		bs = b[:8]
		if _, err := io.ReadAtLeast(wire, bs, 8); err != nil {
			return err
		}
		t.Phases[i] = int(binary.LittleEndian.Uint64(bs[0:]))
	}
	alen3, err := binary.ReadVarint(wire)
	if err != nil {
		return err
	}
	t.Cmds = make([]state.Command, alen3)
	for i := int64(0); i < alen3; i++ {
		t.Cmds[i].Unmarshal(wire)
	}
	alen4, err := binary.ReadVarint(wire)
	if err != nil {
		return err
	}
	t.Deps = make([]SDep, alen4)
	for i := int64(0); i < alen4; i++ {
		t.Deps[i].Unmarshal(wire)
	}
	return nil
}

func (t *MCollect) BinarySize() (nbytes int, sizeKnown bool) {
	return 0, false
}

func (t *MCollect) Marshal(wire io.Writer) {
	var b [10]byte
	var bs []byte
	bs = b[:8]
	binary.LittleEndian.PutUint32(bs[0:], uint32(t.Replica))
	binary.LittleEndian.PutUint32(bs[4:], uint32(t.Ballot))
	wire.Write(bs)
	writeCommandIds(wire, b[:], t.Ids)
}

func (t *MCollect) Unmarshal(rr io.Reader) error {
	wire := asByteReader(rr)
	var b [10]byte
	var bs []byte
	bs = b[:8]
	if _, err := io.ReadAtLeast(wire, bs, 8); err != nil {
		return err
	}
	t.Replica = int32(binary.LittleEndian.Uint32(bs[0:]))
	t.Ballot = int32(binary.LittleEndian.Uint32(bs[4:]))
	ids, err := readCommandIds(wire, b[:])
	if err != nil {
		return err
	}
	t.Ids = ids
	return nil
}

func (t *MPingRep) BinarySize() (nbytes int, sizeKnown bool) {
	return 8, true
}

func (t *MPingRep) Marshal(wire io.Writer) {
	var b [8]byte
	var bs []byte
	bs = b[:8]
	binary.LittleEndian.PutUint32(bs[0:], uint32(t.Replica))
	binary.LittleEndian.PutUint32(bs[4:], uint32(t.Ballot))
	wire.Write(bs)
}

func (t *MPingRep) Unmarshal(wire io.Reader) error {
	var b [8]byte
	var bs []byte
	bs = b[:8]
	if _, err := io.ReadAtLeast(wire, bs, 8); err != nil {
		return err
	}
	t.Replica = int32(binary.LittleEndian.Uint32(bs[0:]))
	t.Ballot = int32(binary.LittleEndian.Uint32(bs[4:]))
	return nil
}

// asByteReader shares the caller's byte reader with nested message decoders.
func asByteReader(r io.Reader) byteReader {
	if r, ok := r.(byteReader); ok {
		return r
	}
	return bufio.NewReader(r)
}

// Slice helpers borrow the message scratch buffer to avoid extra allocations.
func writeCount(w io.Writer, b []byte, n int) {
	size := binary.PutVarint(b[:], int64(n))
	w.Write(b[:size])
}

func writeCommandIds(w io.Writer, b []byte, ids []defs.RequestID) {
	writeCount(w, b, len(ids))
	b = b[:8]
	for _, id := range ids {
		binary.LittleEndian.PutUint32(b[:4], uint32(id.Client))
		binary.LittleEndian.PutUint32(b[4:], uint32(id.Sequence))
		w.Write(b[:])
	}
}

func readCommandIds(r byteReader, b []byte) ([]defs.RequestID, error) {
	n, err := binary.ReadVarint(r)
	if err != nil {
		return nil, err
	}
	ids := make([]defs.RequestID, n)
	b = b[:8]
	for i := range ids {
		if _, err := io.ReadFull(r, b[:]); err != nil {
			return nil, err
		}
		ids[i].Client = int32(binary.LittleEndian.Uint32(b[:4]))
		ids[i].Sequence = int32(binary.LittleEndian.Uint32(b[4:]))
	}
	return ids, nil
}

func writeHashes(w io.Writer, b []byte, hashes []SHash) {
	writeCount(w, b, len(hashes))
	for i := range hashes {
		w.Write(hashes[i].H[:])
	}
}

func readHashes(r byteReader) ([]SHash, error) {
	n, err := binary.ReadVarint(r)
	if err != nil {
		return nil, err
	}
	hashes := make([]SHash, n)
	for i := range hashes {
		if _, err := io.ReadFull(r, hashes[i].H[:]); err != nil {
			return nil, err
		}
	}
	return hashes, nil
}
