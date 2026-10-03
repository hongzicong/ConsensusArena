package paxos

// Message factories, wire encoding, and wire decoding.
import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"io"

	fastrpc "github.com/hongzicong/ConsensusArena/rpc"
)

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
