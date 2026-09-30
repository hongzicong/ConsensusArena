// Package recoverylog implements the recoverable ordered substrate used by
// N²Paxos and the consensus-extension deployment of CURP.
package recoverylog

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	fastrpc "github.com/hongzicong/ConsensusArena/rpc"
	"github.com/hongzicong/ConsensusArena/state"
	"io"
)

type Key struct{ Client, Sequence int32 }
type Request struct {
	ID      Key
	Command state.Command
}
type Record struct {
	Slot, Ballot int64
	Request      Request
	Committed    bool
}

const (
	Prepare uint8 = iota
	Promise
	Accept
	Ack
	Commit
	Ready
	Fetch
	Forward
)

type Packet struct {
	Kind                uint8
	From                int32
	Ballot, Floor, High int64
	Page, Pages         int32
	Records             []Record
	Requests            []Request
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
