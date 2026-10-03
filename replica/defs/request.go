package defs

import (
	"encoding/binary"
	"fmt"
	"io"
)

// RequestID identifies one client request across retries. It is independent of
// the command's database key, protocol instance, ballot, and batch identity.
// Signed fields preserve protocol-specific sentinel requests.
type RequestID struct {
	Client   int32
	Sequence int32
}

func (p Propose) RequestID() RequestID {
	return RequestID{Client: p.ClientId, Sequence: p.CommandId}
}

func (id RequestID) String() string {
	return fmt.Sprintf("%v,%v", id.Client, id.Sequence)
}

func (*RequestID) BinarySize() (int, bool) { return 8, true }

// Marshal preserves the client-then-sequence little-endian binary layout.
func (id *RequestID) Marshal(w io.Writer) {
	var b [8]byte
	binary.LittleEndian.PutUint32(b[:4], uint32(id.Client))
	binary.LittleEndian.PutUint32(b[4:], uint32(id.Sequence))
	w.Write(b[:])
}

func (id *RequestID) Unmarshal(r io.Reader) error {
	var b [8]byte
	if _, err := io.ReadFull(r, b[:]); err != nil {
		return err
	}
	id.Client = int32(binary.LittleEndian.Uint32(b[:4]))
	id.Sequence = int32(binary.LittleEndian.Uint32(b[4:]))
	return nil
}
