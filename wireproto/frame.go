// Package wireproto supplies the same protobuf framing and allocation policy to
// every protocol. Master/admin net/rpc and local stable-store records are separate.
package wireproto

import (
	"encoding/binary"
	"fmt"
	"io"
)

const MaxFrame = 64 << 20
const headerSize = 8

// The magic/version rejects unsupported wire formats. The existing RPC
// registration byte still identifies the message type.
var magic = [4]byte{'C', 'A', 'P', 1}

// Encode writes directly to one final allocation, including the optional RPC
// registration byte. No intermediary serialized-message buffer is copied out.
type Message interface {
	Size() int
	MarshalToSizedBuffer([]byte) (int, error)
	UnmarshalProto([]byte) error
	ValidateProtoPayload([]byte, int) error
	NormalizeProto() error
}

func Encode(code uint8, tagged bool, message Message, limit int) ([]byte, error) {
	size := message.Size()
	if size > limit || size > MaxFrame {
		return nil, fmt.Errorf("protobuf frame exceeds %d-byte limit", limit)
	}
	offset := 0
	if tagged {
		offset = 1
	}
	frame := make([]byte, offset+headerSize+size)
	if tagged {
		frame[0] = code
	}
	copy(frame[offset:offset+4], magic[:])
	n, err := message.MarshalToSizedBuffer(frame[offset+headerSize:])
	if err != nil {
		return nil, err
	}
	if n != size {
		return nil, fmt.Errorf("protobuf size mismatch: predicted %d, wrote %d", size, n)
	}
	binary.LittleEndian.PutUint32(frame[offset+4:offset+8], uint32(n))
	return frame, nil
}

// Write retains Serializable.Marshal's contract: construction failures panic,
// while stream errors are observed by the existing owning writer/flush path.
func Write(w io.Writer, message Message, limit int) {
	frame, err := Encode(0, false, message, limit)
	if err != nil {
		panic(err)
	}
	_, _ = w.Write(frame)
}

// Read consumes exactly one frame, preserving following frames in the stream.
// Bounds are checked before allocating the body; decoding never mutates protocol
// state until the adapter has validated and converted the complete message.
func Read(r io.Reader, message Message, limit int) error {
	var header [headerSize]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		return err
	}
	if string(header[:4]) != string(magic[:]) {
		return fmt.Errorf("wire codec mismatch: expected protobuf-v1 (all participants must use the same build)")
	}
	size := binary.LittleEndian.Uint32(header[4:])
	if uint64(size) > uint64(limit) || size > MaxFrame {
		return fmt.Errorf("protobuf frame exceeds %d-byte limit", limit)
	}
	body := make([]byte, int(size))
	if _, err := io.ReadFull(r, body); err != nil {
		return err
	}
	if err := message.ValidateProtoPayload(body, 64); err != nil {
		return err
	}
	if err := message.UnmarshalProto(body); err != nil {
		return err
	}
	return message.NormalizeProto()
}
