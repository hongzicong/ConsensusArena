// Package wiretypes contains scalar representations shared by generated messages.
package wiretypes

import "fmt"

// Hash keeps protocol hashes comparable and enforces their fixed wire width.
type Hash [32]byte

func (h Hash) Size() int                         { return len(h) }
func (h Hash) Marshal() ([]byte, error)          { return append([]byte(nil), h[:]...), nil }
func (h Hash) MarshalTo(dst []byte) (int, error) { return copy(dst, h[:]), nil }
func (h *Hash) Unmarshal(src []byte) error {
	if len(src) != len(h) {
		return fmt.Errorf("protobuf: expected 32-byte hash")
	}
	copy(h[:], src)
	return nil
}

func (h *Hash) UnmarshalProto(src []byte) error { return h.Unmarshal(src) }
