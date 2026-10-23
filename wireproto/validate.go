package wireproto

import (
	"fmt"

	"google.golang.org/protobuf/encoding/protowire"
)

// Field describes checks that a generated narrowing cast cannot enforce itself.
type Field struct {
	Message      *Shape
	UnsignedBits uint
	SignedBits   uint
	Width        int
	Packed       bool
}

type Shape map[protowire.Number]Field

// Validate checks bounds without constructing an intermediate message graph.
func (shape Shape) Validate(data []byte, depth int) error {
	if depth <= 0 {
		return fmt.Errorf("protobuf: recursion limit exceeded")
	}
	var seen uint64
	for len(data) > 0 {
		num, kind, n := protowire.ConsumeTag(data)
		if n < 0 {
			return protowire.ParseError(n)
		}
		data = data[n:]
		if num > 0 && num <= 64 {
			seen |= uint64(1) << uint(num-1)
		}
		field, known := shape[num]
		if kind == protowire.BytesType && known {
			body, used := protowire.ConsumeBytes(data)
			if used < 0 {
				return protowire.ParseError(used)
			}
			if field.Width != 0 && len(body) != field.Width {
				return fmt.Errorf("protobuf: expected %d-byte hash", field.Width)
			}
			if field.Message != nil {
				if err := field.Message.Validate(body, depth-1); err != nil {
					return err
				}
			} else if field.Packed {
				for len(body) > 0 {
					value, size := protowire.ConsumeVarint(body)
					if size < 0 {
						return protowire.ParseError(size)
					}
					if err := field.check(value); err != nil {
						return err
					}
					body = body[size:]
				}
			}
			data = data[used:]
			continue
		}
		if kind == protowire.VarintType && known {
			value, used := protowire.ConsumeVarint(data)
			if used < 0 {
				return protowire.ParseError(used)
			}
			if err := field.check(value); err != nil {
				return err
			}
			data = data[used:]
			continue
		}
		used := protowire.ConsumeFieldValue(num, kind, data)
		if used < 0 {
			return protowire.ParseError(used)
		}
		data = data[used:]
	}
	for number, field := range shape {
		if field.Width != 0 && number <= 64 && seen&(uint64(1)<<uint(number-1)) == 0 {
			return fmt.Errorf("protobuf: expected %d-byte hash", field.Width)
		}
	}
	return nil
}

func (field Field) check(value uint64) error {
	if field.UnsignedBits != 0 && value>>field.UnsignedBits != 0 {
		return fmt.Errorf("protobuf: out-of-range uint%d", field.UnsignedBits)
	}
	if field.SignedBits != 0 {
		signed := protowire.DecodeZigZag(value)
		bound := int64(1) << (field.SignedBits - 1)
		if signed < -bound || signed >= bound {
			return fmt.Errorf("protobuf: out-of-range int%d", field.SignedBits)
		}
	}
	return nil
}

func EmptySlice[S ~[]E, E any](dst *S) {
	if *dst == nil {
		*dst = make(S, 0)
	}
}
