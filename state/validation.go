package state

import "fmt"

// MaxValueBytes is the value length representable by Arena's ordinary wire format.
const MaxValueBytes = 65535

// Empty Operations accepts the ordinary NONE/PUT/GET/SCAN operations. A
// protocol may explicitly list its operations, including transport controls.
// Zero MaxValueBytes selects the ordinary wire limit.
type CommandPolicy struct {
	Operations    []Operation
	MaxValueBytes int
}

func (p CommandPolicy) Validate(c Command) error {
	allowed := c.Op <= SCAN
	if len(p.Operations) != 0 {
		allowed = false
		for _, op := range p.Operations {
			if c.Op == op {
				allowed = true
				break
			}
		}
	}
	if !allowed {
		return fmt.Errorf("unsupported command operation %d", c.Op)
	}
	limit := p.MaxValueBytes
	if limit == 0 {
		limit = MaxValueBytes
	}
	if len(c.V) > limit {
		return fmt.Errorf("command value exceeds %d-byte limit", limit)
	}
	if c.Op == SCAN && len(c.V) < 8 {
		return fmt.Errorf("SCAN requires an eight-byte count")
	}
	return nil
}
