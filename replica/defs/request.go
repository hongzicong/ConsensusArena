package defs

import (
	"fmt"
)

// RequestID identifies one client request across retries. It is independent of
// the command's database key, protocol instance, ballot, and batch identity.
// Signed fields preserve protocol-specific sentinel requests.
func (m Propose) RequestID() RequestID {
	return RequestID{Client: m.ClientId, Sequence: m.CommandId}
}

func (id RequestID) String() string {
	return fmt.Sprintf("%v,%v", id.Client, id.Sequence)
}
