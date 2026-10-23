package fastpaxos

import (
	"fmt"
	fastrpc "github.com/hongzicong/ConsensusArena/rpc"
	"github.com/hongzicong/ConsensusArena/state"
)

type wireMessage struct{ message }

func (*wireMessage) New() fastrpc.Serializable { return &wireMessage{} }

func wireRecordSize(v *Record, payload bool) int {
	if !payload || v.Noop {
		return v.SizeIdentity()
	}
	return v.Size()
}
func wireRecordMarshal(v *Record, payload bool, dst []byte) (int, error) {
	if !payload || v.Noop {
		return v.MarshalIdentityToSizedBuffer(dst)
	}
	return v.MarshalToSizedBuffer(dst)
}

func validateProtoMessage(m *message) error {
	if m.Kind > msgRecover || len(m.Entries) > pageSize {
		return fmt.Errorf("invalid FastPaxos message")
	}
	total := 0
	for i := range m.Entries {
		e := &m.Entries[i]
		if e.Slot < 0 {
			return fmt.Errorf("negative FastPaxos slot")
		}
		if e.Payload && !e.Value.Noop {
			if e.Value.Command.V == nil {
				e.Value.Command.V = make(state.Value, 0)
			}
			total += len(e.Value.Command.V)
			if e.Value.Command.Op > state.SCAN || len(e.Value.Command.V) > 16<<20 || total > 32<<20 {
				return fmt.Errorf("invalid FastPaxos payload")
			}
		}
	}
	return nil
}
