package swift

import (
	"fmt"
	"github.com/hongzicong/ConsensusArena/replica/defs"
	fastrpc "github.com/hongzicong/ConsensusArena/rpc"
	"github.com/hongzicong/ConsensusArena/state"
)

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

// Recovery transmits entries directly in generated messages. The state machine
// builds lookup indexes when installing that recovery state, not in the codec.
func (m *MSync) recoveryIndex() (map[defs.RequestID]int, map[defs.RequestID]state.Command, map[defs.RequestID]Dep) {
	var phases map[defs.RequestID]int
	var cmds map[defs.RequestID]state.Command
	var deps map[defs.RequestID]Dep
	if m.PhasesPresent || len(m.Phases) > 0 {
		phases = make(map[defs.RequestID]int, len(m.Phases))
	}
	if m.CmdsPresent || len(m.Cmds) > 0 {
		cmds = make(map[defs.RequestID]state.Command, len(m.Cmds))
	}
	if m.DepsPresent || len(m.Deps) > 0 {
		deps = make(map[defs.RequestID]Dep, len(m.Deps))
	}
	for _, e := range m.Phases {
		phases[e.Key] = e.Value
	}
	for _, e := range m.Cmds {
		cmds[e.Key] = e.Value
	}
	for _, e := range m.Deps {
		deps[e.Key] = Dep(e.Value)
	}
	return phases, cmds, deps
}

func (m *MSync) validateRecoveryEntries() error {
	check := func(seen map[defs.RequestID]bool, id defs.RequestID) error {
		if seen[id] {
			return fmt.Errorf("protobuf: duplicate map key")
		}
		seen[id] = true
		return nil
	}
	seen := make(map[defs.RequestID]bool, len(m.Phases))
	for _, e := range m.Phases {
		if err := check(seen, e.Key); err != nil {
			return err
		}
	}
	clear(seen)
	for _, e := range m.Cmds {
		if err := check(seen, e.Key); err != nil {
			return err
		}
	}
	clear(seen)
	for _, e := range m.Deps {
		if err := check(seen, e.Key); err != nil {
			return err
		}
	}
	return nil
}
