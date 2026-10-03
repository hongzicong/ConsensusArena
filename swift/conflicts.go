package swift

import (
	"encoding/binary"

	"github.com/hongzicong/ConsensusArena/replica/defs"
	"github.com/hongzicong/ConsensusArena/state"
)

type keyInfo interface {
	add(state.Command, defs.RequestID)
	remove(state.Command, defs.RequestID)
	getConflictCmds(cmd state.Command) []defs.RequestID
}

func keysOf(cmd state.Command) []state.Key {
	switch cmd.Op {
	case state.SCAN:
		count := binary.LittleEndian.Uint64(cmd.V)
		ks := make([]state.Key, count)
		for i := range ks {
			ks[i] = cmd.K + state.Key(i)
		}
		return ks
	default:
		return []state.Key{cmd.K}
	}
}

type lightKeyInfo struct {
	lastWrite []defs.RequestID
	lastCmd   []defs.RequestID
}

func newLightKeyInfo() *lightKeyInfo {
	return &lightKeyInfo{
		lastWrite: []defs.RequestID{},
		lastCmd:   []defs.RequestID{},
	}
}

func (ki *lightKeyInfo) add(cmd state.Command, cmdId defs.RequestID) {
	ki.lastCmd = []defs.RequestID{cmdId}

	if cmd.Op == state.PUT {
		ki.lastWrite = []defs.RequestID{cmdId}
	}
}

func (ki *lightKeyInfo) remove(_ state.Command, cmdId defs.RequestID) {
	if len(ki.lastCmd) > 0 && ki.lastCmd[0] == cmdId {
		ki.lastCmd = []defs.RequestID{}
	}

	if len(ki.lastWrite) > 0 && ki.lastWrite[0] == cmdId {
		ki.lastWrite = []defs.RequestID{}
	}
}

func (ki *lightKeyInfo) getConflictCmds(cmd state.Command) []defs.RequestID {
	if cmd.Op == state.GET {
		return ki.lastWrite
	} else {
		return ki.lastCmd
	}
}
