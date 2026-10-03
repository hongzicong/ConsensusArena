package swift

import (
	"encoding/binary"

	"github.com/hongzicong/ConsensusArena/state"
)

type keyInfo interface {
	add(state.Command, CommandId)
	remove(state.Command, CommandId)
	getConflictCmds(cmd state.Command) []CommandId
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
	lastWrite []CommandId
	lastCmd   []CommandId
}

func newLightKeyInfo() *lightKeyInfo {
	return &lightKeyInfo{
		lastWrite: []CommandId{},
		lastCmd:   []CommandId{},
	}
}

func (ki *lightKeyInfo) add(cmd state.Command, cmdId CommandId) {
	ki.lastCmd = []CommandId{cmdId}

	if cmd.Op == state.PUT {
		ki.lastWrite = []CommandId{cmdId}
	}
}

func (ki *lightKeyInfo) remove(_ state.Command, cmdId CommandId) {
	if len(ki.lastCmd) > 0 && ki.lastCmd[0] == cmdId {
		ki.lastCmd = []CommandId{}
	}

	if len(ki.lastWrite) > 0 && ki.lastWrite[0] == cmdId {
		ki.lastWrite = []CommandId{}
	}
}

func (ki *lightKeyInfo) getConflictCmds(cmd state.Command) []CommandId {
	if cmd.Op == state.GET {
		return ki.lastWrite
	} else {
		return ki.lastCmd
	}
}
