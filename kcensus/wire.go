package kcensus

import (
	"fmt"
	rpc "github.com/hongzicong/ConsensusArena/rpc"
)

const maxFrame = 16 << 20

type wireMessage struct {
	message
	// Connection context is local to ingress and is never serialized.
	connection *rpc.ClientConnection
}

func (*wireMessage) New() rpc.Serializable { return &wireMessage{} }

func (w *wireMessage) BindClient(connection *rpc.ClientConnection) {
	w.connection = connection
}

// Reference selection copies only the native generated header. Records and
// batches stay owned by protocol state; no converted protobuf graph is built.
func wireValue(v *Value, references bool) Value {
	if v == nil {
		return Value{Absent: true}
	}
	value := *v
	if references || value.Reference {
		value.Reference = true
		value.Records = nil
		value.Batch = nil
	}
	if !value.Reference && value.Batch == nil && !value.Absent && len(value.Records) != 1 {
		panic("Single must have exactly one record")
	}
	return value
}

func wireValueSize(v *Value, references bool) int {
	value := wireValue(v, references)
	return value.Size()
}
func wireValueMarshal(v *Value, references bool, dst []byte) (int, error) {
	value := wireValue(v, references)
	return value.MarshalToSizedBuffer(dst)
}

func (m *Message) normalizeAbsentValues() error {
	clean := func(v **Value) error {
		if *v == nil || !(*v).Absent {
			return nil
		}
		x := *v
		if x.UID != 0 || x.Proposer != 0 || len(x.Records) != 0 || x.Batch != nil || x.Reference {
			return fmt.Errorf("protobuf: absent value contains data")
		}
		*v = nil
		return nil
	}
	if err := clean(&m.Value); err != nil {
		return err
	}
	if err := clean(&m.Fast); err != nil {
		return err
	}
	for i := range m.Reports {
		if err := clean(&m.Reports[i].Value); err != nil {
			return err
		}
	}
	for i := range m.Values {
		if err := clean(&m.Values[i]); err != nil {
			return err
		}
	}
	return nil
}

func validateProtoValue(v *Value) error {
	if v == nil {
		return nil
	}
	if v.Proposer < 0 || v.Proposer >= 63 {
		return fmt.Errorf("invalid proposer")
	}
	if v.Reference {
		if v.Batch != nil || len(v.Records) != 0 {
			return fmt.Errorf("reference contains payload")
		}
		return nil
	}
	if v.Batch != nil {
		b := v.Batch
		if v.UID&1 == 0 || b.Slot == 0 || len(b.Members) < 2 || len(b.Members) > maxBatch || len(v.Records) != 0 {
			return fmt.Errorf("invalid batch header")
		}
		seen := make(map[uint64]bool, len(b.Members))
		for _, uid := range b.Members {
			if uid&1 != 0 || seen[uid] {
				return fmt.Errorf("nested or duplicate batch member")
			}
			seen[uid] = true
		}
		return nil
	}
	if v.UID&1 != 0 || len(v.Records) != 1 || len(v.Records[0].Command.V) > 65535 {
		return fmt.Errorf("invalid single value")
	}
	if v.Records[0].Command.V == nil {
		v.Records[0].Command.V = make([]byte, 0)
	}
	return nil
}

func validateProtoMessage(m *message) error {
	if m.Kind < spread || m.Kind > checkpointAck || len(m.Knowledge) > 63 || len(m.Reports) > 63 || len(m.UIDs) > maxBatch || len(m.Values) > maxBatch || len(m.Result) > 65535 {
		return fmt.Errorf("invalid KCensus frame")
	}
	if err := validateProtoValue(m.Value); err != nil {
		return err
	}
	if err := validateProtoValue(m.Fast); err != nil {
		return err
	}
	var voters uint64
	for _, r := range m.Reports {
		if r.From < 0 || r.From >= 63 || r.Proposer < -1 || r.Proposer >= 63 || voters&(1<<r.From) != 0 {
			return fmt.Errorf("invalid voter report")
		}
		voters |= 1 << r.From
		if err := validateProtoValue(r.Value); err != nil {
			return err
		}
	}
	for _, v := range m.Values {
		if err := validateProtoValue(v); err != nil {
			return err
		}
	}
	if m.Request != nil && len(m.Request.Command.V) > 65535 {
		return fmt.Errorf("invalid command length")
	}
	if m.Request != nil && m.Request.Command.V == nil {
		m.Request.Command.V = make([]byte, 0)
	}
	if m.Result == nil {
		m.Result = make([]byte, 0)
	}
	return nil
}
