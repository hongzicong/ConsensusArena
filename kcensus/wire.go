package kcensus

// Version 11 uses one RPC code for ordinary FIFO delivery.
// Heartbeats match local probes (High nonce, Explicit reply).
// Batch bodies contain single UIDs, never commands.
import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/hongzicong/ConsensusArena/replicaset"
	"github.com/hongzicong/ConsensusArena/rpc"
	"github.com/hongzicong/ConsensusArena/state"
)

const maxFrame = 16 << 20
const wireVersion = 11

type wireMessage struct{ message }

func (*wireMessage) New() rpc.Serializable { return &wireMessage{} }
func (w *wireMessage) BindClient(connection *rpc.ClientConnection) {
	w.ClientConnection = connection
}

// Append scalar fields directly, preserving version 11's little-endian layout.
// binary.Write allocates a temporary byte slice for every scalar field.
func put(w *[]byte, v interface{}) {
	switch x := v.(type) {
	case uint8:
		*w = append(*w, x)
	case int8:
		*w = append(*w, byte(x))
	case bool:
		if x {
			*w = append(*w, 1)
		} else {
			*w = append(*w, 0)
		}
	case uint16:
		*w = binary.LittleEndian.AppendUint16(*w, x)
	case uint32:
		*w = binary.LittleEndian.AppendUint32(*w, x)
	case uint64:
		*w = binary.LittleEndian.AppendUint64(*w, x)
	case int32:
		*w = binary.LittleEndian.AppendUint32(*w, uint32(x))
	case int64:
		*w = binary.LittleEndian.AppendUint64(*w, uint64(x))
	case replicaset.Set:
		*w = binary.LittleEndian.AppendUint64(*w, uint64(x))
	default:
		panic("unsupported KCensus wire scalar")
	}
}
func writeRecord(w *[]byte, r Record) {
	put(w, r.ID.Client)
	put(w, r.ID.Sequence)
	put(w, uint8(r.Command.Op))
	put(w, int64(r.Command.K))
	put(w, uint32(len(r.Command.V)))
	*w = append(*w, r.Command.V...)
}
func writeValue(w *[]byte, v *Value, references bool) {
	if v == nil {
		put(w, uint8(0))
		return
	}
	flag := uint8(2)
	if !references && !v.Reference {
		if v.Batch != nil {
			flag = 3
		} else {
			flag = 1
		}
	}
	put(w, flag)
	put(w, v.UID)
	put(w, uint8(v.Proposer))
	if flag == 1 {
		if len(v.Records) != 1 {
			panic("Single must have exactly one record")
		}
		writeRecord(w, v.Records[0])
	} else if flag == 3 {
		put(w, v.Batch.Slot)
		put(w, uint16(len(v.Batch.Members)))
		for _, uid := range v.Batch.Members {
			put(w, uid)
		}
	}
}
func (w *wireMessage) Marshal(out io.Writer) {
	b := make([]byte, 4, 192)
	put(&b, uint8(wireVersion))
	put(&b, w.Kind)
	put(&b, uint8(w.From))
	put(&b, uint8(w.Proposer))
	put(&b, int64(w.Key))
	put(&b, w.Slot)
	put(&b, w.Ballot)
	put(&b, w.AcceptedBallot)
	put(&b, w.High)
	put(&b, w.ID.Client)
	put(&b, w.ID.Sequence)
	put(&b, w.Mask)
	b = append(b, w.Digest[:]...)
	writeValue(&b, w.Value, w.References)
	writeValue(&b, w.Fast, w.References)
	put(&b, uint8(len(w.Knowledge)))
	for _, k := range w.Knowledge {
		put(&b, k)
	}
	put(&b, w.GraphTime)
	put(&b, w.Explicit)
	put(&b, w.NewValue)
	put(&b, w.StateTime)
	put(&b, uint8(len(w.Reports)))
	for _, r := range w.Reports {
		put(&b, uint8(r.From))
		put(&b, int8(r.Proposer))
		put(&b, r.Ballot)
		put(&b, r.AcceptedBallot)
		put(&b, r.Mask)
		put(&b, r.Time)
		writeValue(&b, r.Value, true)
	}
	put(&b, uint16(len(w.UIDs)))
	for _, uid := range w.UIDs {
		put(&b, uid)
	}
	put(&b, uint16(len(w.Values)))
	for _, v := range w.Values {
		writeValue(&b, v, false)
	}
	put(&b, w.Request != nil)
	if w.Request != nil {
		writeRecord(&b, *w.Request)
	}
	put(&b, uint32(len(w.Result)))
	b = append(b, w.Result...)
	if len(b)-4 > maxFrame {
		panic("KCensus frame exceeds limit")
	}
	binary.LittleEndian.PutUint32(b[:4], uint32(len(b)-4))
	_, _ = out.Write(b)
}
func (w *wireMessage) Unmarshal(in io.Reader) error {
	*w = wireMessage{}
	var size uint32
	if err := binary.Read(in, binary.LittleEndian, &size); err != nil {
		return err
	}
	if size > maxFrame {
		return fmt.Errorf("KCensus frame too large: %d", size)
	}
	data := make([]byte, size)
	if _, err := io.ReadFull(in, data); err != nil {
		return err
	}
	var err error
	get := func(v interface{}) {
		if err != nil {
			return
		}
		size := 0
		switch v.(type) {
		case *uint8, *int8, *bool:
			size = 1
		case *uint16:
			size = 2
		case *uint32, *int32:
			size = 4
		case *uint64, *int64, *replicaset.Set:
			size = 8
		case *[32]byte:
			size = 32
		default:
			panic("unsupported KCensus wire scalar")
		}
		if len(data) < size {
			err = io.ErrUnexpectedEOF
			return
		}
		switch x := v.(type) {
		case *uint8:
			*x = data[0]
		case *int8:
			*x = int8(data[0])
		case *bool:
			*x = data[0] != 0
		case *uint16:
			*x = binary.LittleEndian.Uint16(data)
		case *uint32:
			*x = binary.LittleEndian.Uint32(data)
		case *int32:
			*x = int32(binary.LittleEndian.Uint32(data))
		case *uint64:
			*x = binary.LittleEndian.Uint64(data)
		case *int64:
			*x = int64(binary.LittleEndian.Uint64(data))
		case *replicaset.Set:
			*x = replicaset.Set(binary.LittleEndian.Uint64(data))
		case *[32]byte:
			copy(x[:], data[:size])
		}
		data = data[size:]
	}
	readBytes := func() state.Value {
		var length uint32
		get(&length)
		if err != nil {
			return nil
		}
		if length > 65535 || int(length) > len(data) {
			err = fmt.Errorf("invalid command/result length")
			return nil
		}
		v := make(state.Value, length)
		copy(v, data[:length])
		data = data[length:]
		return v
	}
	readRecord := func() Record {
		var x Record
		var op uint8
		var key int64
		get(&x.ID.Client)
		get(&x.ID.Sequence)
		get(&op)
		get(&key)
		x.Command = state.Command{Op: state.Operation(op), K: state.Key(key), V: readBytes()}
		return x
	}
	readValue := func() *Value {
		var flag, proposer uint8
		get(&flag)
		if err != nil || flag == 0 {
			return nil
		}
		if flag > 3 {
			err = fmt.Errorf("invalid UID value flag")
			return nil
		}
		v := &Value{}
		get(&v.UID)
		get(&proposer)
		v.Proposer = int(proposer)
		if proposer >= 63 {
			err = fmt.Errorf("invalid proposer")
			return nil
		}
		switch flag {
		case 1:
			if v.UID&1 != 0 {
				err = fmt.Errorf("odd Single UID")
				return nil
			}
			v.Records = []Record{readRecord()}
		case 2:
			v.Reference = true
		case 3:
			if v.UID&1 == 0 {
				err = fmt.Errorf("even Batch UID")
				return nil
			}
			v.Batch = &Batch{}
			var count uint16
			get(&v.Batch.Slot)
			get(&count)
			if count < 2 || count > maxBatch || v.Batch.Slot == 0 {
				err = fmt.Errorf("invalid batch header")
				return nil
			}
			v.Batch.Members = make([]uint64, count)
			seen := map[uint64]bool{}
			for i := range v.Batch.Members {
				get(&v.Batch.Members[i])
				uid := v.Batch.Members[i]
				if uid&1 != 0 || seen[uid] {
					err = fmt.Errorf("nested or duplicate batch member")
					return nil
				}
				seen[uid] = true
			}
		}
		return v
	}
	var ver, from, proposer uint8
	var key int64
	get(&ver)
	get(&w.Kind)
	get(&from)
	get(&proposer)
	get(&key)
	w.From, w.Proposer, w.Key = int(from), int(proposer), state.Key(key)
	get(&w.Slot)
	get(&w.Ballot)
	get(&w.AcceptedBallot)
	get(&w.High)
	get(&w.ID.Client)
	get(&w.ID.Sequence)
	get(&w.Mask)
	get(&w.Digest)
	w.Value = readValue()
	w.Fast = readValue()
	var count uint8
	get(&count)
	if count > 63 {
		return fmt.Errorf("oversize knowledge")
	}
	w.Knowledge = make([]replicaset.Set, count)
	for i := range w.Knowledge {
		get(&w.Knowledge[i])
	}
	get(&w.GraphTime)
	get(&w.Explicit)
	get(&w.NewValue)
	get(&w.StateTime)
	get(&count)
	if count > 63 {
		return fmt.Errorf("oversize reports")
	}
	w.Reports = make([]nodeReport, count)
	var voters uint64
	for i := range w.Reports {
		x := &w.Reports[i]
		var from uint8
		var proposer int8
		get(&from)
		get(&proposer)
		x.From, x.Proposer = int(from), int(proposer)
		get(&x.Ballot)
		get(&x.AcceptedBallot)
		get(&x.Mask)
		get(&x.Time)
		x.Value = readValue()
		if from >= 63 || proposer < -1 || proposer >= 63 || voters&(1<<from) != 0 {
			return fmt.Errorf("invalid voter report")
		}
		voters |= 1 << from
	}
	var many uint16
	get(&many)
	if many > maxBatch {
		return fmt.Errorf("too many UID queries")
	}
	w.UIDs = make([]uint64, many)
	for i := range w.UIDs {
		get(&w.UIDs[i])
	}
	get(&many)
	if many > maxBatch {
		return fmt.Errorf("too many payload objects")
	}
	w.Values = make([]*Value, many)
	for i := range w.Values {
		w.Values[i] = readValue()
	}
	var request bool
	get(&request)
	if request {
		x := readRecord()
		w.Request = &x
	}
	w.Result = readBytes()
	if err != nil {
		return err
	}
	if ver != wireVersion || w.Kind < spread || w.Kind > resultQuery || len(data) != 0 {
		return fmt.Errorf("invalid KCensus frame")
	}
	return nil
}
