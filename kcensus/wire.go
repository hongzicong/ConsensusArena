package kcensus

// Version 11 uses one RPC code for ordinary FIFO delivery.
// Heartbeats match local probes (High nonce, Explicit reply).
// Batch bodies contain single UIDs, never commands.
import (
	"bytes"
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
func put(w io.Writer, v interface{}) { _ = binary.Write(w, binary.LittleEndian, v) }
func writeRecord(w io.Writer, r Record) {
	put(w, r.ID.Client)
	put(w, r.ID.Sequence)
	put(w, uint8(r.Command.Op))
	put(w, int64(r.Command.K))
	put(w, uint32(len(r.Command.V)))
	_, _ = w.Write(r.Command.V)
}
func writeValue(w io.Writer, v *Value, references bool) {
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
	var b bytes.Buffer
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
	b.Write(w.Digest[:])
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
	b.Write(w.Result)
	if b.Len() > maxFrame {
		panic("KCensus frame exceeds limit")
	}
	put(out, uint32(b.Len()))
	_, _ = out.Write(b.Bytes())
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
	r := bytes.NewReader(data)
	var err error
	get := func(v interface{}) {
		if err == nil {
			err = binary.Read(r, binary.LittleEndian, v)
		}
	}
	readBytes := func() state.Value {
		var length uint32
		get(&length)
		if err != nil {
			return nil
		}
		if length > 65535 || int(length) > r.Len() {
			err = fmt.Errorf("invalid command/result length")
			return nil
		}
		v := make(state.Value, length)
		_, err = io.ReadFull(r, v)
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
	if ver != wireVersion || w.Kind < spread || w.Kind > resultQuery || r.Len() != 0 {
		return fmt.Errorf("invalid KCensus frame")
	}
	return nil
}
