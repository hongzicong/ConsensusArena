package state

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"strconv"
	"sync"

	"github.com/emirpasic/gods/maps/treemap"
)

type Operation uint8

const (
	NONE Operation = iota
	PUT
	GET
	SCAN
)

type Value []byte

func NIL() Value { return Value([]byte{}) }

type Key int64

type Command struct {
	Op Operation
	K  Key
	V  Value
}

type ID int64
type Phase int8

func NOOP() []Command { return []Command{{NONE, 0, NIL()}} }

type State struct {
	mutex *sync.Mutex
	Store *treemap.Map
}

func KeyComparator(a, b interface{}) int {
	aAsserted := a.(Key)
	bAsserted := b.(Key)
	switch {
	case aAsserted > bAsserted:
		return 1
	case aAsserted < bAsserted:
		return -1
	default:
		return 0
	}
}

func concat(slices []Value) Value {
	var totalLen int
	for _, s := range slices {
		totalLen += len(s)
	}
	tmp := make([]byte, totalLen)
	var i int
	for _, s := range slices {
		i += copy(tmp[i:], s)
	}
	return tmp
}

func InitState() *State {
	return &State{new(sync.Mutex), treemap.NewWith(KeyComparator)}
}

func (st *State) Preload(recordCount, valueSize int, seed uint64) (string, error) {
	if recordCount < 0 {
		return "", errors.New("record count must be non-negative")
	}
	if valueSize < 0 {
		return "", errors.New("value size must be non-negative")
	}

	st.mutex.Lock()
	defer st.mutex.Unlock()
	if st.Store.Size() != 0 {
		return "", errors.New("state must be empty before preload")
	}

	digest := sha256.New()
	var keyBytes [8]byte
	for key := 0; key < recordCount; key++ {
		value := make(Value, valueSize)
		fillPreloadedValue(value, uint64(key), seed)
		stateKey := Key(key)
		st.Store.Put(stateKey, value)
		binary.LittleEndian.PutUint64(keyBytes[:], uint64(stateKey))
		digest.Write(keyBytes[:])
		digest.Write(value)
	}

	return hex.EncodeToString(digest.Sum(nil)), nil
}

func fillPreloadedValue(value Value, key, seed uint64) {
	generatorState := seed ^ (key * 0x9e3779b97f4a7c15)
	for offset := 0; offset < len(value); {
		generatorState += 0x9e3779b97f4a7c15
		word := generatorState
		word = (word ^ (word >> 30)) * 0xbf58476d1ce4e5b9
		word = (word ^ (word >> 27)) * 0x94d049bb133111eb
		word ^= word >> 31
		for remaining := 0; remaining < 8 && offset < len(value); remaining++ {
			value[offset] = byte(word)
			word >>= 8
			offset++
		}
	}
}

func Conflict(gamma *Command, delta *Command) bool {
	key := gamma.K
	lb := delta.K
	ub := delta.K

	if gamma.Op == SCAN && delta.Op == SCAN {
		return false
	}

	if gamma.Op == SCAN {
		key = delta.K
		lb = gamma.K
		ub = gamma.K + Key(binary.LittleEndian.Uint64(gamma.V))
	} else if delta.Op == SCAN {
		ub = delta.K + Key(binary.LittleEndian.Uint64(delta.V))
	}

	if key >= lb && key <= ub {
		if gamma.Op == PUT || delta.Op == PUT {
			return true
		}
	}
	return false
}

func ConflictBatch(batch1 []Command, batch2 []Command) bool {
	for i := 0; i < len(batch1); i++ {
		for j := 0; j < len(batch2); j++ {
			if Conflict(&batch1[i], &batch2[j]) {
				return true
			}
		}
	}
	return false
}

func IsRead(command *Command) bool {
	return command.Op == GET
}

func (c *Command) Execute(st *State) Value {

	st.mutex.Lock()
	defer st.mutex.Unlock()

	switch c.Op {
	case PUT:
		st.Store.Put(c.K, c.V)

	case GET:
		if value, present := st.Store.Get(c.K); present {
			valAsserted := value.(Value)
			return valAsserted
		}

	case SCAN:
		found := make([]Value, 0)
		count := binary.LittleEndian.Uint64(c.V)
		it := st.Store.Select(func(index interface{}, value interface{}) bool {
			keyAsserted := index.(Key)
			return keyAsserted >= c.K && keyAsserted <= c.K+Key(count)
		}).Iterator()
		for it.Next() {
			valAsserted := it.Value().(Value)
			found = append(found, valAsserted)
		}
		ret := concat(found)
		return ret
	}

	return NIL()
}

func (v *Value) String() string {
	if v == nil || len(*v) == 0 {
		return "(void)"
	}
	return hex.EncodeToString(*v)
}

func (k *Key) String() string {
	return strconv.FormatInt(int64(*k), 16)
}

func (c *Command) String() string {
	ret := ""
	switch c.Op {
	case PUT:
		ret = "PUT( " + c.K.String() + " , " + c.V.String() + " )"
	case GET:
		ret = "GET( " + c.K.String() + " )"
	case SCAN:
		count := binary.LittleEndian.Uint64(c.V)
		ret = "SCAN( " + c.K.String() + " , " + fmt.Sprint(count) + " )"
	default:
		ret = "UNKNOWN( " + c.V.String() + " , " + c.K.String() + " )"
	}
	return ret
}

func (c *Command) Marshal(w io.Writer) {
	c.Op.Marshal(w)
	c.K.Marshal(w)
	c.V.Marshal(w)
}

func (c *Command) Unmarshal(r io.Reader) error {

	err := c.Op.Unmarshal(r)
	if err != nil {
		return err
	}

	err = c.K.Unmarshal(r)
	if err != nil {
		return err
	}

	err = c.V.Unmarshal(r)
	if err != nil {
		return err
	}

	return nil
}

func (op *Operation) Marshal(w io.Writer) {
	bs := make([]byte, 1)
	bs[0] = byte(*op)
	w.Write(bs)
}

func (op *Operation) Unmarshal(r io.Reader) error {
	bs := make([]byte, 1)
	if _, err := io.ReadFull(r, bs); err != nil {
		return err
	}
	*op = Operation(bs[0])
	return nil
}

func (k *Key) Marshal(w io.Writer) {
	bs := make([]byte, 8)
	binary.LittleEndian.PutUint64(bs, uint64(*k))
	w.Write(bs)
}

func (k *Key) Unmarshal(r io.Reader) error {
	bs := make([]byte, 8)
	if _, err := io.ReadFull(r, bs); err != nil {
		return err
	}
	*k = Key(binary.LittleEndian.Uint64(bs))
	return nil
}

func (v *Value) Marshal(w io.Writer) {
	bs := make([]byte, 4)
	if v == nil {
		binary.LittleEndian.PutUint16(bs, 0)
		w.Write(bs)
	} else {
		binary.LittleEndian.PutUint16(bs, uint16(len(*v)))
		w.Write(bs)
		w.Write(*v)
	}
}

func (v *Value) Unmarshal(r io.Reader) error {
	bs := make([]byte, 4)
	if _, err := io.ReadFull(r, bs); err != nil {
		return err
	}
	len := binary.LittleEndian.Uint16(bs)
	bs = make([]byte, len)
	if _, err := io.ReadFull(r, bs); err != nil {
		return err
	}
	*v = Value(bs)
	return nil
}
