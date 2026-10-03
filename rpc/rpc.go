package rpc

import (
	"bufio"
	"io"
)

type Serializable interface {
	Marshal(io.Writer)
	Unmarshal(io.Reader) error
	New() Serializable
}

type Pair struct {
	Obj  Serializable
	Chan chan Serializable
}

type Table struct {
	id    uint8
	pairs map[uint8]Pair
}

func NewTable() *Table {
	return &Table{
		id:    0,
		pairs: make(map[uint8]Pair),
	}
}

func NewTableId(id uint8) *Table {
	return &Table{
		id:    id,
		pairs: make(map[uint8]Pair),
	}
}

func (t *Table) Register(obj Serializable, notify chan Serializable) uint8 {
	id := t.id
	t.id++
	t.pairs[id] = Pair{
		Obj:  obj,
		Chan: notify,
	}
	return id
}

func (t *Table) Get(id uint8) (Pair, bool) {
	p, exists := t.pairs[id]
	return p, exists
}

// ReadStream reads tagged messages on one stream. Decode owns message formats
// and code validation; receive owns identity checks and event delivery. False
// stops normally. The caller owns connection closure and failure reporting.
func ReadStream[T any](r *bufio.Reader, decode func(uint8, io.Reader) (T, error), receive func(T) bool) error {
	for {
		code, err := r.ReadByte()
		if err != nil {
			return err
		}
		msg, err := decode(code, r)
		if err != nil {
			return err
		}
		if !receive(msg) {
			return nil
		}
	}
}
