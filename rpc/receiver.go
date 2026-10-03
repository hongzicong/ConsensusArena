package rpc

import (
	"bufio"
	"fmt"
	"io"
)

// Decode creates a fresh message using the registered factory. The registration
// remains unchanged, so simultaneous streams never share a decoded object.
func (t *Table) Decode(code uint8, r io.Reader) (Pair, error) {
	p, ok := t.Get(code)
	if !ok {
		return Pair{}, fmt.Errorf("unknown RPC code %d", code)
	}
	p.Obj = p.Obj.New()
	if err := p.Obj.Unmarshal(r); err != nil {
		return Pair{}, err
	}
	return p, nil
}

// ReadLoop processes one stream in order, stopping on a read error or a false
// receive result. The caller owns connection closure and failure reporting.
// Untagged replies and tagged RPC messages use the same loop.
func ReadLoop[T any](read func() (T, error), receive func(T) bool) error {
	for {
		msg, err := read()
		if err != nil {
			return err
		}
		if !receive(msg) {
			return nil
		}
	}
}

// ReadMessage reads exactly one tagged frame, including its payload.
func ReadMessage[T any](r *bufio.Reader, decode func(uint8, io.Reader) (T, error)) (T, error) {
	code, err := r.ReadByte()
	if err != nil {
		var zero T
		return zero, err
	}
	return decode(code, r)
}

// ReadStream leaves message formats and delivery policy to its callbacks.
func ReadStream[T any](r *bufio.Reader, decode func(uint8, io.Reader) (T, error), receive func(T) bool) error {
	return ReadLoop(func() (T, error) { return ReadMessage(r, decode) }, receive)
}

// Deliver preserves blocking backpressure. A nil stop channel keeps the direct
// channel-send path; protocols with cancellation retain their existing select.
func Deliver[T any](channel chan<- T, msg T, stop <-chan struct{}) bool {
	if stop == nil {
		channel <- msg
		return true
	}
	select {
	case channel <- msg:
		return true
	case <-stop:
		return false
	}
}

func (p Pair) Deliver() bool { return Deliver(p.Chan, p.Obj, nil) }
