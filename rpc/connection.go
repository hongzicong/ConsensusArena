package rpc

import (
	"net"
	"sync"
)

// ClientConnection is local stream context, never a wire field. All messages
// on a connection share it. Its first protocol message resolves the process ID;
// subsequent messages reuse the result, including a failed binding.
type ClientConnection struct {
	Conn     net.Conn
	Identity string
	once     sync.Once
	peer     int
	err      error
}

func (c *ClientConnection) BindPeer(bind func() (int, error)) (int, error) {
	c.once.Do(func() { c.peer, c.err = bind() })
	return c.peer, c.err
}
