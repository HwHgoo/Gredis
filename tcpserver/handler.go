package tcpserver

import (
	"context"
	"net"
	"sync"
	"sync/atomic"

	"github.com/HwHgoo/Gredis/connection"
)

type Handler struct {
	closing atomic.Bool

	_register   chan net.Conn
	connections map[*connection.Connection]struct{}
	conn_lock   sync.RWMutex
}

func (h *Handler) Handle(ctx context.Context, conn net.Conn) {
	if h.closing.Load() {
		conn.Close()
		return
	}

	c := connection.MakeConnection(conn)

	{
		h.conn_lock.Lock()
		defer h.conn_lock.Unlock()
		h.connections[c] = struct{}{}
	}

}
