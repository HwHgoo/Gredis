package tcpserver

import (
	"context"
	"io"
	"net"
	"sync"
	"sync/atomic"

	"github.com/HwHgoo/Gredis/connection"
	"github.com/HwHgoo/Gredis/core/parser"
	"github.com/HwHgoo/Gredis/core/protocol"
	"github.com/HwHgoo/Gredis/core/server"
)

type Handler struct {
	closing atomic.Bool

	connections map[*connection.Connection]struct{}
	conn_lock   sync.RWMutex

	redis *server.Server
}

func MakeHandler(redis_server *server.Server) *Handler {
	return &Handler{
		connections: make(map[*connection.Connection]struct{}),
		redis:       redis_server,
	}
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

	ch := parser.Parse(conn)
	for payload := range ch {
		if err := payload.Err(); err != nil {
			if err == io.EOF { // connection closed
				break
			}

			// make error response
			msg := protocol.MakeError(err)
			c.Write(msg.Bytes())
			continue
		}

		result := h.redis.Exec(c, payload.Msg().Args())
		c.Write(result.Bytes())
	}

}
