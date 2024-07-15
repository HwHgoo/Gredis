package tcpserver

import (
	"context"
	"io"
	"log"
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
	defer log.Println("End handling")
	if h.closing.Load() {
		conn.Close()
		return
	}

	c := connection.MakeConnection(conn)

	h.conn_lock.Lock()
	h.connections[c] = struct{}{}
	h.conn_lock.Unlock()

	ch := parser.Parse(conn)
	for payload := range ch {
		if err := payload.Err(); err != nil {
			if err == io.EOF { // connection closed
				log.Println("Connecton closed")
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

func (h *Handler) Close() {
	h.closing.Store(true)
	h.conn_lock.Lock()
	for c := range h.connections {
		c.Close()
	}
	h.conn_lock.Unlock()

	h.redis.Close()
}
