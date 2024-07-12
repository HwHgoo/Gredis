package tcpserver

import (
	"context"
	"net"

	"github.com/HwHgoo/Gredis/core/server"
)

type Server struct {
	handler *Handler
}

func MakeTcpServer() *Server {
	redis := server.MakeServer()
	return &Server{
		handler: MakeHandler(redis),
	}
}

func (s *Server) ListenAndServe() {
	lsn, err := net.Listen("tcp", ":3301")
	if err != nil {
		return
	}

	for {
		conn, err := lsn.Accept()
		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				continue
			}

			break
		}

		go s.handler.Handle(context.Background(), conn)

	}
}
