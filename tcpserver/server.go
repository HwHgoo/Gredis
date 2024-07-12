package tcpserver

import (
	"context"
	"net"
)

type Server struct {
	handler Handler
}

func MakeServer() *Server {
	return &Server{}
}

func (s *Server) ListenAndServe() {
	lsn, err := net.Listen("tcp", ":8080")
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
