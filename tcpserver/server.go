package tcpserver

import (
	"context"
	"log"
	"net"
	"os"
	"sync"

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

func (s *Server) ListenAndServe(signals <-chan os.Signal) {
	lsn, err := net.Listen("tcp", ":3301")
	if err != nil {
		return
	}

	go func() {
		sig := <-signals
		log.Println("received signal:", sig)
		_ = lsn.Close()
		log.Println("closing handler")
		s.handler.Close()
	}()

	waitHandler := &sync.WaitGroup{}
	for {
		conn, err := lsn.Accept()
		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				continue
			}

			log.Println(err)
			break
		}

		waitHandler.Add(1)
		go func() {
			defer waitHandler.Done()
			s.handler.Handle(context.Background(), conn)
		}()
	}

	waitHandler.Wait()
}
