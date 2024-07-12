package server

import (
	"github.com/HwHgoo/Gredis/connection"
	"github.com/HwHgoo/Gredis/core/db"
	"github.com/HwHgoo/Gredis/core/protocol"
)

const (
	db_num = 16
)

// Redis server
type Server struct {
	databases [db_num]*db.Database
}

func MakeServer() *Server {
	server := &Server{}
	for i := 0; i < db_num; i++ {
		server.databases[i] = db.MakeDatabase()
	}
	return server
}

func (s *Server) Exec(c *connection.Connection, args [][]byte) protocol.RedisMessage {
	db := s.databases[c.GetSelectedDb()]
	return db.Exec(args)
}
