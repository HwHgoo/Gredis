package server

import (
	"log"
	"strings"

	"github.com/HwHgoo/Gredis/connection"
	"github.com/HwHgoo/Gredis/core/command"
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
	cmdName := strings.ToLower(string(args[0]))
	if !command.Exists(cmdName) {
		return protocol.MakeUnknownCommandError(cmdName, argStartWith(args[1:]))
	}

	if !command.ValidateArity(cmdName, args) {
		return protocol.MakeWrongNumberOfArgError(cmdName)
	}

	if command.IsServerCommand(cmdName) {
		return command.ExecServerCommand(cmdName, c, args[1:])
	}
	db := s.databases[c.GetSelectedDb()]
	return db.Exec(c, args)
}

func (s *Server) Close() {
	log.Println("Redis server closing.")
}

func argStartWith(args [][]byte) string {
	if len(args) == 0 || len(args[0]) == 0 {
		return ""
	}

	return string(args[0][0])
}
