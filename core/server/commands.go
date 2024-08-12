package server

import (
	"strconv"

	"github.com/HwHgoo/Gredis/connection"
	"github.com/HwHgoo/Gredis/core/command"
	"github.com/HwHgoo/Gredis/core/protocol"
)

func commandBgSave(conn *connection.Connection, args [][]byte) protocol.RedisMessage {
	return &protocol.RedisNotImplemented
}

func commandSelect(conn *connection.Connection, args [][]byte) protocol.RedisMessage {
	db := string(args[0])
	dbno, err := strconv.ParseInt(db, 10, 32)
	if err != nil {
		return &protocol.InvalidIntegerError
	}

	if dbno < 0 || dbno > 16 {
		return &protocol.DbIndexOutOfRange
	}
	conn.SelectDb(int(dbno))
	return &protocol.RedisOk
}

var register = command.Register[command.ServerCommandExecutor]

func init() {
	register("bgsave", 1, commandBgSave)
	register("select", 2, commandSelect)
}
