package db

import (
	"github.com/HwHgoo/Gredis/core/command"
	"github.com/HwHgoo/Gredis/core/interface/redis"
	"github.com/HwHgoo/Gredis/core/protocol"
)

type CommandParams [][]byte
type CommandExecutor func(db *Database, args CommandParams) protocol.RedisMessage

func register(name string, arity int, exec CommandExecutor) {
	command.Register[command.DatabaseCommandExecutor](name, arity, func(db redis.DB, args [][]byte) protocol.RedisMessage {
		database := db.(*Database)
		argsParams := CommandParams(args)
		return exec(database, argsParams)
	})
}

func init() {
	registerStringCommands()
	registerZSetCommands()
}
