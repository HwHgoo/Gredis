package db

import (
	"strings"

	"github.com/HwHgoo/Gredis/core/protocol"
	"github.com/HwHgoo/Gredis/datastructure"
)

// redis db

type Database struct {
	index int

	data *datastructure.ConcurrentMap[any]
}

func MakeDatabase() *Database {
	return &Database{
		data: datastructure.MakeNewConcurrentMap[any](),
	}
}

func (db *Database) Get(key string) (value any, ok bool) {
	return db.data.Get(key)
}

func (db *Database) Set(key string, value any) {
	db.data.Set(key, value)
}

func (db *Database) Delete(key string, value any) {}

func (db *Database) Exec(args [][]byte) protocol.RedisMessage {
	cmdName := strings.ToLower(string(args[0]))
	cmd, ok := commands[cmdName]
	if !ok {
		var startwith string
		if len(args) > 1 && len(args[1]) > 0 {
			startwith = string(args[1][0])
		}
		return protocol.MakeUnknownCommandError(cmdName, startwith)
	}

	return cmd.exec(db, args[1:])
}
