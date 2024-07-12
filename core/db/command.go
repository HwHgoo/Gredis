package db

import (
	"strings"

	"github.com/HwHgoo/Gredis/core/protocol"
)

type CommandExecutor func(db *Database, args [][]byte) protocol.RedisMessage

type Command struct {
	name  string
	arity int
	exec  CommandExecutor
}

var commands = make(map[string]*Command)

func RegisterCommand(name string, arity int, exec CommandExecutor) {
	name = strings.ToLower(name)
	commands[name] = &Command{name: name, arity: arity, exec: exec}
}
