package command

import "github.com/HwHgoo/Gredis/core/protocol"

type CommandExecutor func(args [][]byte) protocol.RedisMessage

type Command struct {
	name  string
	arity int
	exec  CommandExecutor
}

var commands = make(map[string]*Command)
