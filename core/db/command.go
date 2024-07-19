package db

import (
	"strings"

	"github.com/HwHgoo/Gredis/core/protocol"
)

type CommandParams [][]byte
type CommandExecutor func(db *Database, args CommandParams) protocol.RedisMessage

type Command struct {
	name string
	// including command itself
	// positive arity means exact number of arguments
	// negative arity means at least abs(arity) arguments
	arity int
	exec  CommandExecutor
}

var commands = make(map[string]*Command)

func RegisterCommand(name string, arity int, exec CommandExecutor) {
	name = strings.ToLower(name)
	commands[name] = &Command{name: name, arity: arity, exec: exec}
}
