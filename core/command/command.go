package command

import (
	"github.com/HwHgoo/Gredis/connection"
	"github.com/HwHgoo/Gredis/core/interface/redis"
	"github.com/HwHgoo/Gredis/core/protocol"
)

type DatabaseCommandExecutor func(db redis.DB, args [][]byte) protocol.RedisMessage
type ServerCommandExecutor func(conn *connection.Connection, args [][]byte) protocol.RedisMessage

type CommandExecutor interface {
	DatabaseCommandExecutor | ServerCommandExecutor
}

type Command[T CommandExecutor] struct {
	name string
	// including command itself
	// positive arity means exact number of arguments
	// negative arity means at least abs(arity) arguments
	arity int
	exec  T
}

var dbCommands = make(map[string]*Command[DatabaseCommandExecutor])
var serverCommands = make(map[string]*Command[ServerCommandExecutor])

func Register[T CommandExecutor](name string, arity int, exec T) {
	switch executer := any(exec).(type) {
	case DatabaseCommandExecutor:
		dbCommands[name] = &Command[DatabaseCommandExecutor]{name, arity, executer}
	case ServerCommandExecutor:
		serverCommands[name] = &Command[ServerCommandExecutor]{name, arity, executer}
	default:
		panic("unknown executer type")
	}
}

func init() {
}

func IsServerCommand(name string) bool {
	_, ok := serverCommands[name]
	return ok
}

func IsDbCommand(name string) bool {
	_, ok := dbCommands[name]
	return ok
}

// return true if command exists
func Exists(name string) bool {
	_, ok := serverCommands[name]
	if ok {
		return true
	}

	_, ok = dbCommands[name]
	return ok
}

func ExecServerCommand(name string, conn *connection.Connection, args [][]byte) protocol.RedisMessage {
	cmd := serverCommands[name]
	return cmd.exec(conn, args)
}

func ExecDatabaseCommand(name string, db redis.DB, args [][]byte) protocol.RedisMessage {
	cmd := dbCommands[name]
	return cmd.exec(db, args)
}

func ValidateArity(name string, args [][]byte) bool {
	var arity int
	if cmd := serverCommands[name]; cmd != nil {
		arity = cmd.arity
	} else if cmd := dbCommands[name]; cmd != nil {
		arity = cmd.arity
	}
	return (arity > 0 && len(args) == arity) ||
		(arity < 0 && len(args) >= -arity)
}
