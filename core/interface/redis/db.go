package redis

import (
	"github.com/HwHgoo/Gredis/core/protocol"
)

type CommandLine [][]byte

type DB interface {
	Exec(Connection, [][]byte) protocol.RedisMessage
}
