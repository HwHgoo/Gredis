package db

import (
	"strings"
	"time"

	"github.com/HwHgoo/Gredis/core/command"
	"github.com/HwHgoo/Gredis/core/interface/redis"
	"github.com/HwHgoo/Gredis/core/protocol"
	"github.com/HwHgoo/Gredis/datastructure"
)

// redis db

type Database struct {
	index int

	data    *datastructure.ConcurrentMap[any]
	expires *datastructure.ConcurrentMap[time.Time]
}

// TODO optimize for operation like mget, mset
func MakeDatabase() *Database {
	return &Database{
		data:    datastructure.MakeNewConcurrentMap[any](),
		expires: datastructure.MakeNewConcurrentMap[time.Time](),
	}
}

func (db *Database) Exec(conn redis.Connection, args [][]byte) protocol.RedisMessage {
	return db.execNormal(args)
}

func (db *Database) Get(key string) (value any, ok bool) {
	if db.IsExpired(key) {
		return nil, false
	}

	return db.data.Get(key)
}

func (db *Database) Set(key string, value any) {
	db.data.Set(key, value)
}

func (db *Database) SetIfAbsent(key string, value any) int {
	_, ok := db.data.Get(key)
	if ok {
		return 0
	}

	db.data.Set(key, value)
	return 1
}

func (db *Database) SetIfExist(key string, value any) int {
	_, ok := db.data.Get(key)
	if !ok {
		return 0
	}
	db.data.Set(key, value)
	return 1
}

func (db *Database) Delete(key string) int {
	_, ok := db.data.Get(key)
	if !ok {
		return 0
	}

	db.data.Delete(key)
	return 1
}

func (db *Database) Expire(key string, expireAt time.Time) {
	db.expires.Set(key, expireAt)
}

func (db *Database) Persist(key string) {
	db.expires.Delete(key)
}

func (db *Database) execNormal(args [][]byte) protocol.RedisMessage {
	cmdName := strings.ToLower(string(args[0]))
	return command.ExecDatabaseCommand(cmdName, db, args[1:])
}

// check if key is expired
// and delete the key if it's expired
func (db *Database) IsExpired(key string) bool {
	t, ok := db.expires.Get(key)
	if !ok {
		return false
	}

	expired := time.Now().After(t)
	if expired {
		db.data.Delete(key)
	}

	return expired
}
