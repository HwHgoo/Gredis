package db

import (
	"strings"
	"time"

	"github.com/HwHgoo/Gredis/core/protocol"
	"github.com/HwHgoo/Gredis/datastructure"
)

// redis db

type Database struct {
	index int

	data    *datastructure.ConcurrentMap[any]
	expires *datastructure.ConcurrentMap[time.Time]
}

func MakeDatabase() *Database {
	return &Database{
		data:    datastructure.MakeNewConcurrentMap[any](),
		expires: datastructure.MakeNewConcurrentMap[time.Time](),
	}
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

	// validate args
	if !db.validateArity(cmd, args) {
		return protocol.MakeWrongNumberOfArgError(cmdName)
	}

	return cmd.exec(db, args[1:])
}

func (db *Database) validateArity(cmd *Command, args [][]byte) bool {
	return (cmd.arity >= 0 && len(args) == cmd.arity) ||
		(cmd.arity < 0 && len(args) >= -cmd.arity)
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
