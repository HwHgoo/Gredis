package db

import (
	"strconv"
	"strings"
	"time"

	"github.com/HwHgoo/Gredis/core/protocol"
	"github.com/HwHgoo/Gredis/global"
)

func (db *Database) getAsString(key string) ([]byte, protocol.RedisErrorMessage) {
	v, ok := db.Get(key)
	if !ok {
		return nil, nil
	}

	s, ok := v.([]byte)
	if !ok {
		return nil, &protocol.WrongTypeError
	}

	return s, nil
}

const (
	upsert = iota // default set
	insert        // nx
	update        // xx
)

func get(db *Database, args [][]byte) protocol.RedisMessage {
	key := string(args[0])
	bs, err := db.getAsString(key)
	if err != nil {
		return err
	}

	if bs == nil {
		return protocol.MakeNil()
	}

	return protocol.MakeBulkString(bs)
}

func set(db *Database, args [][]byte) protocol.RedisMessage {
	key := string(args[0])
	value := args[1]
	setType := upsert
	ttl := time.Second * 0
	flagGet := false
	flagKeepttl := false

	for i := 2; i < len(args); i++ {
		arg := strings.ToLower(string(args[i]))
		if arg == "nx" {
			if setType == update || flagGet {
				return &protocol.SyntaxError
			}
			setType = insert
		} else if arg == "xx" {
			if setType == insert {
				return &protocol.SyntaxError
			}
			setType = update
		} else if arg == "ex" {
			if ttl != 0 {
				return &protocol.SyntaxError
			}

			if i+1 >= len(args) {
				return &protocol.SyntaxError
			}

			ex, err := strconv.ParseInt(string(args[i+1]), 10, 64)
			if err != nil {
				return &protocol.InvalidNumberError
			}

			if ex <= 0 {
				return &protocol.InvalidExpireTimeError
			}

			ttl = time.Second * time.Duration(ex)
			i++
		} else if arg == "px" {
			if ttl != 0 || i+1 >= len(args) {
				return &protocol.SyntaxError
			}

			px, err := strconv.ParseInt(string(args[i+1]), 10, 64)
			if err != nil {
				return &protocol.InvalidNumberError
			}

			if px <= 0 {
				return &protocol.InvalidExpireTimeError
			}

			ttl = time.Millisecond * time.Duration(px)
			i++
		} else if arg == "keepttl" {
			if setType != upsert {
				return &protocol.SyntaxError
			}
			flagKeepttl = true
		} else if arg == "get" {
			if setType == insert {
				return &protocol.SyntaxError
			}
			flagGet = true
		} else {
			return &protocol.SyntaxError
		}
	}

	var getMsg protocol.RedisMessage

	if flagGet {
		getMsg = get(db, args)
	}

	result := global.ERR
	if setType == upsert {
		db.Set(key, value)
		result = global.OK
	} else if setType == insert {
		result = db.SetIfAbsent(key, value)
	} else {
		result = db.SetIfExist(key, value)
	}

	if ttl > 0 {
		expireAt := time.Now().Add(ttl)
		db.Expire(key, expireAt)
	} else if !flagKeepttl {
		db.Persist(key)
	}

	if flagGet {
		return getMsg
	}

	if result == global.OK {
		return &protocol.RedisOk
	}
	return &protocol.RedisNil
}

func del(db *Database, args [][]byte) protocol.RedisMessage {
	keys := make([]string, 0, len(args))
	for _, arg := range args {
		keys = append(keys, string(arg))
	}

	result := 0
	for _, key := range keys {
		result += db.Delete(key)
	}

	return protocol.MakeInteger(int64(result))
}

func init() {
	RegisterCommand("get", 2, get)
	RegisterCommand("set", -3, set)
	RegisterCommand("del", -2, del)
}
