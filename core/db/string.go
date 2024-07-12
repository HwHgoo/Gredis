package db

import "github.com/HwHgoo/Gredis/core/protocol"

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
	db.Set(key, value)
	return &protocol.RedisOk
}

func init() {
	RegisterCommand("get", 2, get)
	RegisterCommand("set", -3, set)
}
