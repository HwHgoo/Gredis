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

const (
	flag_no_flag = 1 << iota
	flag_set_nx
	flag_set_xx
	flag_ex
	flag_px
	flag_keepttl
	flag_set_get
	flag_exat
	flag_pxat
	flag_persist
)

const (
	command_set = iota
	command_get
)

/* The parseExtendedStringArgumentOrReply function parses the extended string argument used in SET and GET command.
 * GET specific commands - PERSIST
 * SET specific commands - XX/NX/GET/KEEPTTL
 * Common commands - EX/PX/EXAT/PXAT
 * Function takes arguments of command in which key not included, flags and command_type which can be command_set or command_get.
 * If there are any syntax violations protocol.SyntaxError is returned else nil is returned.
 * If nil is returned, a duration value is also returned if needed.
 * The unit of duraton is always in milliseconds.
 */
func parseExtendedStringArgumentOrReply(args [][]byte, flags *int, command_type int) (protocol.RedisMessage, time.Duration) {
	d := time.Duration(0)
	unixms := time.Now().UnixMilli()
	for i := 0; i < len(args); i++ {
		arg := strings.ToLower(string(args[i]))
		if arg == "ex" {
			if withFlags(*flags, flag_px, flag_exat, flag_pxat, flag_persist, flag_keepttl) ||
				i+1 >= len(args) {
				return &protocol.SyntaxError, 0
			}
			*flags |= flag_ex
			ex, err := strconv.ParseInt(string(args[i+1]), 10, 64)
			if err != nil {
				return &protocol.InvalidNumberError, 0
			}
			if ex <= 0 {
				return &protocol.InvalidExpireTimeError, 0
			}
			d = time.Millisecond * time.Duration(ex*1000)
		} else if arg == "px" {
			if withFlags(*flags, flag_ex, flag_exat, flag_pxat, flag_persist, flag_keepttl) ||
				i+1 >= len(args) {
				return &protocol.SyntaxError, 0
			}

			*flags |= flag_px
			px, err := strconv.ParseInt(string(args[i+1]), 10, 64)
			if err != nil {
				return &protocol.InvalidNumberError, 0
			}
			if px <= 0 {
				return &protocol.InvalidExpireTimeError, 0
			}
			d = time.Millisecond * time.Duration(px)
		} else if arg == "exat" {
			if withFlags(*flags, flag_px, flag_ex, flag_pxat, flag_persist, flag_keepttl) ||
				i+1 >= len(args) {
				return &protocol.SyntaxError, 0
			}
			*flags |= flag_exat
			exat, err := strconv.ParseInt(string(args[i+1]), 10, 64)
			if err != nil {
				return &protocol.InvalidNumberError, 0
			}
			if exat <= 0 {
				return &protocol.InvalidExpireTimeError, 0
			}
			d = time.Millisecond * time.Duration(exat*1000-unixms)
		} else if arg == "pxat" {
			if withFlags(*flags, flag_px, flag_ex, flag_exat, flag_persist, flag_keepttl) ||
				i+1 >= len(args) {
				return &protocol.SyntaxError, 0
			}
			*flags |= flag_pxat
			pxat, err := strconv.ParseInt(string(args[i+1]), 10, 64)
			if err != nil {
				return &protocol.InvalidNumberError, 0
			}
			if pxat <= 0 {
				return &protocol.InvalidExpireTimeError, 0
			}
			d = time.Millisecond * time.Duration(pxat-unixms)
		} else if arg == "persist" {
			if command_type != command_get ||
				withFlags(*flags, flag_ex, flag_px, flag_exat, flag_pxat) {
				return &protocol.SyntaxError, 0
			}
			*flags |= flag_persist
		} else if arg == "nx" {
			if command_type != command_set ||
				withFlags(*flags, flag_set_xx) {
				return &protocol.SyntaxError, 0
			}
			*flags |= flag_set_nx
		} else if arg == "xx" {
			if command_type != command_set ||
				withFlags(*flags, flag_set_nx) {
				return &protocol.SyntaxError, 0
			}
			*flags |= flag_set_xx
		} else if arg == "get" {
			if command_type != command_set {
				return &protocol.SyntaxError, 0
			}
			*flags |= flag_set_get
		} else if arg == "keepttl" {
			if command_type != command_set ||
				withFlags(*flags, flag_ex, flag_px, flag_exat, flag_pxat) {
				return &protocol.SyntaxError, 0
			}
			*flags |= flag_keepttl
		}
	}

	return nil, d
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

// GETDEL get the value of key and delete the key
func getdel(db *Database, args [][]byte) protocol.RedisMessage {
	key := string(args[0])
	s, msg := db.getAsString(key)
	if msg != nil {
		return msg
	}

	if s == nil {
		return &protocol.RedisNil
	}

	db.Delete(key)
	return protocol.MakeBulkString(s)
}

// GETEX get the value of key and optionally set its expiration time
func getex(db *Database, args [][]byte) protocol.RedisMessage {
	key := string(args[0])
	flag := flag_no_flag
	err, ttl := parseExtendedStringArgumentOrReply(args[1:], &flag, command_get)
	if err != nil {
		return err
	}

	s, err := db.getAsString(key)
	if err != nil {
		return err
	}
	if s == nil {
		return &protocol.RedisNil
	}

	if flag != flag_no_flag {
		if flag&flag_persist != 0 {
			db.Persist(key)
		} else {
			db.Expire(key, time.Now().Add(ttl))
		}
	}

	return protocol.MakeBulkString(s)
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
	RegisterCommand("getdel", 2, getdel)
	RegisterCommand("getex", -2, getex)
}

// withFlags checks if the given flag contains some of the given flags.
func withFlags(flag int, flags ...int) bool {
	for _, f := range flags {
		if flag&f != 0 {
			return true
		}
	}
	return false
}
