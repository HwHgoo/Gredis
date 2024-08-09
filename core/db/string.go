package db

import (
	"math/big"
	"strconv"
	"strings"
	"time"

	"github.com/HwHgoo/Gredis/core/protocol"
	"github.com/HwHgoo/Gredis/global"
	"github.com/HwHgoo/Gredis/utils"
	"github.com/HwHgoo/Gredis/utils/pool"
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
func parseExtendedStringArgumentOrReply(args CommandParams, flags *int, command_type int) (protocol.RedisErrorMessage, time.Duration) {
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
				return &protocol.InvalidIntegerError, 0
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
				return &protocol.InvalidIntegerError, 0
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
				return &protocol.InvalidIntegerError, 0
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
				return &protocol.InvalidIntegerError, 0
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

/************************************* GET ************************************/

func getCommand(db *Database, args CommandParams) protocol.RedisMessage {
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
func getdelCommand(db *Database, args CommandParams) protocol.RedisMessage {
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
func getexCommand(db *Database, args CommandParams) protocol.RedisMessage {
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

// GETRANGE get a substring of the string stored at key
func getrangeCommand(db *Database, args CommandParams) protocol.RedisMessage {
	key := string(args[0])
	start, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return &protocol.InvalidIntegerError
	}
	end, err := strconv.ParseInt(string(args[2]), 10, 64)
	if err != nil {
		return &protocol.InvalidIntegerError
	}

	if start > end {
		return protocol.MakeBulkString(nil)
	}

	b, rerr := db.getAsString(key)
	if rerr != nil {
		return rerr
	}

	if b == nil {
		return protocol.MakeBulkString(nil)
	}

	if start >= int64(len(b)) {
		return protocol.MakeBulkString(nil)
	}

	if end >= int64(len(b)) {
		end = int64(len(b)) - 1
	}

	length := int64(len(b))
	if start < 0 {
		start += length
		end += length
	}

	return protocol.MakeBulkString(b[start : end+1])
}

func mgetCommand(db *Database, args CommandParams) protocol.RedisMessage {
	keys := make([]string, 0, len(args))
	for _, arg := range args {
		keys = append(keys, strings.ToLower(string(arg)))
	}

	values := make([]protocol.RedisMessage, 0, len(args))
	for _, key := range keys {
		v, err := db.getAsString(key)
		if err != nil {
			return err
		}

		if v == nil {
			values = append(values, &protocol.RedisNil)
		} else {
			values = append(values, protocol.MakeBulkString(v))
		}
	}

	return protocol.MakeArray(values)
}

// GETSET is deprecated, use SET with GET option instead.

/************************************* INCR/DECR ************************************/

func incrdecrGeneric(db *Database, key string, delta int64) protocol.RedisMessage {
	b, err := db.getAsString(key)
	if err != nil {
		return err
	}

	n, e := strconv.ParseInt(string(b), 10, 64)
	if e != nil {
		return protocol.InvalidIntegerError
	}

	n += delta
	v := strconv.FormatInt(n, 10)
	db.Set(key, []byte(v))

	return protocol.MakeInteger(n)
}

func incrCommand(db *Database, args CommandParams) protocol.RedisMessage {
	key := string(args[0])
	return incrdecrGeneric(db, key, 1)
}

func incrbyCommand(db *Database, args CommandParams) protocol.RedisMessage {
	key := string(args[0])
	delta, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return &protocol.InvalidIntegerError
	}

	return incrdecrGeneric(db, key, delta)
}

func decrCommand(db *Database, args CommandParams) protocol.RedisMessage {
	key := string(args[0])
	return incrdecrGeneric(db, key, -1)
}

func decrbyCommand(db *Database, args CommandParams) protocol.RedisMessage {
	key := string(args[0])
	delta, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return &protocol.InvalidIntegerError
	}
	return incrdecrGeneric(db, key, -delta)
}

var bigfloats = pool.MakePool(1024, func() *big.Float {
	f := big.NewFloat(0)
	return f.SetPrec(80) // simulate long double
})

// NOT as sensiable as redis
func incrbyfloatCommand(db *Database, args CommandParams) protocol.RedisMessage {
	key := string(args[0])
	var err error
	delta := bigfloats.Get()
	defer bigfloats.Put(delta)
	delta, _, err = delta.Parse(string(args[1]), 0)
	if err != nil {
		return &protocol.InvalidFloatError
	}

	f, msg := db.getAsString(key)
	if msg != nil {
		return msg
	}
	fv := bigfloats.Get()
	defer bigfloats.Put(fv)
	fv, _, err = fv.Parse(string(f), 0)
	if err != nil {
		return protocol.InvalidFloatError
	}

	fv = fv.Add(fv, delta)
	res := strings.TrimRight(fv.Text('f', 17), "0")
	db.Set(key, []byte(res))
	return protocol.MakeBulkString([]byte(res))
}

/************************************* SET ************************************/

func setCommand(db *Database, args CommandParams) protocol.RedisMessage {
	key := string(args[0])
	value := args[1]
	flag := flag_no_flag
	err, ttl := parseExtendedStringArgumentOrReply(args[2:], &flag, command_set)
	if err != nil {
		return err
	}

	result := global.ERR
	if flag&flag_set_nx != 0 {
		result = db.SetIfAbsent(key, value)
	} else if flag&flag_set_xx != 0 {
		result = db.SetIfExist(key, value)
	} else {
		db.Set(key, value)
		result = global.OK
	}

	if flag&flag_keepttl != 0 {
		db.Persist(key)
	} else if withFlags(flag, flag_ex, flag_px, flag_exat, flag_pxat) {
		db.Expire(key, time.Now().Add(ttl))
	}

	if withFlags(flag, flag_set_get) {
		return getCommand(db, args)
	}

	if result == global.OK {
		return &protocol.RedisOk
	}
	return &protocol.RedisNil
}

// Deprecated
func setnxCommand(db *Database, args CommandParams) protocol.RedisMessage {
	return protocol.MakeBulkString([]byte("Deprecated. Use SET with NX option instead."))
}

// Deprecated
func setexCommand(db *Database, args CommandParams) protocol.RedisMessage {
	return protocol.MakeBulkString([]byte("Deprecated. Use SET with EX option instead."))
}

// Deprecated
func psetexCommand(db *Database, args CommandParams) protocol.RedisMessage {
	return protocol.MakeBulkString([]byte("Deprecated. Use SET with PX option instead."))
}

func setrangeCommand(db *Database, args CommandParams) protocol.RedisMessage {
	key := string(args[0])
	offset, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return &protocol.InvalidIntegerError
	}
	if offset < 0 {
		return &protocol.OffsetOutofRangeError
	}

	prefix, msg := db.getAsString(key)
	if msg != nil {
		return msg
	}
	suffix := args[2]
	if len(suffix) == 0 {
		return protocol.MakeInteger(int64(len(prefix)))
	}

	newval := make([]byte, offset+int64(len(suffix)))
	copy(newval, prefix)
	copy(newval[offset:], suffix)
	db.Set(key, newval)
	return protocol.MakeInteger(int64(len(newval)))
}

func msetCommand(db *Database, args CommandParams) protocol.RedisMessage {
	if len(args)%2 != 0 {
		return protocol.MakeWrongNumberOfArgError("mset")
	}

	for i := 0; i < len(args); i += 2 {
		key := string(args[i])
		db.Set(key, args[i+1])
	}

	return &protocol.RedisOk
}

func delCommand(db *Database, args CommandParams) protocol.RedisMessage {
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

/************************************* OTHER ************************************/

func appendCommand(db *Database, args CommandParams) protocol.RedisMessage {
	key := string(args[0])
	suffix := args[1]

	prefix, msg := db.getAsString(key)
	if msg != nil {
		return msg
	}

	if prefix == nil {
		db.Set(key, suffix)
		return protocol.MakeInteger(int64(len(suffix)))
	}

	newval := append(prefix, suffix...)
	db.Set(key, newval)
	return protocol.MakeInteger(int64(len(newval)))
}

func strlenCommand(db *Database, args CommandParams) protocol.RedisMessage {
	key := string(args[0])
	v, msg := db.getAsString(key)
	if msg != nil {
		return msg
	}

	return protocol.MakeInteger(int64(len(v)))
}

// Deprecated
func substrCommand(db *Database, args CommandParams) protocol.RedisMessage {
	return protocol.MakeBulkString([]byte("Deprecated. Use GETRANGE instead."))
}

func lcsCommand(db *Database, args CommandParams) protocol.RedisMessage {
	key1, key2 := string(args[0]), string(args[1])
	idx, withmatchlen := false, false
	minmatchlen := int64(0)
	for i := 2; i < len(args); i++ {
		arg := strings.ToLower(string(args[i]))
		if arg == "idx" {
			idx = true
		} else if arg == "withmatchlen" {
			withmatchlen = true
		} else if arg == "minmatchlen" {
			if i+1 >= len(args) {
				return &protocol.SyntaxError
			}
			minlen, err := strconv.ParseInt(string(args[i+1]), 10, 64)
			if err != nil {
				return &protocol.InvalidIntegerError
			}
			minmatchlen = minlen
		} else {
			return &protocol.SyntaxError
		}
	}

	var s1, s2 []byte
	var msg protocol.RedisErrorMessage
	s1, msg = db.getAsString(key1)
	if msg != nil {
		return msg
	}

	nomatch := protocol.MakeArray([]protocol.RedisMessage{
		protocol.MakeBulkString([]byte("matches")),
		protocol.MakeArray(nil),
		protocol.MakeBulkString([]byte("len")),
		protocol.MakeInteger(0),
	})
	if s1 == nil {
		return nomatch
	}

	s2, msg = db.getAsString(key2)
	if msg != nil {
		return msg
	}
	if s2 == nil {
		return nomatch
	}

	dp := make([][]int, len(s1)+1)
	for i := range dp {
		dp[i] = make([]int, len(s2)+1)
	}

	for i := 1; i <= len(s2); i++ {
		for j := 1; j <= len(s1); j++ {
			if s2[i-1] == s1[j-1] {
				dp[j][i] = dp[j-1][i-1] + 1
			} else {
				dp[j][i] = max(dp[j-1][i], dp[j][i-1])
			}
		}
	}

	lcsLen := dp[len(s1)][len(s2)]
	lcs := make([]byte, lcsLen)

	index := lcsLen - 1
	var s1idx, s2idx []int
	if idx {
		s1idx, s2idx = make([]int, lcsLen+1), make([]int, lcsLen+1)
	}
	for i, j := len(s1), len(s2); index >= 0 && i > 0 && j > 0; {
		if dp[i][j] == dp[i-1][j-1]+1 && s2[j-1] == s1[i-1] {
			lcs[index] = s1[i-1]
			if idx {
				s1idx[index], s2idx[index] = i-1, j-1
			}
			index, i, j = index-1, i-1, j-1
		} else if dp[i][j] == dp[i-1][j] {
			i--
		} else {
			j--
		}
	}

	if !idx {
		return protocol.MakeBulkString([]byte(lcs))
	}

	matches := make([]protocol.RedisMessage, 0)
	s1idx[lcsLen], s2idx[lcsLen] = s1idx[lcsLen-1]+1, s2idx[lcsLen-1]+1
	last := lcsLen
	makematch := func(start, end int) protocol.RedisMessage {
		elems := []protocol.RedisMessage{
			protocol.MakeArray([]protocol.RedisMessage{
				protocol.MakeInteger(int64(s1idx[start])),
				protocol.MakeInteger(int64(s1idx[end])),
			}),

			protocol.MakeArray([]protocol.RedisMessage{
				protocol.MakeInteger(int64(s2idx[start])),
				protocol.MakeInteger(int64(s2idx[end])),
			}),

			utils.TerneryOp(withmatchlen, protocol.MakeInteger(int64(end-start+1)), nil),
		}
		if !withmatchlen {
			elems = elems[:len(elems)-1]
		}

		return protocol.MakeArray(elems)
	}
	for i := lcsLen - 1; i >= 0; i-- {
		if s1idx[i]+1 == s1idx[i+1] && s2idx[i]+1 == s2idx[i+1] {
			continue
		}

		length := last - i
		if length >= int(minmatchlen) {
			matches = append(matches, makematch(i+1, last-1))
		}
		last = i + 1
	}

	if last != 0 {
		matches = append(matches, makematch(0, last-1))
	}

	return protocol.MakeArray([]protocol.RedisMessage{
		protocol.MakeBulkString([]byte("matches")),
		protocol.MakeArray(matches),
		protocol.MakeBulkString([]byte("len")),
		protocol.MakeInteger(int64(lcsLen)),
	})
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

func registerStringCommands() {
	// string commands
	RegisterCommand("set", -3, setCommand)
	RegisterCommand("mset", -3, msetCommand)
	RegisterCommand("setrange", 4, setrangeCommand)
	RegisterCommand("del", -2, delCommand)
	RegisterCommand("get", 2, getCommand)
	RegisterCommand("getdel", 2, getdelCommand)
	RegisterCommand("getex", -2, getexCommand)
	RegisterCommand("getrange", 4, getrangeCommand)
	RegisterCommand("mget", -2, mgetCommand)
	RegisterCommand("incr", 2, incrCommand)
	RegisterCommand("incrby", 3, incrbyCommand)
	RegisterCommand("decr", 2, decrCommand)
	RegisterCommand("decrby", 3, decrbyCommand)
	RegisterCommand("incrbyfloat", 3, incrbyfloatCommand)
	RegisterCommand("append", 3, appendCommand)
	RegisterCommand("lcs", -3, lcsCommand)
	RegisterCommand("strlen", 2, strlenCommand)
}
