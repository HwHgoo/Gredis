package db

import (
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/HwHgoo/Gredis/core/protocol"
	. "github.com/agiledragon/gomonkey/v2"
	. "github.com/smartystreets/goconvey/convey"
)

var (
	patchdb *Database

	simGetExistOk = func(patch *Patches) *Patches {
		return patch.ApplyPrivateMethod(patchdb, "getAsString", func(_ *Database, _ string) ([]byte, protocol.RedisMessage) {
			return []byte("test"), nil
		})
	}

	simGetNonExistKey = func(patch *Patches) *Patches {
		return patch.ApplyPrivateMethod(patchdb, "getAsString", func(_ *Database, _ string) ([]byte, protocol.RedisMessage) {
			return nil, nil
		})
	}

	simGetWrongType = func(patch *Patches) *Patches {
		return patch.ApplyPrivateMethod(patchdb, "getAsString", func(_ *Database, _ string) ([]byte, protocol.RedisMessage) {
			return nil, &protocol.WrongTypeError
		})
	}

	simParseExtendedStringParamError = func(patch *Patches) *Patches {
		return patch.ApplyFuncReturn(parseExtendedStringArgumentOrReply, &protocol.SyntaxError, time.Duration(0))
	}
	simParseExtendedStringParamWithSpecificFlags = func(patch *Patches, targetFlags ...int) *Patches {
		return ApplyFunc(parseExtendedStringArgumentOrReply, func(_ CommandParams, flags *int, _ int) (protocol.RedisMessage, time.Duration) {
			*flags = 0
			for _, f := range targetFlags {
				*flags |= f
			}

			return nil, time.Duration(0)
		})
	}
	simParseExtendedStringParamWithTtl = func(patch *Patches, ttl time.Duration) *Patches {
		return patch.ApplyFuncReturn(TestParseExtendedStringArgumentOrReply, nil, ttl)
	}
)

func TestGetAsString(t *testing.T) {
	db := MakeDatabase()
	type testcase struct {
		name    string
		key     string
		args    string
		command CommandExecutor
		expect  []any
		patch   func() *Patches
	}

	patchBeforeGet := func() *Patches {
		var patchdb *Database
		return ApplyMethodReturn(patchdb, "Get", 1, true)
	}

	cases := []testcase{
		{"Get non-exist key", "a", "", nil, []any{[]byte(nil), nil}, nil},
		{"Get exist key", "a", "a 1", setCommand, []any{[]byte("1"), nil}, nil},
		{"Get wrong type", "a", "", nil, []any{[]byte(nil), &protocol.WrongTypeError}, patchBeforeGet},
	}

	Convey("TestGetAsString", t, func() {
		for _, c := range cases {
			Convey(c.name, func() {
				var patch *Patches
				if c.patch != nil {
					patch = c.patch()
					defer patch.Reset()
				}

				if c.command != nil {
					argstr := strings.Split(c.args, " ")
					args := make([][]byte, 0, len(argstr))
					for _, s := range argstr {
						args = append(args, []byte(s))
					}
					c.command(db, args)
				}
				v, msg := db.getAsString(c.key)
				So(v, ShouldEqual, c.expect[0])
				So(msg, ShouldEqual, c.expect[1])
			})
		}
	})
}

func TestParseExtendedStringArgumentOrReply(t *testing.T) {
	type expect struct {
		flag     int
		err      protocol.RedisMessage
		duration time.Duration
	}

	type tcase struct {
		name    string
		args    string
		command int
		expect  expect
	}
	Convey("TestParseExtendedStringArgumentOrReply", t, func() {
		nowmsec := time.Now().UnixMilli()
		const sec = int64(1721198037)
		const msec = int64(1721198037000)
		secstr, msecstr := strconv.FormatInt(sec, 10), strconv.FormatInt(msec, 10)

		secduration, msduration := time.Duration(sec*1000-nowmsec)*time.Millisecond, time.Duration(msec-nowmsec)*time.Millisecond
		cases := []tcase{
			{"CommandSet: Use ex only and correct second arg", "ex 1", command_set, expect{flag_ex, nil, time.Millisecond * 1000}},
			{"CommandSet: Use ex only and nagetive second arg", "ex -1", command_set, expect{flag_ex, &protocol.InvalidExpireTimeError, 0}},
			{"CommandSet: Use ex only and non-integer second arg", "ex 1.1", command_set, expect{flag_ex, &protocol.InvalidIntegerError, 0}},
			{"CommandSet: Use ex only and non-numberic second arg", "ex a123", command_set, expect{flag_ex, &protocol.InvalidIntegerError, 0}},
			{"CommandSet: Use ex only but no second arg", "ex", command_set, expect{flag_ex, &protocol.SyntaxError, 0}},
			{"CommandSet: Use px only and correct second arg", "px 1000", command_set, expect{flag_px, nil, time.Millisecond * 1000}},
			{"CommandSet: Use px only and nagetive second arg", "px -1", command_set, expect{flag_px, &protocol.InvalidExpireTimeError, 0}},
			{"CommandSet: Use px only and non-integer second arg", "px 1.1", command_set, expect{flag_px, &protocol.InvalidIntegerError, 0}},
			{"CommandSet: Use px only and non-numberic second arg", "px a123", command_set, expect{flag_px, &protocol.InvalidIntegerError, 0}},
			{"CommandSet: Use px only but no second arg", "px", command_set, expect{flag_px, &protocol.SyntaxError, 0}},
			{"CommandSet: Use exat only and correct second arg", "exat " + secstr, command_set, expect{flag_exat, nil, secduration}},
			{"CommandSet: Use exat only and negative second arg", "exat -1", command_set, expect{flag_px, &protocol.InvalidExpireTimeError, 0}},
			{"CommandSet: Use exat only and non-integer second arg", "exat 1.1", command_set, expect{flag_px, &protocol.InvalidIntegerError, 0}},
			{"CommandSet: Use exat only and non-numberic second arg", "exat -a1", command_set, expect{flag_px, &protocol.InvalidIntegerError, 0}},
			{"CommandSet: Use exat only but no second arg", "exat", command_set, expect{flag_exat, &protocol.SyntaxError, 0}},
			{"CommandSet: Use pxat only and correct second arg", "pxat " + msecstr, command_set, expect{flag_pxat, nil, msduration}},
			{"CommandSet: Use pxat only and negative second arg", "pxat -1", command_set, expect{flag_pxat, &protocol.InvalidExpireTimeError, 0}},
			{"CommandSet: Use pxat only and non-integer second arg", "pxat 1.1", command_set, expect{flag_pxat, &protocol.InvalidIntegerError, 0}},
			{"CommandSet: Use pxat only and non-numberic second arg", "pxat -a1", command_set, expect{flag_pxat, &protocol.InvalidIntegerError, 0}},
			{"CommandSet: Use pxat only but no second arg", "pxat", command_set, expect{flag_pxat, &protocol.SyntaxError, 0}},
			{"CommandSet: Use keepttl only", "keepttl", command_set, expect{flag_keepttl, nil, 0}},
			{"CommandSet: Use get only", "get", command_set, expect{flag_set_get, nil, 0}},
			{"CommandSet: Use nx only", "nx", command_set, expect{flag_set_nx, nil, 0}},
			{"CommandSet: Use xx only", "xx", command_set, expect{flag_set_xx, nil, 0}},
			{"CommandSet: Mixed use of nx and xx", "nx xx", command_set, expect{flag_set_nx, &protocol.SyntaxError, 0}},
			{"CommandSet: Misuse option persist", "persist", command_set, expect{flag_no_flag, &protocol.SyntaxError, 0}},
			{"CommandSet: Mixed use of ex and px", "ex 1 px 1", command_set, expect{flag_ex, &protocol.SyntaxError, 0}},
			{"CommandSet: Mixed use of exat and pxat", "exat 1 pxat 1", command_set, expect{flag_exat, &protocol.SyntaxError, 0}},
			{"CommandSet: Mixed use of ex and exat", "ex 1 exat 1", command_set, expect{flag_ex, &protocol.SyntaxError, 0}},
			{"CommandSet: Mixed use of ex and pxat", "ex 1 pxat 1", command_set, expect{flag_ex, &protocol.SyntaxError, 0}},
			{"CommandSet: Mixed use of px and exat", "px 1 exat 1", command_set, expect{flag_ex, &protocol.SyntaxError, 0}},
			{"CommandSet: Mixed use of px and pxat", "px 1 pxat 1", command_set, expect{flag_ex, &protocol.SyntaxError, 0}},
			{"CommandSet: Mixed use of nx and ex", "nx ex 1", command_set, expect{flag_set_nx | flag_ex, nil, time.Millisecond * 1000}},
			{"CommandSet: Mixed use of nx and px", "nx px 1", command_set, expect{flag_set_nx | flag_px, nil, time.Millisecond}},
			{"CommandSet: Mixed use of nx and exat", "nx exat " + secstr, command_set, expect{flag_set_nx | flag_exat, nil, secduration}},
			{"CommandSet: Mixed use of nx and pxat", "nx pxat " + msecstr, command_set, expect{flag_set_nx | flag_pxat, nil, msduration}},
			{"CommandSet: Mixed use of xx and ex", "xx ex 1", command_set, expect{flag_set_xx | flag_ex, nil, time.Millisecond * 1000}},
			{"CommandSet: Mixed use of xx and px", "xx px 1", command_set, expect{flag_set_xx | flag_px, nil, time.Millisecond}},
			{"CommandSet: Mixed use of xx and exat", "xx exat " + secstr, command_set, expect{flag_set_xx | flag_exat, nil, secduration}},
			{"CommandSet: Mixed use of xx and pxat", "xx pxat " + msecstr, command_set, expect{flag_set_xx | flag_pxat, nil, msduration}},

			// GET
			{"CommandGet: Use ex only and correct second arg", "ex 1", command_get, expect{flag_ex, nil, time.Millisecond * 1000}},
			{"CommandGet: Use ex only and nagetive second arg", "ex -1", command_get, expect{flag_ex, &protocol.InvalidExpireTimeError, 0}},
			{"CommandGet: Use ex only and non-integer second arg", "ex 1.1", command_get, expect{flag_ex, &protocol.InvalidIntegerError, 0}},
			{"CommandGet: Use ex only and non-numberic second arg", "ex a123", command_get, expect{flag_ex, &protocol.InvalidIntegerError, 0}},
			{"CommandGet: Use ex only but no second arg", "ex", command_get, expect{flag_ex, &protocol.SyntaxError, 0}},
			{"CommandGet: Use px only and correct second arg", "px 1000", command_get, expect{flag_px, nil, time.Millisecond * 1000}},
			{"CommandGet: Use px only and nagetive second arg", "px -1", command_get, expect{flag_px, &protocol.InvalidExpireTimeError, 0}},
			{"CommandGet: Use px only and non-integer second arg", "px 1.1", command_get, expect{flag_px, &protocol.InvalidIntegerError, 0}},
			{"CommandGet: Use px only and non-numberic second arg", "px a123", command_get, expect{flag_px, &protocol.InvalidIntegerError, 0}},
			{"CommandGet: Use px only but no second arg", "px", command_get, expect{flag_px, &protocol.SyntaxError, 0}},
			{"CommandGet: Use exat only and correct second arg", "exat " + secstr, command_get, expect{flag_exat, nil, secduration}},
			{"CommandGet: Use exat only and negative second arg", "exat -1", command_get, expect{flag_px, &protocol.InvalidExpireTimeError, 0}},
			{"CommandGet: Use exat only and non-integer second arg", "exat 1.1", command_get, expect{flag_px, &protocol.InvalidIntegerError, 0}},
			{"CommandGet: Use exat only and non-numberic second arg", "exat -a1", command_get, expect{flag_px, &protocol.InvalidIntegerError, 0}},
			{"CommandGet: Use exat only but no second arg", "exat", command_get, expect{flag_exat, &protocol.SyntaxError, 0}},
			{"CommandGet: Use pxat only and correct second arg", "pxat " + msecstr, command_get, expect{flag_pxat, nil, msduration}},
			{"CommandGet: Use pxat only and negative second arg", "pxat -1", command_get, expect{flag_pxat, &protocol.InvalidExpireTimeError, 0}},
			{"CommandGet: Use pxat only and non-integer second arg", "pxat 1.1", command_get, expect{flag_pxat, &protocol.InvalidIntegerError, 0}},
			{"CommandGet: Use pxat only and non-numberic second arg", "pxat -a1", command_get, expect{flag_pxat, &protocol.InvalidIntegerError, 0}},
			{"CommandGet: Use pxat only but no second arg", "pxat", command_get, expect{flag_pxat, &protocol.SyntaxError, 0}},
			{"CommandGet: Use persist only", "persist", command_get, expect{flag_persist, nil, 0}},
			{"CommandGet: Misuse option keepttl", "keepttl", command_get, expect{flag_no_flag, &protocol.SyntaxError, 0}},
			{"CommandGet: Misuse option nx", "nx", command_get, expect{flag_no_flag, &protocol.SyntaxError, 0}},
			{"CommandGet: Misuse option xx", "xx", command_get, expect{flag_no_flag, &protocol.SyntaxError, 0}},
			{"CommandGet: Misuse option get", "get", command_get, expect{flag_no_flag, &protocol.SyntaxError, 0}},
			{"CommandGet: Mixed use of ex and px", "ex 1 px 1", command_get, expect{flag_ex, &protocol.SyntaxError, 0}},
			{"CommandGet: Mixed use of exat and pxat", "exat 1 pxat 1", command_get, expect{flag_exat, &protocol.SyntaxError, 0}},
			{"CommandGet: Mixed use of ex and exat", "ex 1 exat 1", command_get, expect{flag_ex, &protocol.SyntaxError, 0}},
			{"CommandGet: Mixed use of ex and pxat", "ex 1 pxat 1", command_get, expect{flag_ex, &protocol.SyntaxError, 0}},
			{"CommandGet: Mixed use of px and exat", "px 1 exat 1", command_get, expect{flag_ex, &protocol.SyntaxError, 0}},
			{"CommandGet: Mixed use of px and pxat", "px 1 pxat 1", command_get, expect{flag_ex, &protocol.SyntaxError, 0}},
			{"CommandGet: Mixed use of nx and ex", "nx ex 1", command_get, expect{flag_no_flag, &protocol.SyntaxError, 0}},
			{"CommandGet: Mixed use of nx and px", "nx px 1", command_get, expect{flag_no_flag, &protocol.SyntaxError, 0}},
			{"CommandGet: Mixed use of nx and exat", "nx exat " + secstr, command_get, expect{flag_no_flag, &protocol.SyntaxError, 0}},
			{"CommandGet: Mixed use of nx and pxat", "nx pxat " + msecstr, command_get, expect{flag_no_flag, &protocol.SyntaxError, 0}},
			{"CommandGet: Mixed use of xx and ex", "xx ex 1", command_get, expect{flag_no_flag, &protocol.SyntaxError, 0}},
			{"CommandGet: Mixed use of xx and px", "xx px 1", command_get, expect{flag_no_flag, &protocol.SyntaxError, 0}},
			{"CommandGet: Mixed use of xx and exat", "xx exat " + secstr, command_get, expect{flag_no_flag, &protocol.SyntaxError, 0}},
			{"CommandGet: Mixed use of xx and pxat", "xx pxat " + msecstr, command_get, expect{flag_no_flag, &protocol.SyntaxError, 0}},
		}

		for _, c := range cases {
			Convey(c.name, func() {
				flag := 0
				strargs := strings.Split(c.args, " ")
				args := make([][]byte, 0, len(strargs))
				for _, s := range strargs {
					args = append(args, []byte(s))
				}
				msg, duration := parseExtendedStringArgumentOrReply(args, &flag, c.command)
				So(msg, ShouldEqual, c.expect.err)
				if msg == nil {
					So(flag, ShouldEqual, c.expect.flag)
					So(duration, ShouldAlmostEqual, c.expect.duration, 10000000) // take 10ms as margin
				} else {
					So(duration, ShouldEqual, 0)
				}
			})
		}
	})
}

func TestGetCommand(t *testing.T) {
	type testcase struct {
		name   string
		patch  func(patch *Patches) *Patches
		expect protocol.RedisMessage
	}

	cases := []testcase{
		{"Get exist key and value is string", simGetExistOk, protocol.MakeBulkString([]byte("test"))},
		{"Get non-exist key", simGetNonExistKey, protocol.MakeNil()},
		{"Get exist key but value is not string", simGetWrongType, &protocol.WrongTypeError},
	}
	Convey("TestGetCommand", t, func() {
		db := MakeDatabase()
		patch := NewPatches()
		for _, c := range cases {
			Convey(c.name, func() {
				patch := c.patch(patch)
				defer patch.Reset()

				msg := getCommand(db, [][]byte{[]byte("key")}) // no need to check args because it's checked before getCommand
				So(msg, ShouldEqual, c.expect)
			})
		}
	})
}

func TestGetDELCommand(t *testing.T) {

	type testcase struct {
		name   string
		patch  func(patch *Patches) *Patches
		expect protocol.RedisMessage
	}

	cases := []testcase{
		{"Get exist key and value is string", simGetExistOk, protocol.MakeBulkString([]byte("test"))},
		{"Get non-exist key", simGetNonExistKey, protocol.MakeNil()},
		{"Get exist key but value is not string", simGetWrongType, &protocol.WrongTypeError},
	}
	Convey("TestGetCommand", t, func() {
		db := MakeDatabase()
		patch := NewPatches()
		for _, c := range cases {
			Convey(c.name, func() {
				patch := c.patch(patch)
				defer patch.Reset()

				msg := getdelCommand(db, [][]byte{[]byte("key")}) // no need to check args because it's checked before getCommand
				So(msg, ShouldEqual, c.expect)
			})
		}
	})
}

func TestGetExCommand(t *testing.T) {
	type testcase struct {
		name   string
		patch  func(patch *Patches) *Patches
		expect protocol.RedisMessage
		extra  func(*Database, string) bool
	}

	okOnPersist := func(db *Database, key string) bool {
		_, expiring := db.expires.Get(key)
		return !expiring
	}

	okOnExpire := func(db *Database, key string) bool {
		_, expiring := db.expires.Get(key)
		return expiring
	}

	simEx100 := func(patch *Patches) *Patches {
		patch = simGetExistOk(patch)
		patch = ApplyFunc(parseExtendedStringArgumentOrReply, func(_ CommandParams, flags *int, _ int) (protocol.RedisMessage, time.Duration) {
			*flags = flag_ex
			return nil, 100 * time.Second
		})
		return patch
	}

	simPersist := func(patch *Patches) *Patches {
		patch = simGetExistOk(patch)
		patch = simParseExtendedStringParamWithSpecificFlags(patch, flag_persist)
		return patch
	}

	okResultOfCommand := protocol.MakeBulkString([]byte("test"))

	cases := []testcase{
		{"Parse extend params error", simParseExtendedStringParamError, &protocol.SyntaxError, nil},
		{"Get exist key but value type is not string", simGetWrongType, &protocol.WrongTypeError, nil},
		{"Get exist key and value type is string", simGetExistOk, protocol.MakeBulkString([]byte("test")), nil},
		{"Get non-exist kye", simGetNonExistKey, protocol.MakeNil(), nil},
		{"Get exist key and value type is string and ex 100", simEx100, okResultOfCommand, okOnExpire},
		{"Get exist key and value type is string and persist", simPersist, okResultOfCommand, okOnPersist},
	}

	Convey("TestGetExCommand", t, func() {
		db := MakeDatabase()
		args := [][]byte{[]byte("key")}
		patch := NewPatches()
		for _, c := range cases {
			Convey(c.name, func() {
				patch = c.patch(patch)
				defer patch.Reset()
				msg := getexCommand(db, args)
				So(msg, ShouldEqual, c.expect)
				if c.extra != nil {
					So(true, ShouldEqual, c.extra(db, "key"))
				}
			})
		}
	})
}

func TestGetRangeCommand(t *testing.T) {
	type testcase struct {
		name   string
		args   string
		patch  func(patch *Patches) *Patches
		expect protocol.RedisMessage
	}

	cases := []testcase{
		{"Start is not integer", "key a 1", nil, &protocol.InvalidIntegerError},
		{"End is not integer", "key 1 b", nil, &protocol.InvalidIntegerError},
		{"Start is greater than end", "key 2 1", nil, protocol.MakeBulkString(nil)},
		{"Get exist key and value type is string", "key 0 1", simGetExistOk, protocol.MakeBulkString([]byte("te"))},
		{"Get non-exist key", "key 0 1", simGetNonExistKey, protocol.MakeBulkString(nil)},
		{"Get exist key but value type is not string", "key 0 1", simGetWrongType, &protocol.WrongTypeError},
		{"Negative start and end", "key -2 -1", simGetExistOk, protocol.MakeBulkString([]byte("st"))},
		{"Start is greater than length of string", "key 10 100", simGetExistOk, protocol.MakeBulkString(nil)},
		{"End is greater than length of string", "key 0 100", simGetExistOk, protocol.MakeBulkString([]byte("test"))},
	}

	Convey("TestGetRangeCommand", t, func() {
		db := MakeDatabase()
		for _, c := range cases {
			Convey(c.name, func() {
				var patch *Patches
				if c.patch != nil {
					patch = c.patch(NewPatches())
					defer patch.Reset()
				}

				args := parseargs(c.args)
				msg := getrangeCommand(db, args)
				So(msg, ShouldEqual, c.expect)
			})
		}
	})

}

func BenchmarkIncrByFloat(b *testing.B) {
	db := MakeDatabase()
	db.Set("a", 0.1)
	b.ReportAllocs()
	for i := 0; i < 1024; i++ {
		f := bigfloats.Get()
		bigfloats.Put(f)
	}

	args := [][]byte{
		[]byte("a"),
		[]byte("0.3"),
	}
	b.RunParallel(func(p *testing.PB) {
		for p.Next() {
			incrbyfloatCommand(db, args)
		}
	})
}

func parseargs(args string) CommandParams {
	parts := strings.Split(args, " ")
	params := make([][]byte, 0, len(parts))
	for _, p := range parts {
		params = append(params, []byte(p))
	}
	return params
}
