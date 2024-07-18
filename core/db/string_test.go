package db

import (
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/HwHgoo/Gredis/core/protocol"
	. "github.com/smartystreets/goconvey/convey"
)

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
			{"CommandSet: Use ex only and non-integer second arg", "ex 1.1", command_set, expect{flag_ex, &protocol.InvalidNumberError, 0}},
			{"CommandSet: Use ex only and non-numberic second arg", "ex a123", command_set, expect{flag_ex, &protocol.InvalidNumberError, 0}},
			{"CommandSet: Use ex only but no second arg", "ex", command_set, expect{flag_ex, &protocol.SyntaxError, 0}},
			{"CommandSet: Use px only and correct second arg", "px 1000", command_set, expect{flag_px, nil, time.Millisecond * 1000}},
			{"CommandSet: Use px only and nagetive second arg", "px -1", command_set, expect{flag_px, &protocol.InvalidExpireTimeError, 0}},
			{"CommandSet: Use px only and non-integer second arg", "px 1.1", command_set, expect{flag_px, &protocol.InvalidNumberError, 0}},
			{"CommandSet: Use px only and non-numberic second arg", "px a123", command_set, expect{flag_px, &protocol.InvalidNumberError, 0}},
			{"CommandSet: Use px only but no second arg", "px", command_set, expect{flag_px, &protocol.SyntaxError, 0}},
			{"CommandSet: Use exat only and correct second arg", "exat " + secstr, command_set, expect{flag_exat, nil, secduration}},
			{"CommandSet: Use exat only and negative second arg", "exat -1", command_set, expect{flag_px, &protocol.InvalidExpireTimeError, 0}},
			{"CommandSet: Use exat only and non-integer second arg", "exat 1.1", command_set, expect{flag_px, &protocol.InvalidNumberError, 0}},
			{"CommandSet: Use exat only and non-numberic second arg", "exat -a1", command_set, expect{flag_px, &protocol.InvalidNumberError, 0}},
			{"CommandSet: Use exat only but no second arg", "exat", command_set, expect{flag_exat, &protocol.SyntaxError, 0}},
			{"CommandSet: Use pxat only and correct second arg", "pxat " + msecstr, command_set, expect{flag_pxat, nil, msduration}},
			{"CommandSet: Use pxat only and negative second arg", "pxat -1", command_set, expect{flag_pxat, &protocol.InvalidExpireTimeError, 0}},
			{"CommandSet: Use pxat only and non-integer second arg", "pxat 1.1", command_set, expect{flag_pxat, &protocol.InvalidNumberError, 0}},
			{"CommandSet: Use pxat only and non-numberic second arg", "pxat -a1", command_set, expect{flag_pxat, &protocol.InvalidNumberError, 0}},
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
			{"CommandGet: Use ex only and non-integer second arg", "ex 1.1", command_get, expect{flag_ex, &protocol.InvalidNumberError, 0}},
			{"CommandGet: Use ex only and non-numberic second arg", "ex a123", command_get, expect{flag_ex, &protocol.InvalidNumberError, 0}},
			{"CommandGet: Use ex only but no second arg", "ex", command_get, expect{flag_ex, &protocol.SyntaxError, 0}},
			{"CommandGet: Use px only and correct second arg", "px 1000", command_get, expect{flag_px, nil, time.Millisecond * 1000}},
			{"CommandGet: Use px only and nagetive second arg", "px -1", command_get, expect{flag_px, &protocol.InvalidExpireTimeError, 0}},
			{"CommandGet: Use px only and non-integer second arg", "px 1.1", command_get, expect{flag_px, &protocol.InvalidNumberError, 0}},
			{"CommandGet: Use px only and non-numberic second arg", "px a123", command_get, expect{flag_px, &protocol.InvalidNumberError, 0}},
			{"CommandGet: Use px only but no second arg", "px", command_get, expect{flag_px, &protocol.SyntaxError, 0}},
			{"CommandGet: Use exat only and correct second arg", "exat " + secstr, command_get, expect{flag_exat, nil, secduration}},
			{"CommandGet: Use exat only and negative second arg", "exat -1", command_get, expect{flag_px, &protocol.InvalidExpireTimeError, 0}},
			{"CommandGet: Use exat only and non-integer second arg", "exat 1.1", command_get, expect{flag_px, &protocol.InvalidNumberError, 0}},
			{"CommandGet: Use exat only and non-numberic second arg", "exat -a1", command_get, expect{flag_px, &protocol.InvalidNumberError, 0}},
			{"CommandGet: Use exat only but no second arg", "exat", command_get, expect{flag_exat, &protocol.SyntaxError, 0}},
			{"CommandGet: Use pxat only and correct second arg", "pxat " + msecstr, command_get, expect{flag_pxat, nil, msduration}},
			{"CommandGet: Use pxat only and negative second arg", "pxat -1", command_get, expect{flag_pxat, &protocol.InvalidExpireTimeError, 0}},
			{"CommandGet: Use pxat only and non-integer second arg", "pxat 1.1", command_get, expect{flag_pxat, &protocol.InvalidNumberError, 0}},
			{"CommandGet: Use pxat only and non-numberic second arg", "pxat -a1", command_get, expect{flag_pxat, &protocol.InvalidNumberError, 0}},
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
