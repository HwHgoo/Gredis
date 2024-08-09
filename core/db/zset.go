package db

import (
	"math"
	"strconv"
	"strings"

	"github.com/HwHgoo/Gredis/core/protocol"
	"github.com/HwHgoo/Gredis/datastructure/zset"
	"github.com/HwHgoo/Gredis/utils"
)

const zadd_in_none = 0
const (
	zadd_in_incr = 1 << iota
	zadd_in_nx
	zadd_in_xx
	zadd_in_gt
	zadd_in_lt
)
const (
	zadd_out_nop = 1 << iota
	zadd_out_nan
	zadd_out_added
	zadd_out_updated
)

func (db *Database) getAsZset(key string) (zset.ZSet, protocol.RedisErrorMessage) {
	val, ok := db.Get(key)
	if !ok {
		return nil, nil
	}

	zs, ok := val.(zset.ZSet)
	if !ok {
		return nil, protocol.WrongTypeError
	}

	return zs, nil
}

func zsetAdd(set zset.ZSet, score float64, member string, in_flags int, out_flags *int, newscore *float64) int {
	*out_flags = 0
	incr := in_flags&zadd_in_incr != 0
	nx := in_flags&zadd_in_nx != 0
	xx := in_flags&zadd_in_xx != 0
	gt := in_flags&zadd_in_gt != 0
	lt := in_flags&zadd_in_lt != 0
	if math.IsNaN(score) {
		*out_flags |= zadd_out_nan
		return 0
	}

	curscore, exists := set.Score(member)
	if exists {
		if nx {
			*out_flags |= zadd_out_nop
			return 1
		}

		if incr {
			score += curscore
			if math.IsNaN(score) {
				*out_flags |= zadd_out_nan
				return 0
			}

			if newscore != nil {
				*newscore = score
			}
		}

		if gt && score <= curscore || lt && score >= curscore {
			*out_flags |= zadd_out_nop
			return 1
		}

		set.Update(member, score)
		*out_flags |= zadd_out_updated
		return 1
	} else if !xx {
		set.Insert(member, score)
		*out_flags |= zadd_out_added
		if incr && newscore != nil {
			*newscore = score
		}
		return 1
	} else {
		*out_flags |= zadd_out_nop
		return 1
	}
}

func (db *Database) zaddGenericCommand(args CommandParams, flags int) protocol.RedisMessage {
	ch := false
	key := string(args[0])
	score_idx := 1
	added, updated, processed := 0, 0, 0
	for score_idx < len(args) {
		arg := strings.ToLower(string(args[score_idx]))
		if arg == "nx" {
			flags |= zadd_in_nx
		} else if arg == "xx" {
			flags |= zadd_in_xx
		} else if arg == "ch" {
			ch = true
		} else if arg == "incr" {
			flags |= zadd_in_incr
		} else if arg == "gt" {
			flags |= zadd_in_gt
		} else if arg == "lt" {
			flags |= zadd_in_lt
		} else {
			break
		}

		score_idx++
	}

	incr, nx := flags&zadd_in_incr != 0, flags&zadd_in_nx != 0
	xx, gt, lt := flags&zadd_in_xx != 0, flags&zadd_in_gt != 0, flags&zadd_in_lt != 0
	pairs := 0 // score-member pairs
	if pairs = (len(args) - score_idx); pairs == 0 || pairs%2 != 0 {
		return protocol.SyntaxError
	}
	pairs /= 2

	if nx && xx {
		return protocol.ZSetNXAndXXError
	}

	// nx is not comptible with either gt or lt
	// and gt and lt are not comptible with each other
	if (gt && nx) || (lt && nx) || (gt && lt) {
		return protocol.ZSetGTLTAndNXError
	}

	if incr && pairs > 1 {
		return protocol.ZSetIncrMultiPairsError
	}

	scores := make([]float64, 0, pairs)
	for i := 0; i < pairs; i++ {
		score, err := strconv.ParseFloat(string(args[score_idx+i*2]), 64)
		if err != nil {
			return protocol.InvalidFloatError
		}
		scores = append(scores, score)
	}

	set, rerr := db.getAsZset(key)
	if rerr != nil {
		return rerr
	}
	if set == nil {
		set = zset.NewZSet()
		db.Set(key, set)
	}

	out_flags := 0
	newscore := float64(0)
	for i := 0; i < pairs; i++ {
		ret := zsetAdd(set, scores[i], string(args[score_idx+i*2+1]), flags, &out_flags, &newscore)
		if ret == 0 {
			return protocol.NanError
		}

		if out_flags&zadd_out_added != 0 {
			added++
		}
		if out_flags&zadd_out_updated != 0 {
			updated++
		}
		if out_flags&zadd_out_nop == 0 {
			processed++
		}
	}

	if incr {
		if processed > 0 {
			return protocol.MakeBulkString(utils.FloatBytes(newscore))
		} else {
			return &protocol.RedisNil
		}
	}

	return protocol.MakeInteger(int64(utils.TerneryOp(ch, added+updated, added)))
}

func zaddCommand(db *Database, args CommandParams) protocol.RedisMessage {
	return db.zaddGenericCommand(args, zadd_in_none)
}

func zcardCommand(db *Database, args CommandParams) protocol.RedisMessage {
	key := string(args[0])
	set, rerr := db.getAsZset(key)
	if rerr != nil {
		return rerr
	}

	if set == nil {
		return protocol.MakeInteger(0)
	}

	return protocol.MakeInteger(int64(set.Card()))
}

func zcountCommand(db *Database, args CommandParams) protocol.RedisMessage {
	key := string(args[0])
	set, rerr := db.getAsZset(key)
	if rerr != nil {
		return rerr
	}

	if set == nil {
		return protocol.MakeInteger(0)
	}

	rg, rerr := makeZRange(string(args[1]), string(args[2]))
	if rerr != nil {
		return rerr
	}

	x := set.NthInRange(rg, 0)
	if x == nil {
		return protocol.MakeInteger(0)
	}
	rank := set.Rank(x.Name(), x.Score())
	count := set.Card() - rank
	x = set.NthInRange(rg, -1)
	if x == nil {
		return protocol.MakeInteger(0)
	}
	rank = set.Rank(x.Name(), x.Score())
	count -= (set.Card() - rank - 1)
	return protocol.MakeInteger(int64(count))
}

func zscoreCommand(db *Database, args CommandParams) protocol.RedisMessage {
	key := string(args[0])
	set, rerr := db.getAsZset(key)
	if rerr != nil {
		return rerr
	}

	if set == nil {
		return &protocol.RedisNil
	}

	score, exists := set.Score(string(args[1]))
	if !exists {
		return &protocol.RedisNil
	}

	return protocol.MakeBulkString(utils.FloatBytes(score))
}

func makeZRange(min, max string) (*zset.ZRangeSpec, protocol.RedisErrorMessage) {
	rg := &zset.ZRangeSpec{}
	if min[0] == '(' {
		rg.MinEx = true
		min = min[1:]
	}
	var err error
	rg.Min, err = strconv.ParseFloat(min, 64)
	if err != nil {
		return nil, protocol.MinOrMaxNotFloatError
	}

	if max[0] == '(' {
		rg.MaxEx = true
		max = max[1:]
	}

	rg.Max, err = strconv.ParseFloat(max, 64)
	if err != nil {
		return nil, protocol.MinOrMaxNotFloatError
	}
	return rg, nil
}

func registerZSetCommands() {
	// zset commands
	RegisterCommand("zadd", -4, zaddCommand)
	RegisterCommand("zcard", 2, zcardCommand)
	RegisterCommand("zcount", 4, zcountCommand)
	RegisterCommand("zscore", 3, zscoreCommand)
}
