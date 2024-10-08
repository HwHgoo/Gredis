package protocol

type RedisErrorMessage interface {
	RedisMessage
	Error() string
}

type redisErrorMessage struct {
	msg []byte
}

var (
	WrongTypeError         = redisErrorMessage{[]byte("-WRONGTYPE Operation against a key holding the wrong kind of value\r\n")}
	SyntaxError            = redisErrorMessage{[]byte("-ERR syntax error\r\n")}
	InvalidIntegerError    = redisErrorMessage{[]byte("-ERR value is not an integer or out of range\r\n")}
	InvalidFloatError      = redisErrorMessage{[]byte("-ERR value is not an float\r\n")}
	InvalidExpireTimeError = redisErrorMessage{[]byte("-ERR invalid expire time in EXPIRE command\r\n")}
	OffsetOutofRangeError  = redisErrorMessage{[]byte("-ERR offset out of range\r\n")}
	NanError               = redisErrorMessage{[]byte("-ERR result score is not a number (NaN)\r\n")}
	MinOrMaxNotFloatError  = redisErrorMessage{[]byte("-ERR min or max is not a float\r\n")}
	DbIndexOutOfRange      = redisErrorMessage{[]byte("-ERR DB index is out of range\r\n")}

	ZSetNXAndXXError        = redisErrorMessage{[]byte("-ERR XX and NX options at the same time are not compatible\r\n")}
	ZSetGTLTAndNXError      = redisErrorMessage{[]byte("-ERR GT, LT, and/or NX options at the same time are not compatible\r\n")}
	ZSetIncrMultiPairsError = redisErrorMessage{[]byte("-ERR INCR option supports a single score-member pair only\r\n")}
)

func (e redisErrorMessage) Bytes() []byte { return e.msg }

func (e redisErrorMessage) Args() [][]byte { return nil }

func (e redisErrorMessage) Error() string { return string(e.msg)[1 : len(e.msg)-2] }

func MakeUnknownCommandError(cmdName string, startwith string) RedisErrorMessage {
	return &redisErrorMessage{[]byte("-ERR unknown command `" + cmdName + "`, with args beginning with: `" + startwith + "`\r\n")}
}

func MakeWrongNumberOfArgError(cmdname string) RedisErrorMessage {
	return &redisErrorMessage{[]byte("-ERR wrong number of arguments for '" + cmdname + "' command\r\n")}
}
