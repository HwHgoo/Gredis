package protocol

type RedisErrorMessage interface {
	RedisMessage
	Error() string
}

type redisErrorMessage struct {
	msg []byte
}

var (
	WrongTypeError = redisErrorMessage{[]byte("-WRONGTYPE Operation against a key holding the wrong kind of value\r\n")}
)

func (e redisErrorMessage) Bytes() []byte { return e.msg }

func (e redisErrorMessage) Args() [][]byte { return nil }

func (e redisErrorMessage) Error() string { return string(e.msg)[1 : len(e.msg)-2] }

func MakeUnknownCommandError(cmdName string, startwith string) redisErrorMessage {
	return redisErrorMessage{[]byte("-ERR unknown command `" + cmdName + "`, with args beginning with: `" + startwith + "`\r\n")}
}
