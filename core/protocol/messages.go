package protocol

var (
	RedisOk  = SimpleString{[]byte("OK")}
	RedisNil = SimpleNil{[]byte("_\r\n")}
)
