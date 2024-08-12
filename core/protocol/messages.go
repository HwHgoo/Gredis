package protocol

var (
	RedisOk             = SimpleString{[]byte("OK")}
	RedisNil            = SimpleNil{[]byte("_\r\n")}
	RedisBgSave         = SimpleString{[]byte("Background saving started")}
	RedisNotImplemented = SimpleString{[]byte("Command not implemented yet")}
)
