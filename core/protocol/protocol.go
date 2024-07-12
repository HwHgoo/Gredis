package protocol

// Make message according to 'Redis serialization protocol specification'

func MakeSimpleString([]byte) RedisMessage {
	return &SimpleString{}
}
