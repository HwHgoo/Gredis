package protocol

import "strconv"

func MakeSimpleString(b []byte) RedisMessage {
	return &SimpleString{
		data: b,
	}
}

func MakeBulkString(b []byte) RedisMessage {
	return &BulkString{
		data: b,
	}
}

func MakeInteger(i int64) RedisMessage {
	ai := strconv.FormatInt(i, 10)
	return &Integer{b: []byte(":" + ai + "\r\n")}
}

func MakeArray(elements []RedisMessage) RedisMessage {
	return &Array{
		elements: elements,
	}
}

func MakeError(err error) RedisMessage {
	return &SimpleError{
		data: []byte(err.Error()),
	}
}

func MakeNil() RedisMessage {
	return &SimpleNilInstance
}
