package protocol

type RedisMessage interface {
	Bytes() []byte
	Args() [][]byte
}

type SimpleString struct {
	bytes []byte
}

func (s *SimpleString) Bytes() []byte {
	b := make([]byte, len(s.bytes)+3)
	b[0] = '+'
	copy(b[1:], s.bytes)
	b[len(b)-2] = '\r'
	b[len(b)-1] = '\n'
	return b
}

func (s *SimpleString) Args() [][]byte {
	args := make([][]byte, 1)
	args[0] = s.bytes
	return args
}
