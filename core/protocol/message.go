package protocol

import "strconv"

type RedisMessage interface {
	Bytes() []byte  // encoded message according to Redis serialization protocol specification
	Args() [][]byte // decoded arguments for coresponding command (if any)
}

type SimpleString struct {
	data []byte
}

func (ss *SimpleString) Bytes() []byte {
	b := make([]byte, 1+len(ss.data)+2)
	b[0] = '+'
	copy(b[1:], ss.data)
	b[len(b)-2] = '\r'
	b[len(b)-1] = '\n'
	return b
}

func (ss *SimpleString) Args() [][]byte {
	args := make([][]byte, 1)
	args[0] = ss.data
	return args
}

type BulkString struct {
	data []byte
}

func (bs *BulkString) Bytes() []byte {
	l := len(bs.data)
	bulkLen := []byte(strconv.Itoa(l))
	b := make([]byte, 1+len(bulkLen)+2+l+2) // $ + " + {data} + " + \r\n
	b[0] = '$'
	copy(b[1:], bulkLen)
	offset := 1 + len(bulkLen)
	b[offset], b[offset+1] = '\r', '\n'
	offset += 2
	copy(b[offset:], bs.data)
	offset += len(bs.data)
	b[len(b)-2] = '\r'
	b[len(b)-1] = '\n'
	return b
}

func (bs *BulkString) Args() [][]byte {
	args := make([][]byte, 1)
	args[0] = bs.data
	return args
}

type SimpleError struct {
	data []byte
}

func (se *SimpleError) Bytes() []byte {
	b := make([]byte, 1+len(se.data)+2)
	b[0] = '-'
	copy(b[1:], se.data)
	b[len(b)-2] = '\r'
	b[len(b)-1] = '\n'
	return b
}

func (se *SimpleError) Args() [][]byte {
	return nil
}

type SimpleNil struct {
	data []byte
}

var SimpleNilInstance = SimpleNil{[]byte("_\r\n")}

func (sn *SimpleNil) Bytes() []byte { return sn.data }

func (sn *SimpleNil) Args() [][]byte { return nil }

type Array struct {
	elements []RedisMessage
}

func (a *Array) Bytes() []byte {
	length := strconv.Itoa(len(a.elements))
	data := []byte("*" + length + "\r\n")
	for i := range a.elements {
		if a == nil {
			continue
		}
		data = append(data, a.elements[i].Bytes()...)
	}

	return data
}

func (a *Array) Args() [][]byte {
	args := make([][]byte, 0)
	for i := range a.elements {
		args = append(args, a.elements[i].Args()...)
	}
	return args
}

type Integer struct {
	b []byte
}

func (i *Integer) Bytes() []byte {
	return i.b
}

func (a *Integer) Args() [][]byte {
	return nil
}
