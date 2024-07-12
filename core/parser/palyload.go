package parser

import "github.com/HwHgoo/Gredis/core/protocol"

type Payload struct {
	msg protocol.RedisMessage
	err error
}

func (p Payload) Msg() protocol.RedisMessage {
	return p.msg
}

func (p Payload) Err() error {
	return p.err
}
