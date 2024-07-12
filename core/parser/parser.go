package parser

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"strconv"

	"github.com/HwHgoo/Gredis/core/protocol"
)

const chan_size = 64

type Payload struct {
	msg protocol.RedisMessage
	err error
}

func Parse(stream io.Reader) <-chan *Payload {
	payloads := make(chan *Payload, chan_size)
	return payloads
}

func parse(stream io.Reader, payloads chan<- *Payload) {
	r := bufio.NewReader(stream)
	for {
		buf, err := r.ReadBytes('\n')
		if err != nil {
			close(payloads)
			break
		}

		if len(buf) <= 2 || buf[len(buf)-2] != '\r' {
			continue
		}

		buf = bytes.TrimSuffix(buf, []byte{'\r', '\n'})
		switch buf[0] {
		}
	}
}

func parseArray(buf []byte, r *bufio.Reader, ch chan<- *Payload) error {
	l, err := strconv.ParseInt(string(buf), 10, 32)
	if err != nil {
		return nil
	}
	for i := 0; i < int(l); i++ {
		buf, err := r.ReadBytes('\n')
		if err != nil {
			return nil
		}

		if len(buf) <= 2 || buf[len(buf)-2] != '\r' {
			continue
		}
	}

	return nil
}

func porotocolError(ch chan<- *Payload, msg string) {
	err := errors.New("protocol error: " + msg)
	ch <- &Payload{err: err}
}
