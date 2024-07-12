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

func Parse(stream io.Reader) <-chan *Payload {
	payloads := make(chan *Payload, chan_size)
	go parse(stream, payloads)
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
		case '+':
		case '*':
			parseArray(buf, r, payloads)
		case '$':
			parseBulk(buf, r, payloads)
		}
	}
}

func parseArray(buf []byte, r *bufio.Reader, ch chan<- *Payload) error {
	l, err := strconv.ParseInt(string(buf[1:]), 10, 32)
	if err != nil {
		porotocolError(ch, "invalid array header: "+string(buf[1:]))
		return nil
	}
	args := make([][]byte, 0, 1)
	for i := 0; i < int(l); i++ {
		buf, err := r.ReadBytes('\n')
		if err != nil {
			return nil
		}

		if len(buf) <= 2 || buf[len(buf)-2] != '\r' {
			continue
		}

		buf = bytes.TrimSuffix(buf, []byte{'\r', '\n'})
		if buf[0] == '$' {
			l, err := strconv.ParseInt(string(buf[1:]), 10, 32)
			if err != nil {
				porotocolError(ch, "invalid bulk header: "+string(buf[1:]))
				return nil
			}
			length := int(l)
			bulk, err := readBulk(length, r)
			if err != nil {
				return err
			}

			args = append(args, bulk)

			_, err = r.ReadBytes('\n')
			if err != nil {
				return nil
			}
		}
	}

	ch <- &Payload{msg: protocol.MakeArray(args)}

	return nil
}

func parseBulk(buf []byte, r *bufio.Reader, ch chan<- *Payload) error {
	l, err := strconv.ParseInt(string(buf[1:]), 10, 32)
	if err != nil {
		porotocolError(ch, "invalid bulk header: "+string(buf[1:]))
		return nil
	}
	length := int(l)

	bulk, err := readBulk(length, r)
	if err != nil {
		return err
	}
	_, err = r.ReadBytes('\n')
	if err != nil {
		return err
	}

	ch <- &Payload{msg: protocol.MakeBulkString(bulk)}
	return nil
}

func readBulk(length int, r *bufio.Reader) ([]byte, error) {
	value := make([]byte, length)
	_, err := io.ReadFull(r, value)
	return value, err
}

func porotocolError(ch chan<- *Payload, msg string) {
	err := errors.New("protocol error: " + msg)
	ch <- &Payload{err: err}
}
