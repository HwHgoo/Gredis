package connection

import "net"

type Connection struct {
	conn net.Conn
}

func MakeConnection(conn net.Conn) *Connection {
	return nil
}
