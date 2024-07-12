package connection

import "net"

type Connection struct {
	conn       net.Conn
	selectedDb int
}

func MakeConnection(conn net.Conn) *Connection {
	return &Connection{conn: conn}
}

func (c *Connection) Write(data []byte) error {
	_, err := c.conn.Write(data)
	return err
}

func (c *Connection) GetSelectedDb() int {
	return c.selectedDb
}
