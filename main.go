package main

import "github.com/HwHgoo/Gredis/tcpserver"

func main() {
	s := tcpserver.MakeTcpServer()
	s.ListenAndServe()
}
