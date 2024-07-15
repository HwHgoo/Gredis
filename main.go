package main

import (
	"os"
	"os/signal"
	"syscall"

	"github.com/HwHgoo/Gredis/tcpserver"
)

func main() {
	s := tcpserver.MakeTcpServer()
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	s.ListenAndServe(signals)
}
