package test

import (
	"bytes"
	"log"
	"testing"
	"time"
)

func TestIoCopy(t *testing.T) {
	a := make([]byte, 100)
	log.Println(len(a))
	a = a[:10]
	log.Println(len(a))
	a = a[:]
	log.Println(len(a))
}

func TestBytesBuffer(t *testing.T) {
	bb := &bytes.Buffer{}
	bb.Write([]byte("hello"))
	bb.Reset()
	bb.Write([]byte("world"))
	log.Println(string(bb.Bytes()))
}

func TestTimeDurationMultiply(t *testing.T) {
	s := time.Second * time.Duration(1)
	log.Println(s)
}

func TestFloatAdd(t *testing.T) {
}
