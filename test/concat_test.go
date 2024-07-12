package test

import (
	"testing"
	"time"

	"golang.org/x/exp/rand"
)

func ConcatUsingPlus(b []byte) []byte {
	bs := []byte("-" + string(b) + "\r\n")
	a := bs
	_ = a
	return bs
}

func ConcatUsingCopyAndPrealloc(b []byte) []byte {
	bs := make([]byte, 1+len(b)+2)
	bs[0] = '-'
	copy(bs[1:], b)
	bs[len(b)+1] = '\r'
	bs[len(b)+2] = '\n'
	a := bs
	_ = a
	return bs
}
func GenerateRandomString(length int) string {
	// 定义字符集
	charset := "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

	// 使用当前时间作为种子初始化随机数生成器
	rand.Seed(uint64(time.Now().UnixNano()))

	// 创建一个长度为 length 的字节切片
	bytes := make([]byte, length)

	// 为每个字节生成一个随机索引,并将对应的字符写入字节切片
	for i := range bytes {
		bytes[i] = charset[rand.Intn(len(charset))]
	}

	return string(bytes)
}

func BenchmarkConcatUsingPlus(b *testing.B) {
	s := GenerateRandomString(100)
	for i := 0; i < b.N; i++ {
		ConcatUsingPlus([]byte(s))
	}
}

func BenchmarkConcatUsingCopyAndPrealloc(b *testing.B) {
	s := GenerateRandomString(100)
	for i := 0; i < b.N; i++ {
		ConcatUsingCopyAndPrealloc([]byte(s))
	}
}

func BenchmarkConcat(b *testing.B) {
	b.Run("ConcatUsingPlus", BenchmarkConcatUsingPlus)
	b.Run("ConcatUsingCopyAndPrealloc", BenchmarkConcatUsingCopyAndPrealloc)
}
