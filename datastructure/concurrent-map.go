package datastructure

import (
	"sync"

	"github.com/HwHgoo/Gredis/utils"
)

const shard_count = 32

type ConcurrentMap[T any] []concurrentMapShard[T]

type concurrentMapShard[T any] struct {
	m    map[string]T
	lock sync.RWMutex
}

func MakeNewConcurrentMap[T any]() *ConcurrentMap[T] {
	cm := make(ConcurrentMap[T], shard_count)
	for i := range cm {
		cm[i] = concurrentMapShard[T]{m: make(map[string]T)}
	}
	return &cm
}

func (cm ConcurrentMap[T]) Get(key string) (value T, ok bool) {
	s := cm.shard(key)
	s.lock.RLock()
	defer s.lock.RUnlock()
	value, ok = s.m[key]
	return
}

func (cm *ConcurrentMap[T]) Delete(key string) {
	s := cm.shard(key)
	s.lock.Lock()
	defer s.lock.Unlock()
	delete(s.m, key)
}

func (cm *ConcurrentMap[T]) Set(key string, value T) {
	s := cm.shard(key)
	s.lock.Lock()
	defer s.lock.Unlock()
	s.m[key] = value
}

func (cm ConcurrentMap[T]) shard(key string) *concurrentMapShard[T] {
	hash := utils.Fnv32([]byte(key))
	s := hash % uint32(shard_count)
	return &cm[s]
}
