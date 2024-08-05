package zset

import (
	"math/rand"
	"time"
)

const (
	maxLevel = 32            // max level of skiplist
	p        = float64(0.25) // probability of increasing level
	// which makSes the average level of the list is 1/(1-0.25) = 1.33
)

type SkipList interface {
	Insert(name string, score float64) *skiplistNode
	Delete(name string, score float64) int
	Length() int
}

type skiplistLevel struct {
	foward *skiplistNode
	span   uint32
}

type skiplistNode struct {
	name     string
	score    float64
	backward *skiplistNode // previous node, only exist in level 0
	level    []*skiplistLevel
}

type skiplist struct {
	head   *skiplistNode
	tail   *skiplistNode
	level  int
	length int
}

func NewSkipList() SkipList {
	head := &skiplistNode{
		level: make([]*skiplistLevel, maxLevel),
	}
	for i := 0; i < maxLevel; i++ {
		head.level[i] = &skiplistLevel{foward: nil, span: 0}
	}
	return &skiplist{
		head:   head,
		level:  1,
		length: 0,
	}
}

func (sl *skiplist) Length() int { return sl.length }

func (sl *skiplist) Insert(name string, score float64) *skiplistNode {
	node := sl.head
	update := make([]*skiplistNode, maxLevel)
	for i := sl.level - 1; i >= 0; i-- {
		for {
			if node.level[i].foward == nil || node.level[i].foward.score > score {
				break
			}

			node = node.level[i].foward
		}
		update[i] = node
	}

	level := randomLevel()
	if level > sl.level {
		for i := sl.level; i < level; i++ {
			update[i] = sl.head
		}
		sl.level = level
	}

	x := &skiplistNode{
		level: make([]*skiplistLevel, level),
		score: score,
		name:  name,
	}

	for i := 0; i < level; i++ {
		x.level[i] = &skiplistLevel{foward: nil, span: 0}
		x.level[i].foward = update[i].level[i].foward
		update[i].level[i].foward = x
	}
	if x.level[0].foward != nil {
		x.level[0].foward.backward = x
	} else {
		sl.tail = x
	}

	if update[0] != sl.head {
		x.backward = update[0]
	}

	sl.length++
	return x
}

// Return 0 if not found, 1 if found and deleted.
func (sl *skiplist) Delete(name string, score float64) int {
	x := sl.head
	update := make([]*skiplistNode, maxLevel)
	for i := sl.level - 1; i >= 0; i-- {
		for x.level[i] != nil && (x.level[i].foward.score < score ||
			(x.level[i].foward.score == score && x.level[i].foward.name < name)) {
			x = x.level[i].foward
		}

		update[i] = x
	}

	x = x.level[0].foward
	if x == nil || x.score != score || x.name != name {
		return 0
	}

	sl.deleteNode(x, update)
	return 1
}

func (sl *skiplist) deleteNode(x *skiplistNode, update []*skiplistNode) {
	for i := 0; i < sl.level; i++ {
		if update[i].level[i].foward == x {
			update[i].level[i].foward = x.level[i].foward
		}
	}

	if x.level[0].foward != nil {
		x.level[0].foward.backward = x.backward
	} else {
		sl.tail = x.backward
	}

	for sl.level > 1 && sl.head.level[sl.level-1].foward == nil {
		sl.level--
	}

	sl.length--
}

func (sl *skiplist) Contains(name string) bool { return false }

func (sl *skiplist) GetScore(name string) (float64, bool) { return 0, false }

func (sl *skiplist) GetRange(start, end float64) []string { return nil }

func randomLevel() int {
	level := 1
	generator := rand.New(rand.NewSource(time.Now().UnixNano()))
	for {
		random := generator.Float64()
		if random > p || level >= maxLevel {
			break
		}
		level++
	}

	return level
}
