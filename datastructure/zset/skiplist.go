package zset

import (
	"math/rand"
	"time"
)

const (
	maxLevel = 32            // max level of skiplist
	p        = float64(0.25) // probability of increasing level
	// which makes the average level of the list is 1/(1-0.25) = 1.33

	skiplist_max_search = 10
)

type SkipList interface {
	Insert(name string, score float64) *skiplistNode
	Delete(name string, score float64) int
	NthInRange(zrange *ZRangeSpec, n int) *skiplistNode
	GetRank(name string, score float64) int
	Length() int
}

type SkipListNode interface {
	Name() string
	Score() float64
}

type skiplistLevel struct {
	foward *skiplistNode
	span   int
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

func (node *skiplistNode) Name() string   { return node.name }
func (node *skiplistNode) Score() float64 { return node.score }

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
	rank := make([]int, maxLevel)
	for i := sl.level - 1; i >= 0; i-- {
		if i != sl.level-1 {
			rank[i] = rank[i+1]
		}
		for node.level[i].foward != nil && (node.level[i].foward.score < score ||
			(node.level[i].foward.score == score && node.level[i].foward.name < name)) {
			rank[i] += node.level[i].span
			node = node.level[i].foward
		}
		update[i] = node
	}

	level := randomLevel()
	if level > sl.level {
		for i := sl.level; i < level; i++ {
			update[i] = sl.head
			rank[i] = 0
			update[i].level[i].span = sl.length
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

		x.level[i].span = update[i].level[i].span - (rank[0] - rank[i])
		update[i].level[i].span = rank[0] - rank[i] + 1
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
		for x.level[i].foward != nil && (x.level[i].foward.score < score ||
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
			update[i].level[i].span += x.level[i].span - 1
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

func (sl *skiplist) NthInRange(zrange *ZRangeSpec, n int) *skiplistNode {
	// check if sl is in range
	if !sl.InRange(zrange) {
		return nil
	}
	x := sl.head
	edge_rank := 0
	i := sl.level - 1
	for x.level[i].foward != nil && !zrange.ValueGteMin(x.level[i].foward.score) {
		edge_rank += x.level[i].span
		x = x.level[i].foward
	}
	last_highest_level_node := x
	last_highest_level_node_rank := edge_rank

	if n >= 0 {
		for i = sl.level - 2; i >= 0; i-- {
			for x.level[i].foward != nil && !zrange.ValueGteMin(x.level[i].foward.score) {
				edge_rank += x.level[i].span
				x = x.level[i].foward
			}
		}

		// check if n is out of the list
		if edge_rank+n >= sl.length {
			return nil
		}

		if n < skiplist_max_search {
			// if offset is small, we just jump node by node
			for i = 0; i < n+1; i++ {
				x = x.level[0].foward
			}
		} else {
			rank_diff := edge_rank - last_highest_level_node_rank + n + 1
			x = sl.GetElementByRankFromNode(last_highest_level_node, sl.level-1, rank_diff)
		}
		if x != nil && !zrange.ValueLteMax(x.score) {
			return nil
		}
	} else {
		for i = sl.level - 1; i >= 0; i-- {
			for x.level[i].foward != nil && zrange.ValueLteMax(x.level[i].foward.score) {
				edge_rank += x.level[i].span // the rank of the last node in the range
				x = x.level[i].foward        // last node in the range
			}
		}

		if edge_rank < -n {
			return nil
		}

		// when n is negative, n is -1 based
		if -n-1 < skiplist_max_search {
			for i = 0; i < -n-1; i++ {
				x = x.backward
			}
		} else {
			rank_diff := edge_rank - last_highest_level_node_rank + n + 1
			x = sl.GetElementByRankFromNode(last_highest_level_node, sl.level-1, rank_diff)
		}

		if x != nil && !zrange.ValueGteMin(x.score) {
			return nil
		}
	}

	return x
}

func (sl *skiplist) GetRank(name string, score float64) int {
	rank := 0
	x := sl.head
	for i := sl.level - 1; i >= 0; i-- {
		for x.level[i].foward != nil && (x.level[i].foward.score < score ||
			(x.level[i].foward.score == score && x.level[i].foward.name < name)) {
			rank += x.level[i].span
			x = x.level[i].foward
		}
	}

	x = x.level[0].foward
	if x == nil || x.name != name || x.score != score {
		return 0
	}

	return rank
}

// Get an element by rank from the given node. The rank needs to be 1-based.
func (sl *skiplist) GetElementByRankFromNode(node *skiplistNode, start_level int, rank int) *skiplistNode {
	x := node
	traversed := 0
	for i := start_level; i >= 0; i-- {
		for x.level[i].foward != nil && traversed+int(x.level[i].span) <= rank {
			traversed += int(x.level[i].span)
			x = x.level[i].foward
		}

		if traversed == rank {
			return x
		}
	}

	return nil
}

func (sl *skiplist) InRange(zrange *ZRangeSpec) bool {
	if zrange.Min > zrange.Max || (zrange.Min == zrange.Max && (zrange.MinEx || zrange.MaxEx)) {
		return false
	}

	x := sl.head.level[0].foward
	if x == nil || !zrange.ValueLteMax(x.score) {
		return false
	}

	x = sl.tail
	if x == nil || !zrange.ValueGteMin(x.score) {
		return false
	}

	return true
}

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
