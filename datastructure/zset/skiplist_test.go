package zset

import (
	"math/rand"
	"testing"

	"github.com/HwHgoo/Gredis/utils"
	. "github.com/agiledragon/gomonkey/v2"
	. "github.com/smartystreets/goconvey/convey"
)

type patch_fn func() *Patches

func TestSkipListInsert(t *testing.T) {
	type testcase struct {
		name     string
		pf       patch_fn // patch before callback
		pf_after patch_fn // patch after callback
		sl       SkipList
		callback func(tc *testcase) // callback function to prepare testcase data
		verify   func(tc *testcase) // verify function to check the result
		node     *skiplistNode
	}

	cases := []testcase{
		{
			"insert one element into empty list",
			nil,
			nil,
			NewSkipList(),
			nil,
			func(tc *testcase) {
				So(tc.sl.Length(), ShouldEqual, 1)
				sl_head := SkipListHead(tc.sl)
				sl_tail := SkipListTail(tc.sl)
				node := SkipListFind(tc.sl, "a", 1)
				So(sl_head.level[0].foward, ShouldEqual, node)
				So(sl_tail, ShouldEqual, node)
			},
			&skiplistNode{name: "a", score: 1},
		},
		{
			"insert one element into empty list with level 2",
			func() *Patches {
				return ApplyFuncReturn(randomLevel, 2)
			},
			nil,
			NewSkipList(),
			nil,
			func(tc *testcase) {
				So(tc.sl.Length(), ShouldEqual, 1)
				sl_head := SkipListHead(tc.sl)
				sl_tail := SkipListTail(tc.sl)
				sl_level := SkipListLevel(tc.sl)
				node := SkipListFind(tc.sl, "a", 1)
				So(sl_head.level[0].foward, ShouldEqual, node)
				So(sl_tail, ShouldEqual, node)
				So(sl_level, ShouldEqual, 2)
			},
			&skiplistNode{name: "a", score: 1},
		},
		{
			"insert one element into the head of the list",
			nil,
			nil,
			NewSkipList(),
			func(tc *testcase) {
				sl := tc.sl
				sl.Insert("b", 2)
				sl.Insert("c", 3)
			},
			func(tc *testcase) {
				So(tc.sl.Length(), ShouldEqual, 3)
				head := SkipListHead(tc.sl)
				node := SkipListFind(tc.sl, "a", 1)
				next := SkipListFind(tc.sl, "b", 2)
				So(head.level[0].foward, ShouldEqual, node)
				So(node.level[0].foward, ShouldEqual, next)
				So(next.backward, ShouldEqual, node)
			},
			&skiplistNode{name: "a", score: 1},
		},
		{
			"insert one element into the tail of the list",
			nil,
			nil,
			NewSkipList(),
			func(tc *testcase) {
				sl := tc.sl
				sl.Insert("a", 1)
				sl.Insert("b", 2)
			},
			func(tc *testcase) {
				So(tc.sl.Length(), ShouldEqual, 3)
				prev := SkipListFind(tc.sl, "b", 2)
				node := SkipListFind(tc.sl, "c", 3)
				tail := SkipListTail(tc.sl)
				So(prev.level[0].foward, ShouldEqual, node)
				So(node.backward, ShouldEqual, prev)
				So(tail, ShouldEqual, node)
			},
			&skiplistNode{name: "c", score: 3},
		},
		{
			"insert one element into the middle of the list",
			nil,
			nil,
			NewSkipList(),
			func(tc *testcase) {
				sl := tc.sl
				sl.Insert("a", 1)
				sl.Insert("b", 2)
				sl.Insert("d", 4)
				sl.Insert("e", 5)
			},
			func(tc *testcase) {
				So(tc.sl.Length(), ShouldEqual, 5)
				prev := SkipListFind(tc.sl, "b", 2)
				next := SkipListFind(tc.sl, "d", 4)
				node := SkipListFind(tc.sl, "c", 3)
				So(prev.level[0].foward, ShouldEqual, node)
				So(node.level[0].foward, ShouldEqual, next)
				So(node.backward, ShouldEqual, prev)
				So(next.backward, ShouldEqual, node)
			},
			&skiplistNode{name: "c", score: 3},
		},
		{
			"insert an element with the score alreay exists in the list",
			nil,
			nil,
			NewSkipList(),
			func(tc *testcase) {
				sl := tc.sl
				sl.Insert("a", 1)
				sl.Insert("b1", 2)
				sl.Insert("c", 3)
			},
			func(tc *testcase) {
				So(tc.sl.Length(), ShouldEqual, 4)
				b := SkipListFind(tc.sl, "b", 2)
				b1 := SkipListFind(tc.sl, "b1", 2)
				So(b.level[0].foward, ShouldEqual, b1)
				So(b1.backward, ShouldEqual, b)
			},
			&skiplistNode{name: "b", score: 2},
		},
		{
			"check span when inserting in the middle of the list",
			nil,
			func() *Patches { return ApplyFuncReturn(randomLevel, 4) },
			NewSkipList(),
			func(tc *testcase) {
				sl := tc.sl
				for i := 0; i < 1000; i++ {
					sl.Insert(utils.RadomString(i%20), float64(i))
				}
			},
			func(tc *testcase) {
				x := SkipListFind(tc.sl, tc.node.name, tc.node.score)
				actual := make([]int, len(x.level))
				for i := range x.level {
					actual[i] = x.level[i].span
				}
				_, xspan := SkipListNodeSpan(tc.sl, tc.node.name, tc.node.score)
				So(actual, ShouldResemble, xspan)
			},
			&skiplistNode{name: utils.RadomString(10), score: float64(rand.Intn(998) + 1)},
		},
	}

	Convey("TestSkipListInsert", t, func() {
		for _, c := range cases {
			Convey(c.name, func() {
				if c.pf != nil {
					patches := c.pf()
					defer patches.Reset()
				}

				if c.callback != nil {
					c.callback(&c)
				}

				if c.pf_after != nil {
					patches := c.pf_after()
					defer patches.Reset()
				}

				c.sl.Insert(c.node.name, c.node.score)
				c.verify(&c)
			})
		}
	})
}

func TestSkipDelete(t *testing.T) {
	type testcase struct {
		name         string
		node         *skiplistNode
		sl           SkipList
		pf           patch_fn
		callback     func(tc *testcase)
		verify       func(tc *testcase) // verify function to check the result
		deleteResult int
	}
	cases := []testcase{
		{
			"delete from head of the list",
			&skiplistNode{name: "a", score: 1},
			NewSkipList(),
			nil,
			func(tc *testcase) {
				sl := tc.sl
				sl.Insert("a", 1)
				sl.Insert("b", 2)
				sl.Insert("c", 3)
			},
			func(tc *testcase) {
				sl := tc.sl
				So(sl.Length(), ShouldEqual, 2)
				a := SkipListFind(sl, "a", 1)
				So(a, ShouldBeNil)
				head := SkipListHead(sl)
				b := SkipListFind(sl, "b", 2)
				So(head.level[0].foward, ShouldEqual, b)
				So(tc.deleteResult, ShouldEqual, 1)
			},
			-1,
		},
		{
			"delete from tail of the list",
			&skiplistNode{name: "c", score: 3},
			NewSkipList(),
			nil,
			func(tc *testcase) {
				sl := tc.sl
				sl.Insert("a", 1)
				sl.Insert("b", 2)
				sl.Insert("c", 3)
			},
			func(tc *testcase) {
				sl := tc.sl
				So(sl.Length(), ShouldEqual, 2)
				tail := SkipListTail(sl)
				b := SkipListFind(sl, "b", 2)
				So(tail, ShouldEqual, b)
				So(tc.deleteResult, ShouldEqual, 1)
			},
			-1,
		},
		{
			"delete from middle of the list",
			&skiplistNode{name: "b", score: 2},
			NewSkipList(),
			nil,
			func(tc *testcase) {
				sl := tc.sl
				sl.Insert("a", 1)
				sl.Insert("b", 2)
				sl.Insert("c", 3)
			},
			func(tc *testcase) {
				sl := tc.sl
				So(sl.Length(), ShouldEqual, 2)
				a := SkipListFind(sl, "a", 1)
				c := SkipListFind(sl, "c", 3)
				So(a.level[0].foward, ShouldEqual, c)
				So(c.backward, ShouldEqual, a)
				So(tc.deleteResult, ShouldEqual, 1)
			},
			-1,
		},
		{
			"delete an non-exist element",
			&skiplistNode{name: "d", score: 4},
			NewSkipList(),
			nil,
			func(tc *testcase) {
				sl := tc.sl
				sl.Insert("a", 1)
				sl.Insert("b", 2)
				sl.Insert("c", 3)
			},
			func(tc *testcase) {
				sl := tc.sl
				So(sl.Length(), ShouldEqual, 3)
				So(tc.deleteResult, ShouldEqual, 0)
			},
			-1,
		},
	}
	Convey("TestSkipDelete", t, func() {
		for _, c := range cases {
			Convey(c.name, func() {
				if c.pf != nil {
					defer c.pf().Reset()
				}

				if c.callback != nil {
					c.callback(&c)
				}

				c.deleteResult = c.sl.Delete(c.node.name, c.node.score)
				c.verify(&c)
			})
		}
	})
}

func TestSkipListNthInRange(t *testing.T) {
	type testcase struct {
		name      string
		pf_before patch_fn
		pf_after  patch_fn
		prepare   func(tc *testcase)
		verify    func(tc *testcase)
		sl        SkipList
		zrange    *ZRangeSpec
		n         int
		rank      int // rank of the node
	}

	insert_nodes := func(sl SkipList, n int, offset int) {
		for i := offset; i < n+offset; i++ {
			sl.Insert(utils.RadomString(i%20), float64(i))
		}
	}

	verify := func(tc *testcase) {
		node := tc.sl.NthInRange(tc.zrange, tc.n)
		expected := SkipListNthNode(tc.sl, tc.rank)
		So(node, ShouldEqual, expected)
	}

	cases := []*testcase{
		{
			"first node in the range and the range starts from the first node in the list",
			nil,
			nil,
			func(tc *testcase) { insert_nodes(tc.sl, 1000, 0) },
			func(tc *testcase) {
				node := tc.sl.NthInRange(tc.zrange, tc.n)
				expected := SkipListNthNode(tc.sl, tc.rank)
				So(node, ShouldEqual, expected)
			},
			NewSkipList(),
			&ZRangeSpec{0, 1000, false, false},
			0,
			0,
		},
		{
			"node in the middle of the range and range starts from the first node in the list",
			nil, nil, func(tc *testcase) { insert_nodes(tc.sl, 1000, 0) },
			verify, NewSkipList(),
			&ZRangeSpec{0, 1000, false, false},
			500,
			500,
		},
		{
			"get the node in the middle of the range and range starts from the middle of the list",
			nil, nil, func(tc *testcase) { insert_nodes(tc.sl, 1000, 0) },
			verify, NewSkipList(),
			&ZRangeSpec{821, 913, false, false},
			26, 847,
		},
		{
			"range min > list max",
			nil, nil, func(tc *testcase) { insert_nodes(tc.sl, 1000, 0) },
			verify, NewSkipList(),
			&ZRangeSpec{1008, 2000, true, false},
			8, 1016,
		},
		{
			"range max < list min",
			nil, nil, func(tc *testcase) { insert_nodes(tc.sl, 10, 120) },
			verify, NewSkipList(),
			&ZRangeSpec{1, 99, true, false},
			1, 11,
		},
		{
			"part of the list is in the range and the target node is in the list",
			nil, nil, func(tc *testcase) { insert_nodes(tc.sl, 1000, 0) },
			verify, NewSkipList(),
			&ZRangeSpec{500, 1500, false, false},
			125, 625,
		},
		{
			"part of the list is in the range but the target node is not in the list",
			nil, nil, func(tc *testcase) { insert_nodes(tc.sl, 1000, 0) },
			verify, NewSkipList(),
			&ZRangeSpec{500, 1500, false, false},
			781, 500 + 781,
		},
		{
			"last node in the range",
			nil, nil, func(tc *testcase) { insert_nodes(tc.sl, 1000, 0) },
			verify, NewSkipList(),
			&ZRangeSpec{12, 178, false, false},
			-1, 178,
		},
		{
			"second last node in the range",
			nil, nil, func(tc *testcase) { insert_nodes(tc.sl, 1000, 0) },
			verify, NewSkipList(),
			&ZRangeSpec{12, 178, false, false},
			-2, 177,
		},
		{
			"invalid range",
			nil, nil, func(tc *testcase) { insert_nodes(tc.sl, 1000, 0) },
			verify, NewSkipList(),
			&ZRangeSpec{192, 100, false, false},
			0, 10000,
		},
		{
			"target node exceeds the range",
			nil, nil, func(tc *testcase) {
				for i := 0; i < 1000; i++ {
					if i > 400 && i < 500 {
						continue
					}
					tc.sl.Insert(utils.RadomString(i%20), float64(i))
				}
			},
			verify, NewSkipList(),
			&ZRangeSpec{200, 600, false, false},
			315, 10000, // no such node in the list
		},
		{
			"target node exceeds the range and searching is reversed",
			nil, nil, func(tc *testcase) {
				for i := 0; i < 1000; i++ {
					if i > 400 && i < 500 {
						continue
					}
					tc.sl.Insert(utils.RadomString(i%20), float64(i))
				}
			},
			verify, NewSkipList(),
			&ZRangeSpec{200, 600, false, false},
			-325, 10000, // no such node in the list
		},
	}

	Convey("TestSkipListNthInRange", t, func() {
		for _, c := range cases {
			Convey(c.name, func() {
				if c.pf_before != nil {
					defer c.pf_before().Reset()
				}
				c.prepare(c)
				if c.pf_after != nil {
					defer c.pf_after().Reset()
				}

				c.verify(c)
			})
		}
	})
}

func SkipListFind(skip_list SkipList, name string, score float64) *skiplistNode {
	sl := skip_list.(*skiplist)
	x := sl.head
	for i := sl.level - 1; i >= 0; i-- {
		for x.level[i].foward != nil &&
			(x.level[i].foward.score < score || (x.level[i].foward.score == score && x.level[i].foward.name < name)) {
			x = x.level[i].foward
		}
	}

	x = x.level[0].foward
	if x == nil || x.score != score || x.name != name {
		return nil
	}

	return x
}

func SkipListHead(skip_list SkipList) *skiplistNode {
	sl := skip_list.(*skiplist)
	return sl.head
}

func SkipListTail(skip_list SkipList) *skiplistNode {
	sl := skip_list.(*skiplist)
	return sl.tail
}

func SkipListLevel(skip_list SkipList) int {
	sl := skip_list.(*skiplist)
	return sl.level
}

func SkipListNodeSpan(skip_list SkipList, name string, score float64) (prev []int, curr []int) {
	sl := skip_list.(*skiplist)
	x := sl.head
	update := make([]*skiplistNode, sl.level)
	for i := sl.level - 1; i >= 0; i-- {
		for x.level[i].foward != nil && (x.level[i].foward.score < score ||
			(x.level[i].foward.score == score && x.level[i].foward.name < name)) {
			x = x.level[i].foward
		}
		update[i] = x
	}
	x = x.level[0].foward
	if x == nil || x.score != score || x.name != name {
		return nil, nil
	}
	// count one by one
	// don't rely on the span of nodes
	prev = make([]int, sl.level)
	for i := 0; i < sl.level; i++ {
		for node := update[i].level[0].foward; node != nil; {
			prev[i]++
			if len(node.level) >= i+1 {
				break
			}
			node = node.level[0].foward
		}
	}

	curr = make([]int, len(x.level))
	for i := 0; i < len(x.level); i++ {
		for node := x.level[0].foward; node != nil; {
			curr[i]++
			if len(node.level) >= i+1 {
				break
			}
			node = node.level[0].foward
		}
	}

	return prev, curr
}

// n is 0-based
func SkipListNthNode(skip_list SkipList, n int) *skiplistNode {
	if n < 0 || n > skip_list.Length() {
		return nil
	}
	sl := skip_list.(*skiplist)
	x := sl.head
	for i := 0; i <= n; i++ {
		x = x.level[0].foward
	}
	return x
}
