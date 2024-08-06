package zset

import (
	"testing"

	. "github.com/agiledragon/gomonkey/v2"
	. "github.com/smartystreets/goconvey/convey"
)

type patch_fn func() *Patches

func TestSkipListInsert(t *testing.T) {
	type testcase struct {
		name     string
		pf       patch_fn
		sl       SkipList
		callback func(tc *testcase) // callback function to prepare testcase data
		verify   func(tc *testcase) // verify function to check the result
		node     *skiplistNode
	}

	cases := []testcase{
		{
			"insert one element into empty list",
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
