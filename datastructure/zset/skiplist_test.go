package zset

import "testing"

func TestSkipListInsert(t *testing.T) {
	sl := NewSkipList()
	sl.Insert("a", 1)
	sl.Insert("c", 3)
	sl.Insert("b", 2)
}

func TestSkipDelete(t *testing.T) {
	sl := NewSkipList()
	sl.Insert("a", 1)
	sl.Insert("c", 3)
	sl.Insert("b", 2)
	sl.Delete("c", 3)
	if sl.Length() != 2 {
		t.Fail()
	}
}
