package zset

type ZSet interface {
	Insert(member string, score float64)
	Delete(member string, score float64)
	Update(member string, newscore float64)
	Score(member string) (float64, bool)
	Rank(member string, score float64) int
	NthInRange(zrange *ZRangeSpec, n int) SkipListNode
	GetRange(start, end float64) []string
	Card() int
}

type zset struct {
	skiplist SkipList
	// fast access score by keyname
	m map[string]float64 // member -> score
}

type ZRangeSpec struct {
	Min   float64
	Max   float64
	MinEx bool
	MaxEx bool
}

func (zrange *ZRangeSpec) ValueGteMin(val float64) bool {
	if zrange.MinEx {
		return val > zrange.Min
	}

	return val >= zrange.Min
}

func (zrange *ZRangeSpec) ValueLteMax(val float64) bool {
	if zrange.MaxEx {
		return val < zrange.Max
	}

	return val <= zrange.Max
}

func NewZSet() ZSet {
	set := &zset{
		skiplist: NewSkipList(),
		m:        make(map[string]float64),
	}
	return set
}

func (z *zset) Insert(member string, score float64) {
	if s, ok := z.m[member]; ok && s == score {
		return
	}
	z.m[member] = score
	z.skiplist.Insert(member, score)
}

func (z *zset) Delete(member string, score float64) {
	if _, ok := z.m[member]; !ok {
		return
	}

	z.skiplist.Delete(member, score)
	delete(z.m, member)
}

func (z *zset) Update(member string, newscore float64) {
	curscore := z.m[member]
	z.m[member] = newscore
	z.skiplist.Delete(member, curscore)
	z.skiplist.Insert(member, newscore)
}

func (z *zset) Score(member string) (float64, bool) {
	s, ok := z.m[member]
	return s, ok
}

func (z *zset) Rank(member string, score float64) int {
	curscore, ok := z.m[member]
	if !ok || curscore != score {
		return 0
	}

	return z.skiplist.GetRank(member, score)
}

func (z *zset) NthInRange(zrange *ZRangeSpec, n int) SkipListNode {
	node := z.skiplist.NthInRange(zrange, n)
	return node
}

func (z *zset) GetRange(start, end float64) []string {
	return nil
}

func (z *zset) Card() int {
	return len(z.m)
}
