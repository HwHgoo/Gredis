package zset

type ZSet interface {
	Insert(name string, score float64)
	Delete(name string)
	Contains(name string) bool
	GetScore(name string) (float64, bool)
	GetRange(start, end float64) []string
}

type zset struct {
	head SkipList
	tail SkipList
	// fast access score by keyname
	m      map[string]float64 // keynamae -> score
	level  int                // current level of the list
	length int                // current length of the list
}

func NewZSet() ZSet {
	return nil
}
