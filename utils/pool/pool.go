package pool

type Pool[T any] struct {
	pool       chan T
	newElement func() T
}

func (p *Pool[T]) Get() T {
	select {
	case elem := <-p.pool:
		return elem
	default:
		return p.newElement()
	}
}

func (p *Pool[T]) Put(elem T) {
	select {
	case p.pool <- elem:
	default:
		// drop the element if the pool is full
	}
}

func MakePool[T any](size int, newElem func() T) *Pool[T] {
	return &Pool[T]{
		pool:       make(chan T, size),
		newElement: newElem,
	}
}
