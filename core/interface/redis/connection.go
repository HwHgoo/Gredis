package redis

type Connection interface {
	GetSelectedDb() int
	SelectDb(db int)
}
