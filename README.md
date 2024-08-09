# Gredis - Redis implementation in Golang

## Goal of this project

- Implement Redis server in Golang
- Deeper dive into Redis and Golang

## Implementing Progress
### Commands
#### Basic server commands
- [ ] PING
- [ ] SELECT
- [ ] ...

#### Done
- [x] String

#### In progress
- ZSet

#### TODOs
- Hash
- List
- Set
- Stream

### Persistence
- [ ] RDB: Linux `fork()` doesn't work well with Golang. It may require an implementation of `Copy-On-Write` mechanism.
- [ ] AOF

### Cluster
not implemented yet

### Pub/Sub
not implemented yet

### Transactions
not implemented yet