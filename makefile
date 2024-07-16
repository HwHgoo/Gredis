rel: main.go
	go build -ldflags "-s -w" -o bin/Gredis
debug: main.go
	go build -o bin/Gredis