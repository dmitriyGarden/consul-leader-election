.PHONY: all build vendor test lint

all: lint vendor test

vendor:
	govendor sync

test:
	go test -race -coverprofile=coverage.txt -covermode=atomic

lint:
	golint ./...

