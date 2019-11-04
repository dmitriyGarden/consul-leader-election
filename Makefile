.PHONY: all build test lint

all: lint  test

test:
	go test -race -coverprofile=coverage.txt -covermode=atomic

lint:
	golint ./...

