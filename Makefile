.PHONY: all build vendor test lint

all: lint test

build: vendor
	govendor install +local

vendor:
	govendor sync

test: vendor
	go test -race -coverprofile=coverage.txt -covermode=atomic

lint:
	golint ./...

