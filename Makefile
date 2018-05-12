.PHONY: all build vendor test lint

all: lint test

build: vendor
    govendor install +local

vendor:
    govendor sync

test: vendor
    govendor test +local

lint:
    golint ./src/..
