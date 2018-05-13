.PHONY: all build vendor test lint path

all: path lint test

build: vendor
	govendor install +local

vendor:
	govendor sync

test: vendor
	govendor test +local

lint:
	golint ./...

path:
	echo $PATH
