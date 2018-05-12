.PHONY: all build vendor test

all: test

build: vendor
	govendor install +local

vendor:
	govendor sync

test: vendor
	govendor test +local

