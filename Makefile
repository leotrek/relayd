GO ?= go
DESTDIR ?=
export DESTDIR
.PHONY: build install install-system test
build:
	mkdir -p bin
	$(GO) build -trimpath -o bin/relayd ./cmd/relayd
install: build install-system
install-system:
	sh scripts/install.sh
test:
	$(GO) test ./...
	python3 -m unittest discover -s scripts -p 'test_*.py' -v
