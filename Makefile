all: build

build:
	go build github.com/ytinirt/doggie/cmd/doggie

clean:
	@git clean -xdf .

.PHONY: build clean
