all: build

build:
	GO111MODULE="off" CGO_ENABLED=0 go build github.com/ytinirt/doggie/cmd/doggie

clean:
	@git clean -xdf .

.PHONY: build clean
