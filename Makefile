# hawarden — build and packaging. The SPA has no build step: dist/ is
# embedded directly, so `make build`/`make console` need only Go.
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
GOFLAGS  = -trimpath -ldflags "-s -w -X main.version=$(VERSION)"

.PHONY: build build-linux console test vet clean docker-testbed

build:
	CGO_ENABLED=0 go build $(GOFLAGS) -o bin/hawarden ./cmd/hawarden

console:
	CGO_ENABLED=0 go build $(GOFLAGS) -o bin/hawardenconsole ./cmd/hawardenconsole

build-linux:
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build $(GOFLAGS) -o dist/hawarden-linux-amd64 ./cmd/hawarden
	CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build $(GOFLAGS) -o dist/hawarden-linux-arm64 ./cmd/hawarden
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build $(GOFLAGS) -o dist/hawardenconsole-linux-amd64 ./cmd/hawardenconsole
	CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build $(GOFLAGS) -o dist/hawardenconsole-linux-arm64 ./cmd/hawardenconsole

test:
	go test -race ./...

vet:
	go vet ./... && gofmt -l .

clean:
	rm -rf bin dist

# Local docker testbed (compose file lives in testbed/, which is not pushed)
docker-testbed:
	docker compose -f testbed/docker-compose.yml up -d --build
