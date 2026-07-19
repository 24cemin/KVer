.PHONY: proto build test test-compile lint run-server clean tidy \
       docker-build docker-up docker-down docker-logs \
       node1-kill node1-restart

# ─── Build ─────────────────────────────────────────────────────────────────────

build:
	go build ./...

build-bin:
	go build -o bin/server ./cmd/server
	go build -o bin/kvctl ./cmd/kvctl

# ─── Test ──────────────────────────────────────────────────────────────────────

test:
	go test -race ./...

test-compile:
	go test ./... -run NOMATCH

# ─── Proto ─────────────────────────────────────────────────────────────────────

proto:
	bash scripts/gen_proto.sh

# ─── Lint ──────────────────────────────────────────────────────────────────────

lint:
	golangci-lint run ./...

# ─── Docker ────────────────────────────────────────────────────────────────────

docker-build:
	docker compose build

docker-up:
	docker compose up -d

docker-down:
	docker compose down -v

docker-logs:
	docker compose logs -f

# ─── Node Management ──────────────────────────────────────────────────────────

node1-kill:
	docker compose stop node1

node1-restart:
	docker compose start node1

# ─── Dev ───────────────────────────────────────────────────────────────────────

run-server:
	go run ./cmd/server --node-id node1 --addr :7001 --peers ""

clean:
	rm -rf bin/
	rm -f *.wal *.snap *.db

tidy:
	go mod tidy
