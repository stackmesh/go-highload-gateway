GO ?= go
PROTOC ?= protoc

.PHONY: test race bench peak tidy proto up down load

test:
	$(GO) test ./...

race:
	$(GO) test -race ./...

bench:
	$(GO) test ./internal/... -bench=. -benchmem -run '^$$'

peak:
	PEAK=1 $(GO) test ./internal/gateway -run TestSustainsFiveThousandRPS -count=1 -v

tidy:
	$(GO) mod tidy

proto:
	$(PROTOC) -I api/proto \
		--go_out=. --go_opt=module=github.com/stackmesh/go-highload-gateway \
		--go-grpc_out=. --go-grpc_opt=module=github.com/stackmesh/go-highload-gateway \
		api/proto/gateway/v1/gateway.proto

up:
	docker compose up -d --build

down:
	docker compose down -v

load:
	$(GO) run ./cmd/loadgen -url http://127.0.0.1:8080/v1/echo -key gw_live_demo -c 128 -d 15s
