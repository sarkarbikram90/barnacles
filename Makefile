.PHONY: all build test race lint vet bench fuzz run-server run-agent docker clean

BIN_DIR := ./bin
SERVER_BIN := $(BIN_DIR)/barnacles-server
AGENT_BIN := $(BIN_DIR)/barnacles-agent

all: build

build:
	@mkdir -p $(BIN_DIR)
	go build -o $(SERVER_BIN) ./cmd/barnacles-server
	go build -o $(AGENT_BIN) ./cmd/barnacles-agent
	@echo "Build complete: $(SERVER_BIN), $(AGENT_BIN)"

test:
	go test -v ./...

race:
	go test -v -race ./...

vet:
	go vet ./...

lint:
	golangci-lint run ./...

bench:
	go test -bench=. -benchmem ./...

fuzz:
	go test -run=^$$ -fuzz=^FuzzJSONParser$$ -fuzztime=2s -timeout=30s ./internal/parser
	go test -run=^$$ -fuzz=^FuzzRegexpParser$$ -fuzztime=2s -timeout=30s ./internal/parser
	go test -run=^$$ -fuzz=^FuzzAutoParser$$ -fuzztime=2s -timeout=30s ./internal/parser

run-server:
	go run ./cmd/barnacles-server -config ./config/server.yaml

run-agent:
	go run ./cmd/barnacles-agent -config ./config/agent.yaml

docker:
	docker compose up --build

clean:
	rm -rf $(BIN_DIR) data/logs data/agent-spool data/demo
