# Makefile for user-platform
.PHONY: proto sqlc build test docker up down k8s-apply k8s-delete lint

BIN_DIR=bin

BUF_MODULE_DIR := api
SQLC_DIR=pkg/sqlc
MIGRATIONS_DIR := /Users/futurecx/Downloads/project_final/pkg/migrations/up
DB_URL := postgresql://postgres:postgres@192.168.1.11:5432/user_platform?sslmode=disable
SQLC_CONFIG := internal/user/sqlc/sqlc/sqlc.yaml


# The proto and sqlc targets are left empty because generated code is
# checked into version control for this exercise. If you wish to regenerate
# code using buf or sqlc, install the tools locally and invoke them here.
proto:
	@echo "Generating protobuf, gRPC, grpc-gateway, and OpenAPI..."
	cd $(BUF_MODULE_DIR) && buf generate
	@echo "✓ proto generation complete"

sqlc:
	@echo "Generating sqlc code from $(SQLC_CONFIG)..."
	sqlc generate -f $(SQLC_CONFIG)

migrate-up:
	migrate -path $(MIGRATIONS_DIR) -database "$(DB_URL)" up

migrate-down:
	migrate -path $(MIGRATIONS_DIR) -database "$(DB_URL)" down 1

build:
	@echo "Building binaries..."
	@go build -o $(BIN_DIR)/user-service cmd/user-service/main.go
	@go build -o $(BIN_DIR)/logger-service cmd/logger-service/main.go
	@go build -o $(BIN_DIR)/gateway cmd/gateway/main.go

test:
	@echo "Running tests..."
	@go test -v -race /Users/futurecx/Downloads/project_final/testcases


docker:
	@echo "Building Docker images..."
	@docker build -f build/Dockerfile.user -t user-service:latest .
	@docker build -f build/Dockerfile.logger -t logger-service:latest .
	@docker build -f build/Dockerfile.gateway -t gateway:latest .

up:
	@echo "Starting docker-compose..."
	@docker-compose up -d

down:
	@echo "Stopping docker-compose..."
	@docker-compose down

k8s-apply:
	@kubectl apply -f build/k8s

k8s-delete:
	@kubectl delete -f build/k8s

lint:
	@echo "Running go vet..."
	@go vet ./...
