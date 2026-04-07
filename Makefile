.PHONY: help test test-coverage lint build run docker-build docker-up docker-down clean

# Default target
help: ## Show this help message
	@echo 'Usage: make [target]'
	@echo ''
	@echo 'Available targets:'
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

# Testing
test: ## Run all tests
	go test -v -race ./...

test-coverage: ## Run tests with coverage report
	go test -v -race -coverprofile=coverage.out -covermode=atomic ./...
	go tool cover -html=coverage.out -o coverage.html
	go tool cover -func=coverage.out | tail -n 1

test-short: ## Run short tests only
	go test -v -short ./...

# Linting
lint: ## Run golangci-lint
	golangci-lint run --timeout 5m ./...

lint-fix: ## Run golangci-lint with auto-fix
	golangci-lint run --fix ./...

# Building
build: ## Build the Go application
	CGO_ENABLED=0 go build -o bin/api ./cmd/api

build-linux: ## Build for Linux
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o bin/api-linux ./cmd/api

# Running
run: ## Run the application locally
	go run ./cmd/api

# Docker
docker-build: ## Build Docker image
	docker build -t project-go-api:latest .

docker-up: ## Start all services with docker-compose
	docker-compose up -d

docker-down: ## Stop all services
	docker-compose down

docker-logs: ## Follow logs from all services
	docker-compose logs -f

docker-ps: ## Show docker-compose service status
	docker-compose ps

docker-restart: ## Restart go-api service
	docker-compose restart go-api


# Dependencies
deps: ## Download dependencies
	go mod download

deps-update: ## Update dependencies
	go get -u ./...
	go mod tidy

# CI/CD helpers
ci-test: test-coverage lint ## Run CI tests (coverage + lint)

ci-build: build-linux ## Build for CI/CD

# Development
dev: docker-up ## Start development environment
	@echo "Services starting..."
	@sleep 5
	docker-compose logs -f go-api

dev-rebuild: docker-down docker-build docker-up ## Rebuild and restart dev environment

# Health checks
health-check: ## Check service health endpoints
	@echo "Checking health endpoints..."
	@curl -f http://localhost:8080/health || echo "API health check failed"
	@curl -f http://localhost:9090/-/healthy || echo "Prometheus health check failed"
	@curl -f http://localhost:3000/api/health || echo "Grafana health check failed"

# Monitoring
metrics: ## Show Prometheus metrics
	curl http://localhost:8080/metrics

logs-api: ## Follow API logs
	docker-compose logs -f go-api

logs-kafka: ## Follow Kafka logs
	docker-compose logs -f kafka

logs-clickhouse: ## Follow ClickHouse logs
	docker-compose logs -f clickhouse-server
