# Architecture Overview

This project follows clean architecture principles with a clear separation of concerns.

## Project Structure

```
project/
├── cmd/
│   └── api/              # Application entry point
│       └── main.go
├── internal/              # Private application code
│   ├── config/           # Configuration management
│   ├── handler/          # HTTP handlers (presentation layer)
│   ├── service/          # Business logic layer
│   ├── repository/       # Data access layer
│   └── middleware/       # HTTP middleware (auth, metrics)
├── pkg/                  # Public reusable packages
│   ├── auth/             # JWT authentication
│   ├── kafka/            # Kafka producer/consumer
│   └── metrics/          # Prometheus metrics
├── docker-compose.yml    # Docker Compose setup
└── Dockerfile           # Application Dockerfile
```

## Clean Architecture Layers

### Handler Layer (`internal/handler/`)
- Handles HTTP requests/responses
- Validates input
- Calls service layer
- Returns JSON responses

### Service Layer (`internal/service/`)
- Contains business logic
- Orchestrates repository and Kafka operations
- Handles event processing

### Repository Layer (`internal/repository/`)
- Data access abstraction
- ClickHouse operations
- Database queries

## API Endpoints

### POST /api/sources
- **Auth**: Required (Admin role)
- **Description**: Creates a source and generates events to Kafka
- **Body**: `{ "topic": "events", "count": 10, "interval": 100 }`

### POST /api/process
- **Auth**: Required (Admin role)
- **Description**: Processes a single event and stores it in ClickHouse
- **Body**: `{ "event_id": "...", "timestamp": 1234567890, "page_url": "/home", "user_id": 1001 }`

### GET /api/query
- **Auth**: Required (Viewer or Admin role)
- **Description**: Queries events from ClickHouse
- **Query Params**: `start_time`, `end_time`, `page_url`, `user_id`, `limit`

### GET /health
- **Auth**: Not required
- **Description**: Health check endpoint

### GET /metrics
- **Auth**: Not required
- **Description**: Prometheus metrics endpoint

## Authentication

The API uses JWT-based authentication with role-based access control (RBAC).

### Roles
- **admin**: Full access to all endpoints
- **viewer**: Read-only access (can query events)

### Generating JWT Tokens

Use the token generator utility:
```bash
go run pkg/auth/token_gen.go <username> <role>
```

Or use the JWT manager programmatically:
```go
jwtManager := auth.NewJWTManager(secretKey, tokenExpiry, refreshExpiry)
token, err := jwtManager.GenerateToken("user1", "admin")
```

### Using Tokens

Include the token in the Authorization header:
```
Authorization: Bearer <token>
```

## Kafka Integration

- **Producer**: Sends events to Kafka topics
- **Consumer**: Consumes events from Kafka and stores them in ClickHouse
- Uses Shopify/sarama library for Kafka operations

## Metrics

Prometheus metrics are exposed at `/metrics`:
- HTTP request metrics
- Kafka producer/consumer metrics
- ClickHouse query metrics
- Event processing metrics

## Configuration

Configuration is loaded from environment variables. See `internal/config/config.go` for all available options.

## Docker Compose Services

- **zookeeper**: Kafka coordination
- **kafka**: Message broker
- **clickhouse-server**: Analytical database
- **prometheus**: Metrics collection
- **grafana**: Metrics visualization
- **go-api**: Application service

