# ClickHouse Integration Layer

## Overview

A comprehensive ClickHouse integration layer with connection pooling, retry logic, parameterized queries, and analytics support.

## Features

### 1. Table Schemas

#### Events Table
- **Schema**: `(event_id, event_type, user_id, page_url, timestamp)`
- **Partitioning**: `toYYYYMMDD(timestamp)` for efficient date-based queries
- **Indexes**: Secondary indexes on `event_type`, `page_url`, `user_id`
- **Engine**: MergeTree optimized for analytical queries

#### Metrics Table
- **Schema**: `(timestamp, metric_name, dimensions, value)`
- **Dimensions**: `Map(String, String)` for flexible key-value pairs
- **Partitioning**: `toYYYYMM(timestamp)` for monthly partitions
- **Indexes**: Secondary index on `metric_name`
- **Engine**: MergeTree optimized for analytics

### 2. Repository Pattern

#### Core Methods
- `InsertEvents()`: Batch insert events with retry logic
- `ExecuteQuery()`: Execute parameterized queries
- `ExecuteQueryWithResult()`: Execute queries with custom result scanning
- `ExportToCSV()`: Export query results to CSV files

### 3. Connection Management

- **Connection Pooling**: Configurable max connections (default: 10)
- **Idle Connections**: Configurable max idle connections (default: 5)
- **Connection Lifetime**: Configurable max lifetime (default: 1 hour)
- **Automatic Reconnection**: Handled by ClickHouse driver

### 4. Retry Logic

- **Retry Attempts**: Configurable (default: 3)
- **Retry Backoff**: Exponential backoff (default: 100ms)
- **Retryable Errors**: Network errors, timeouts, connection failures

### 5. Query Configuration

- **Query Timeout**: Configurable (default: 10 seconds)
- **Parameterized Queries**: All queries use parameters to prevent SQL injection
- **Context Support**: All operations support context cancellation

### 6. Analytics Repository

Pre-built methods for common analytics:
- `GetTopPages()`: Top N pages by click count
- `GetHourlyTransactions()`: Hourly transaction statistics
- `ExportTopPagesToCSV()`: Export top pages to CSV
- `ExportHourlyTransactionsToCSV()`: Export hourly transactions to CSV

## Usage

### Basic Repository Usage

```go
import "devops-fall2025-23b031152/pkg/clickhouse"

// Create repository
config := clickhouse.Config{
    Host:         "clickhouse-server",
    Port:         "9000",
    Database:     "analytics",
    Username:     "default",
    Password:     "clickhouse123",
    QueryTimeout: 10 * time.Second,
    RetryAttempts: 3,
}

repo, err := clickhouse.NewRepository(config, logger)
if err != nil {
    log.Fatal(err)
}
defer repo.Close()

// Insert events
events := []clickhouse.Event{
    {
        EventID:   "event-1",
        EventType: "click",
        UserID:    "user-123",
        PageURL:   "/home",
        Timestamp: time.Now(),
    },
}

err = repo.InsertEvents(ctx, events)

// Execute query
rows, err := repo.ExecuteQuery(ctx, 
    "SELECT * FROM analytics.events WHERE event_type = ? LIMIT ?",
    "click", 10)
defer rows.Close()

// Export to CSV
err = repo.ExportToCSV(ctx,
    "SELECT page_url, count() FROM analytics.events GROUP BY page_url",
    "/tmp/export.csv")
```

### Analytics Repository Usage

```go
analyticsRepo := clickhouse.NewAnalyticsRepository(repo, logger)

// Get top pages
startTime := time.Now().AddDate(0, 0, -7)
endTime := time.Now()
topPages, err := analyticsRepo.GetTopPages(ctx, startTime, endTime, 10)

// Get hourly transactions
hourlyTxs, err := analyticsRepo.GetHourlyTransactions(ctx, startTime, endTime)

// Export to CSV
err = analyticsRepo.ExportTopPagesToCSV(ctx, startTime, endTime, 10, "/tmp/top_pages.csv")
```

## Configuration

Environment variables:
- `CLICKHOUSE_HOST`: Server hostname
- `CLICKHOUSE_PORT`: Server port
- `CLICKHOUSE_DATABASE`: Database name
- `CLICKHOUSE_USER`: Username
- `CLICKHOUSE_PASSWORD`: Password
- `CLICKHOUSE_MAX_OPEN_CONNS`: Max open connections (default: 10)
- `CLICKHOUSE_MAX_IDLE_CONNS`: Max idle connections (default: 5)
- `CLICKHOUSE_QUERY_TIMEOUT`: Query timeout (default: 10s)
- `CLICKHOUSE_RETRY_ATTEMPTS`: Retry attempts (default: 3)
- `CLICKHOUSE_RETRY_BACKOFF`: Retry backoff (default: 100ms)

## Migrations

Migration scripts are located in `clickhouse/migrations/`:
- `001_create_events_table.sql`: Creates events table
- `002_create_metrics_table.sql`: Creates metrics table
- `003_add_event_type_column.sql`: Adds event_type column (if needed)
- `004_update_metrics_dimensions.sql`: Updates metrics to use Map type

Run migrations using the migration runner:
```go
runner := clickhouse.NewMigrationRunner(repo, logger)
err := runner.RunAllMigrations(ctx, "clickhouse/migrations")
```

## Example Queries

See `clickhouse/examples/analytics_queries.sql` for:
- Top 10 pages by click count
- Hourly transactions
- User activity by hour
- Page performance metrics
- Daily summary statistics
- And more...

## Security

- **Parameterized Queries**: All queries use parameters to prevent SQL injection
- **Connection Security**: Supports TLS/SSL connections
- **Authentication**: Username/password authentication
- **Query Timeout**: Prevents long-running queries from blocking

## Performance

- **Batch Inserts**: Efficient batch insertion for high throughput
- **Connection Pooling**: Reuses connections for better performance
- **Secondary Indexes**: Optimized indexes for common query patterns
- **Partitioning**: Date-based partitioning for efficient queries

