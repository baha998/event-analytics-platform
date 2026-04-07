package clickhouse

import (
	"context"
	"encoding/csv"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/rs/zerolog"
)

// Config holds ClickHouse connection configuration
type Config struct {
	Host            string
	Port            string
	Database        string
	Username        string
	Password        string
	MaxOpenConns    int
	MaxIdleConns    int
	ConnMaxLifetime time.Duration
	QueryTimeout    time.Duration
	RetryAttempts   int
	RetryBackoff    time.Duration
}

// DefaultConfig returns default configuration
func DefaultConfig() Config {
	return Config{
		MaxOpenConns:    10,
		MaxIdleConns:    5,
		ConnMaxLifetime: time.Hour,
		QueryTimeout:    10 * time.Second,
		RetryAttempts:   3,
		RetryBackoff:    100 * time.Millisecond,
	}
}

// Repository provides ClickHouse operations with connection pooling and retry logic
type Repository struct {
	conn   driver.Conn
	config Config
	log    zerolog.Logger
	mu     sync.RWMutex
}

// NewRepository creates a new ClickHouse repository with connection pooling
func NewRepository(config Config, log zerolog.Logger) (*Repository, error) {
	opts := &clickhouse.Options{
		Addr: []string{fmt.Sprintf("%s:%s", config.Host, config.Port)},
		Auth: clickhouse.Auth{
			Database: config.Database,
			Username: config.Username,
			Password: config.Password,
		},
		DialTimeout:     10 * time.Second,
		MaxOpenConns:    config.MaxOpenConns,
		MaxIdleConns:    config.MaxIdleConns,
		ConnMaxLifetime: config.ConnMaxLifetime,
		Compression: &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		},
	}

	conn, err := clickhouse.Open(opts)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to ClickHouse: %w", err)
	}

	// Test connection
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := conn.Ping(ctx); err != nil {
		return nil, fmt.Errorf("failed to ping ClickHouse: %w", err)
	}

	log.Info().
		Str("host", config.Host).
		Str("database", config.Database).
		Int("max_open_conns", config.MaxOpenConns).
		Msg("ClickHouse repository initialized")

	return &Repository{
		conn:   conn,
		config: config,
		log:    log,
	}, nil
}

// executeWithRetry executes a function with retry logic
func (r *Repository) executeWithRetry(ctx context.Context, fn func() error) error {
	var lastErr error
	for attempt := 0; attempt <= r.config.RetryAttempts; attempt++ {
		if attempt > 0 {
			backoff := time.Duration(attempt) * r.config.RetryBackoff
			r.log.Warn().
				Err(lastErr).
				Int("attempt", attempt).
				Dur("backoff", backoff).
				Msg("Retrying ClickHouse operation")
			time.Sleep(backoff)
		}

		if err := fn(); err != nil {
			lastErr = err
			// Check if error is retryable
			if !isRetryableError(err) {
				return err
			}
			continue
		}

		return nil
	}

	return fmt.Errorf("operation failed after %d attempts: %w", r.config.RetryAttempts, lastErr)
}

// isRetryableError checks if an error is retryable
func isRetryableError(err error) bool {
	// Network errors, timeouts, and connection errors are retryable
	errStr := err.Error()
	return contains(errStr, "timeout") ||
		contains(errStr, "connection") ||
		contains(errStr, "network") ||
		contains(errStr, "broken pipe")
}

func contains(s, substr string) bool {
	if len(substr) == 0 {
		return true
	}
	if len(s) < len(substr) {
		return false
	}
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

// InsertEvents inserts multiple events in a batch with retry logic
func (r *Repository) InsertEvents(ctx context.Context, events []Event) error {
	if len(events) == 0 {
		return nil
	}

	return r.executeWithRetry(ctx, func() error {
		ctx, cancel := context.WithTimeout(ctx, r.config.QueryTimeout)
		defer cancel()

		batch, err := r.conn.PrepareBatch(ctx, `
			INSERT INTO analytics.events 
			(event_id, event_type, user_id, page_url, timestamp)
		`)
		if err != nil {
			return fmt.Errorf("failed to prepare batch: %w", err)
		}

		for _, event := range events {
			if err := batch.Append(
				event.EventID,
				event.EventType,
				event.UserID,
				event.PageURL,
				event.Timestamp,
			); err != nil {
				return fmt.Errorf("failed to append event: %w", err)
			}
		}

		if err := batch.Send(); err != nil {
			return fmt.Errorf("failed to send batch: %w", err)
		}

		r.log.Debug().Int("count", len(events)).Msg("Events inserted successfully")
		return nil
	})
}

// ExecuteQuery executes a parameterized query and returns rows
func (r *Repository) ExecuteQuery(ctx context.Context, query string, args ...interface{}) (driver.Rows, error) {
	var rows driver.Rows
	var queryErr error

	err := r.executeWithRetry(ctx, func() error {
		ctx, cancel := context.WithTimeout(ctx, r.config.QueryTimeout)
		defer cancel()

		rows, queryErr = r.conn.Query(ctx, query, args...)
		return queryErr
	})

	if err != nil {
		return nil, fmt.Errorf("query execution failed: %w", err)
	}

	return rows, nil
}

// ExecuteQueryWithResult executes a query and scans results using a scanner function
func (r *Repository) ExecuteQueryWithResult(ctx context.Context, query string, scanner func(rows driver.Rows) error, args ...interface{}) error {
	rows, err := r.ExecuteQuery(ctx, query, args...)
	if err != nil {
		return err
	}
	defer rows.Close()

	return scanner(rows)
}

// ExportToCSV exports query results to a CSV file
func (r *Repository) ExportToCSV(ctx context.Context, query string, filePath string, args ...interface{}) error {
	rows, err := r.ExecuteQuery(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("failed to execute query: %w", err)
	}
	defer rows.Close()

	file, err := os.Create(filePath)
	if err != nil {
		return fmt.Errorf("failed to create file: %w", err)
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	// Write header
	columns := rows.Columns()
	if err := writer.Write(columns); err != nil {
		return fmt.Errorf("failed to write header: %w", err)
	}

	// Write rows
	rowCount := 0
	for rows.Next() {
		row := make([]interface{}, len(columns))
		rowPtrs := make([]interface{}, len(columns))
		for i := range row {
			rowPtrs[i] = &row[i]
		}

		if err := rows.Scan(rowPtrs...); err != nil {
			r.log.Error().Err(err).Msg("Failed to scan row")
			continue
		}

		// Convert row to string slice
		record := make([]string, len(columns))
		for i, val := range row {
			record[i] = fmt.Sprintf("%v", val)
		}

		if err := writer.Write(record); err != nil {
			return fmt.Errorf("failed to write row: %w", err)
		}
		rowCount++
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("error iterating rows: %w", err)
	}

	r.log.Info().
		Str("file", filePath).
		Int("rows", rowCount).
		Msg("CSV export completed")

	return nil
}

// Close closes the repository connection
func (r *Repository) Close() error {
	return r.conn.Close()
}

// Ping checks the connection
func (r *Repository) Ping(ctx context.Context) error {
	return r.conn.Ping(ctx)
}

// Event represents an event record
type Event struct {
	EventID   string
	EventType string
	UserID    string
	PageURL   string
	Timestamp time.Time
}
