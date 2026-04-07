package clickhouse

import (
	"context"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/rs/zerolog"
)

// AnalyticsRepository provides analytics-specific query methods
type AnalyticsRepository struct {
	repo *Repository
	log  zerolog.Logger
}

// NewAnalyticsRepository creates a new analytics repository
func NewAnalyticsRepository(repo *Repository, log zerolog.Logger) *AnalyticsRepository {
	return &AnalyticsRepository{
		repo: repo,
		log:  log,
	}
}

// TopPagesResult represents top pages query result
type TopPagesResult struct {
	PageURL     string
	ClickCount  uint64
	UniqueUsers uint64
}

// GetTopPages returns top N pages by click count
func (r *AnalyticsRepository) GetTopPages(ctx context.Context, startTime, endTime time.Time, limit int) ([]TopPagesResult, error) {
	query := `
		SELECT 
			page_url,
			count() as click_count,
			count(DISTINCT user_id) as unique_users
		FROM analytics.events
		WHERE event_type = 'click'
			AND timestamp >= ?
			AND timestamp <= ?
		GROUP BY page_url
		ORDER BY click_count DESC
		LIMIT ?
	`

	var results []TopPagesResult
	err := r.repo.ExecuteQueryWithResult(ctx, query, func(rows driver.Rows) error {
		for rows.Next() {
			var result TopPagesResult
			if err := rows.Scan(&result.PageURL, &result.ClickCount, &result.UniqueUsers); err != nil {
				return fmt.Errorf("failed to scan row: %w", err)
			}
			results = append(results, result)
		}
		return rows.Err()
	}, startTime, endTime, limit)

	if err != nil {
		return nil, fmt.Errorf("failed to get top pages: %w", err)
	}

	return results, nil
}

// HourlyTransactionResult represents hourly transaction query result
type HourlyTransactionResult struct {
	Hour             time.Time
	TransactionCount uint64
	UniqueUsers      uint64
}

// GetHourlyTransactions returns hourly transaction statistics
func (r *AnalyticsRepository) GetHourlyTransactions(ctx context.Context, startTime, endTime time.Time) ([]HourlyTransactionResult, error) {
	query := `
		SELECT 
			toStartOfHour(timestamp) as hour,
			count() as transaction_count,
			count(DISTINCT user_id) as unique_users
		FROM analytics.events
		WHERE event_type = 'transaction'
			AND timestamp >= ?
			AND timestamp <= ?
		GROUP BY hour
		ORDER BY hour DESC
	`

	var results []HourlyTransactionResult
	err := r.repo.ExecuteQueryWithResult(ctx, query, func(rows driver.Rows) error {
		for rows.Next() {
			var result HourlyTransactionResult
			if err := rows.Scan(&result.Hour, &result.TransactionCount, &result.UniqueUsers); err != nil {
				return fmt.Errorf("failed to scan row: %w", err)
			}
			results = append(results, result)
		}
		return rows.Err()
	}, startTime, endTime)

	if err != nil {
		return nil, fmt.Errorf("failed to get hourly transactions: %w", err)
	}

	return results, nil
}

// ExportTopPagesToCSV exports top pages to CSV
func (r *AnalyticsRepository) ExportTopPagesToCSV(ctx context.Context, startTime, endTime time.Time, limit int, filePath string) error {
	query := `
		SELECT 
			page_url,
			count() as click_count,
			count(DISTINCT user_id) as unique_users
		FROM analytics.events
		WHERE event_type = 'click'
			AND timestamp >= ?
			AND timestamp <= ?
		GROUP BY page_url
		ORDER BY click_count DESC
		LIMIT ?
	`

	return r.repo.ExportToCSV(ctx, query, filePath, startTime, endTime, limit)
}

// ExportHourlyTransactionsToCSV exports hourly transactions to CSV
func (r *AnalyticsRepository) ExportHourlyTransactionsToCSV(ctx context.Context, startTime, endTime time.Time, filePath string) error {
	query := `
		SELECT 
			toStartOfHour(timestamp) as hour,
			count() as transaction_count,
			count(DISTINCT user_id) as unique_users
		FROM analytics.events
		WHERE event_type = 'transaction'
			AND timestamp >= ?
			AND timestamp <= ?
		GROUP BY hour
		ORDER BY hour DESC
	`

	return r.repo.ExportToCSV(ctx, query, filePath, startTime, endTime)
}
