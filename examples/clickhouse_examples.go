package examples

import (
	"context"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/rs/zerolog"

	"devops-fall2025-23b031152/pkg/clickhouse"
)

// ExampleClickHouseIntegration demonstrates ClickHouse integration usage
func ExampleClickHouseIntegration(logger zerolog.Logger) {
	// Create repository configuration
	config := clickhouse.Config{
		Host:            "clickhouse-server",
		Port:            "9000",
		Database:        "analytics",
		Username:        "default",
		Password:        "clickhouse123",
		MaxOpenConns:    10,
		MaxIdleConns:    5,
		ConnMaxLifetime: time.Hour,
		QueryTimeout:    10 * time.Second,
		RetryAttempts:   3,
		RetryBackoff:    100 * time.Millisecond,
	}

	// Create repository
	repo, err := clickhouse.NewRepository(config, logger)
	if err != nil {
		logger.Fatal().Err(err).Msg("Failed to create repository")
	}
	defer repo.Close()

	ctx := context.Background()

	// Example 1: Insert events
	events := []clickhouse.Event{
		{
			EventID:   "event-1",
			EventType: "click",
			UserID:    "user-123",
			PageURL:   "/home",
			Timestamp: time.Now(),
		},
		{
			EventID:   "event-2",
			EventType: "transaction",
			UserID:    "user-456",
			PageURL:   "/checkout",
			Timestamp: time.Now(),
		},
	}

	if err := repo.InsertEvents(ctx, events); err != nil {
		logger.Error().Err(err).Msg("Failed to insert events")
		return
	}

	logger.Info().Int("count", len(events)).Msg("Events inserted")

	// Example 2: Execute parameterized query
	query := `
		SELECT 
			page_url,
			count() as click_count
		FROM analytics.events
		WHERE event_type = ?
			AND timestamp >= ?
			AND timestamp <= ?
		GROUP BY page_url
		ORDER BY click_count DESC
		LIMIT ?
	`

	startTime := time.Now().AddDate(0, 0, -7)
	endTime := time.Now()

	type QueryResult struct {
		PageURL    string
		ClickCount uint64
	}

	var results []QueryResult
	err = repo.ExecuteQueryWithResult(ctx, query, func(rows driver.Rows) error {
		for rows.Next() {
			var result QueryResult
			if err := rows.Scan(&result.PageURL, &result.ClickCount); err != nil {
				return fmt.Errorf("failed to scan row: %w", err)
			}
			results = append(results, result)
		}
		return rows.Err()
	}, "click", startTime, endTime, 10)

	if err != nil {
		logger.Error().Err(err).Msg("Query failed")
		return
	}

	logger.Info().Int("results", len(results)).Msg("Query executed")

	// Example 3: Export to CSV
	exportQuery := `
		SELECT 
			page_url,
			count() as click_count
		FROM analytics.events
		WHERE event_type = 'click'
			AND timestamp >= ?
			AND timestamp <= ?
		GROUP BY page_url
		ORDER BY click_count DESC
		LIMIT 100
	`

	if err := repo.ExportToCSV(ctx, exportQuery, "/tmp/top_pages.csv", startTime, endTime); err != nil {
		logger.Error().Err(err).Msg("CSV export failed")
		return
	}

	logger.Info().Str("file", "/tmp/top_pages.csv").Msg("CSV exported")

	// Example 4: Use analytics repository
	analyticsRepo := clickhouse.NewAnalyticsRepository(repo, logger)

	// Get top pages
	topPages, err := analyticsRepo.GetTopPages(ctx, startTime, endTime, 10)
	if err != nil {
		logger.Error().Err(err).Msg("Failed to get top pages")
		return
	}

	for _, page := range topPages {
		fmt.Printf("Page: %s, Clicks: %d, Users: %d\n", page.PageURL, page.ClickCount, page.UniqueUsers)
	}

	// Get hourly transactions
	hourlyTxs, err := analyticsRepo.GetHourlyTransactions(ctx, startTime, endTime)
	if err != nil {
		logger.Error().Err(err).Msg("Failed to get hourly transactions")
		return
	}

	for _, tx := range hourlyTxs {
		fmt.Printf("Hour: %s, Transactions: %d, Users: %d\n", tx.Hour, tx.TransactionCount, tx.UniqueUsers)
	}

	// Export to CSV
	if err := analyticsRepo.ExportTopPagesToCSV(ctx, startTime, endTime, 10, "/tmp/top_pages_export.csv"); err != nil {
		logger.Error().Err(err).Msg("Failed to export top pages")
		return
	}

	logger.Info().Msg("ClickHouse integration example completed")
}

