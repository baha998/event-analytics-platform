package batch

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"time"

	"github.com/rs/zerolog"

	"devops-fall2025-23b031152/internal/storage"
)

// Exporter handles batch export of events to MinIO as Parquet files
type Exporter struct {
	db          *sql.DB
	minioClient *storage.MinIOClient
	log         zerolog.Logger
}

// NewExporter creates a new batch exporter
func NewExporter(db *sql.DB, minioClient *storage.MinIOClient, log zerolog.Logger) *Exporter {
	return &Exporter{
		db:          db,
		minioClient: minioClient,
		log:         log,
	}
}

// ExportDailyEvents exports events for a specific date to MinIO as Parquet
func (e *Exporter) ExportDailyEvents(date string) error {
	// Create context with 5-minute timeout
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	e.log.Info().Str("date", date).Msg("Starting daily events export")

	// Step 1: Query ClickHouse for events
	e.log.Debug().Msg("Querying ClickHouse for events")
	events, err := e.queryEventsForDate(ctx, date)
	if err != nil {
		e.log.Error().Err(err).Str("date", date).Msg("Failed to query events")
		return fmt.Errorf("failed to query events for date %s: %w", date, err)
	}

	if len(events) == 0 {
		e.log.Warn().Str("date", date).Msg("No events found for date")
		return nil
	}

	e.log.Info().Int("count", len(events)).Str("date", date).Msg("Retrieved events from ClickHouse")

	// Step 2: Write to local Parquet file
	tempFile := fmt.Sprintf("/tmp/events-%s.parquet", date)
	e.log.Debug().Str("file", tempFile).Msg("Writing events to Parquet file")

	if err := WriteEventsToParquet(events, tempFile); err != nil {
		e.log.Error().Err(err).Str("file", tempFile).Msg("Failed to write Parquet file")
		return fmt.Errorf("failed to write parquet file: %w", err)
	}

	e.log.Info().Str("file", tempFile).Int("events", len(events)).Msg("Parquet file created")

	// Ensure temp file cleanup on error
	defer func() {
		if err := os.Remove(tempFile); err != nil {
			e.log.Warn().Err(err).Str("file", tempFile).Msg("Failed to delete temp file")
		} else {
			e.log.Debug().Str("file", tempFile).Msg("Temp file deleted")
		}
	}()

	// Step 3: Upload to MinIO
	bucketName := "events"
	objectKey := fmt.Sprintf("date=%s/events.parquet", date)

	e.log.Debug().
		Str("bucket", bucketName).
		Str("key", objectKey).
		Msg("Uploading to MinIO")

	if err := e.minioClient.UploadFile(ctx, bucketName, objectKey, tempFile); err != nil {
		e.log.Error().
			Err(err).
			Str("bucket", bucketName).
			Str("key", objectKey).
			Msg("Failed to upload to MinIO")
		return fmt.Errorf("failed to upload to MinIO: %w", err)
	}

	e.log.Info().
		Str("bucket", bucketName).
		Str("key", objectKey).
		Int("events", len(events)).
		Msg("Successfully exported daily events to MinIO")

	return nil
}

// ExportLastDayEvents exports events from yesterday
func (e *Exporter) ExportLastDayEvents() error {
	yesterday := time.Now().AddDate(0, 0, -1).Format("2006-01-02")
	e.log.Info().Str("date", yesterday).Msg("Exporting last day events")
	return e.ExportDailyEvents(yesterday)
}

// queryEventsForDate queries ClickHouse for events on a specific date
func (e *Exporter) queryEventsForDate(ctx context.Context, date string) ([]EventParquet, error) {
	query := `
		SELECT 
			event_id,
			event_type,
			user_id,
			page_url,
			timestamp
		FROM events
		WHERE toDate(timestamp) = ?
		ORDER BY timestamp
	`

	rows, err := e.db.QueryContext(ctx, query, date)
	if err != nil {
		return nil, fmt.Errorf("query failed: %w", err)
	}
	defer rows.Close()

	events := make([]EventParquet, 0)

	for rows.Next() {
		var event EventParquet
		var timestamp time.Time

		if err := rows.Scan(
			&event.EventID,
			&event.EventType,
			&event.UserID,
			&event.PageURL,
			&timestamp,
		); err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}

		// Convert timestamp to RFC3339 string
		event.Timestamp = timestamp.Format(time.RFC3339)

		events = append(events, event)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating rows: %w", err)
	}

	return events, nil
}

// ExportDateRange exports events for a range of dates
func (e *Exporter) ExportDateRange(startDate, endDate string) error {
	start, err := time.Parse("2006-01-02", startDate)
	if err != nil {
		return fmt.Errorf("invalid start date: %w", err)
	}

	end, err := time.Parse("2006-01-02", endDate)
	if err != nil {
		return fmt.Errorf("invalid end date: %w", err)
	}

	e.log.Info().
		Str("start", startDate).
		Str("end", endDate).
		Msg("Starting date range export")

	successCount := 0
	errorCount := 0

	for d := start; !d.After(end); d = d.AddDate(0, 0, 1) {
		dateStr := d.Format("2006-01-02")

		if err := e.ExportDailyEvents(dateStr); err != nil {
			e.log.Error().Err(err).Str("date", dateStr).Msg("Failed to export date")
			errorCount++
			continue
		}

		successCount++
	}

	e.log.Info().
		Int("success", successCount).
		Int("errors", errorCount).
		Msg("Date range export completed")

	if errorCount > 0 {
		return fmt.Errorf("export completed with %d errors", errorCount)
	}

	return nil
}
