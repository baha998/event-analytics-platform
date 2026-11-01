package repository

import (
	"context"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/rs/zerolog"
)

// ClickHouseRepository handles ClickHouse operations
type ClickHouseRepository struct {
	conn clickhouse.Conn
	log  zerolog.Logger
}

// NewClickHouseRepository creates a new ClickHouse repository
func NewClickHouseRepository(conn clickhouse.Conn, log zerolog.Logger) *ClickHouseRepository {
	return &ClickHouseRepository{
		conn: conn,
		log:  log,
	}
}

// InsertEvent inserts a single event into ClickHouse
func (r *ClickHouseRepository) InsertEvent(ctx context.Context, event *Event) error {
	query := `
		INSERT INTO events (event_id, timestamp, page_url, user_id, event_type)
		VALUES (?, ?, ?, ?, ?)
	`

	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	if err := r.conn.Exec(ctx, query, event.EventID, event.Timestamp, event.PageURL, event.UserID, event.EventType); err != nil {
		r.log.Error().Err(err).Str("event_id", event.EventID).Msg("Failed to insert event")
		return fmt.Errorf("failed to insert event: %w", err)
	}

	r.log.Debug().Str("event_id", event.EventID).Msg("Event inserted successfully")
	return nil
}

// BatchInsertEvents inserts multiple events in a batch
func (r *ClickHouseRepository) BatchInsertEvents(ctx context.Context, events []*Event) error {
	if len(events) == 0 {
		return nil
	}

	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	batch, err := r.conn.PrepareBatch(ctx, "INSERT INTO events (event_id, timestamp, page_url, user_id, event_type)")
	if err != nil {
		r.log.Error().Err(err).Msg("Failed to prepare batch")
		return fmt.Errorf("failed to prepare batch: %w", err)
	}

	for _, event := range events {
		if err := batch.Append(event.EventID, event.Timestamp, event.PageURL, event.UserID, event.EventType); err != nil {
			r.log.Error().Err(err).Str("event_id", event.EventID).Msg("Failed to append event to batch")
			return fmt.Errorf("failed to append event to batch: %w", err)
		}
	}

	if err := batch.Send(); err != nil {
		r.log.Error().Err(err).Int("count", len(events)).Msg("Failed to send batch")
		return fmt.Errorf("failed to send batch: %w", err)
	}

	r.log.Info().Int("count", len(events)).Msg("Batch inserted successfully")
	return nil
}

// QueryEvents queries events based on the request
func (r *ClickHouseRepository) QueryEvents(ctx context.Context, req *QueryRequest) ([]*QueryResult, error) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	// Build query dynamically
	query := `
		SELECT 
			page_url,
			uniqExact(event_id) as click_count
		FROM events
		WHERE event_type = 'click'
	`
	args := []interface{}{}

	if req.StartTime > 0 {
		query += " AND timestamp >= ?"
		args = append(args, time.Unix(req.StartTime, 0))
	}

	if req.EndTime > 0 {
		query += " AND timestamp <= ?"
		args = append(args, time.Unix(req.EndTime, 0))
	} else {
		query += " AND timestamp <= ?"
		args = append(args, time.Now())
	}

	if req.PageURL != "" {
		query += " AND page_url = ?"
		args = append(args, req.PageURL)
	}

	if req.UserID != "" {
		query += " AND user_id = ?"
		args = append(args, req.UserID)
	}

	query += " GROUP BY page_url ORDER BY click_count DESC"

	limit := req.Limit
	if limit <= 0 {
		limit = 10
	}
	query += " LIMIT ?"
	args = append(args, limit)

	rows, err := r.conn.Query(ctx, query, args...)
	if err != nil {
		r.log.Error().Err(err).Msg("Failed to query events")
		return nil, fmt.Errorf("failed to query events: %w", err)
	}
	defer rows.Close()

	results := make([]*QueryResult, 0)
	for rows.Next() {
		var result QueryResult
		if err := rows.Scan(&result.PageURL, &result.ClickCount); err != nil {
			r.log.Error().Err(err).Msg("Failed to scan row")
			return nil, fmt.Errorf("scan event: %w", err)
		}
		results = append(results, &result)
	}

	if err := rows.Err(); err != nil {
		r.log.Error().Err(err).Msg("Error iterating rows")
		return nil, fmt.Errorf("error iterating rows: %w", err)
	}

	r.log.Debug().Int("count", len(results)).Msg("Query executed successfully")
	return results, nil
}

// Ping checks the ClickHouse connection
func (r *ClickHouseRepository) Ping(ctx context.Context) error {
	return r.conn.Ping(ctx)
}

func (r *ClickHouseRepository) CountEventsOnDate(ctx context.Context, date string) (uint64, error) {
	var count uint64
	err := r.conn.QueryRow(ctx, "SELECT uniqExact(event_id) FROM events WHERE toDate(timestamp) = toDate(?)", date).Scan(&count)
	return count, err
}
func (r *ClickHouseRepository) DeleteEventsBefore(ctx context.Context, cutoff time.Time) (uint64, error) {
	var count uint64
	if err := r.conn.QueryRow(ctx, "SELECT count() FROM events WHERE timestamp < ?", cutoff).Scan(&count); err != nil {
		return 0, err
	}
	err := r.conn.Exec(ctx, "ALTER TABLE events DELETE WHERE timestamp < ? SETTINGS mutations_sync = 1", cutoff)
	return count, err
}
func (r *ClickHouseRepository) Optimize(ctx context.Context) error {
	return r.conn.Exec(ctx, "OPTIMIZE TABLE events")
}
