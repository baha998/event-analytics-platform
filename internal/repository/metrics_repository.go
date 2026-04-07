package repository

import (
	"context"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/rs/zerolog"
)

// Metric represents a stored metric
type Metric struct {
	Timestamp  time.Time
	MetricName string
	Dimension  string
	Value      float64
}

// MetricsRepository handles ClickHouse operations for metrics
type MetricsRepository struct {
	conn clickhouse.Conn
	log  zerolog.Logger
}

// NewMetricsRepository creates a new metrics repository
func NewMetricsRepository(conn clickhouse.Conn, log zerolog.Logger) *MetricsRepository {
	return &MetricsRepository{
		conn: conn,
		log:  log,
	}
}

// InsertMetric inserts a single metric
func (r *MetricsRepository) InsertMetric(ctx context.Context, metric *Metric) error {
	query := `
		INSERT INTO metrics (timestamp, metric_name, dimension, value)
		VALUES (?, ?, ?, ?)
	`

	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	if err := r.conn.Exec(ctx, query, metric.Timestamp, metric.MetricName, metric.Dimension, metric.Value); err != nil {
		r.log.Error().Err(err).
			Str("metric_name", metric.MetricName).
			Str("dimension", metric.Dimension).
			Msg("Failed to insert metric")
		return fmt.Errorf("failed to insert metric: %w", err)
	}

	return nil
}

// BatchInsertMetrics inserts multiple metrics in a batch
func (r *MetricsRepository) BatchInsertMetrics(ctx context.Context, metrics []*Metric) error {
	if len(metrics) == 0 {
		return nil
	}

	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	batch, err := r.conn.PrepareBatch(ctx, "INSERT INTO metrics (timestamp, metric_name, dimension, value)")
	if err != nil {
		r.log.Error().Err(err).Msg("Failed to prepare batch")
		return fmt.Errorf("failed to prepare batch: %w", err)
	}

	for _, metric := range metrics {
		if err := batch.Append(metric.Timestamp, metric.MetricName, metric.Dimension, metric.Value); err != nil {
			r.log.Error().Err(err).
				Str("metric_name", metric.MetricName).
				Msg("Failed to append metric to batch")
			return fmt.Errorf("failed to append metric: %w", err)
		}
	}

	if err := batch.Send(); err != nil {
		r.log.Error().Err(err).Int("count", len(metrics)).Msg("Failed to send batch")
		return fmt.Errorf("failed to send batch: %w", err)
	}

	r.log.Debug().Int("count", len(metrics)).Msg("Batch inserted successfully")
	return nil
}

// QueryMetrics queries metrics based on criteria
func (r *MetricsRepository) QueryMetrics(ctx context.Context, req *MetricQueryRequest) ([]*Metric, error) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	query := `
		SELECT timestamp, metric_name, dimension, value
		FROM metrics
		WHERE 1=1
	`
	args := []interface{}{}

	if !req.StartTime.IsZero() {
		query += " AND timestamp >= ?"
		args = append(args, req.StartTime)
	}

	if !req.EndTime.IsZero() {
		query += " AND timestamp <= ?"
		args = append(args, req.EndTime)
	} else {
		query += " AND timestamp <= ?"
		args = append(args, time.Now())
	}

	if req.MetricName != "" {
		query += " AND metric_name = ?"
		args = append(args, req.MetricName)
	}

	if req.Dimension != "" {
		query += " AND dimension = ?"
		args = append(args, req.Dimension)
	}

	query += " ORDER BY timestamp DESC"

	if req.Limit > 0 {
		query += " LIMIT ?"
		args = append(args, req.Limit)
	}

	rows, err := r.conn.Query(ctx, query, args...)
	if err != nil {
		r.log.Error().Err(err).Msg("Failed to query metrics")
		return nil, fmt.Errorf("failed to query metrics: %w", err)
	}
	defer rows.Close()

	var results []*Metric
	for rows.Next() {
		var metric Metric
		if err := rows.Scan(&metric.Timestamp, &metric.MetricName, &metric.Dimension, &metric.Value); err != nil {
			r.log.Error().Err(err).Msg("Failed to scan row")
			continue
		}
		results = append(results, &metric)
	}

	if err := rows.Err(); err != nil {
		r.log.Error().Err(err).Msg("Error iterating rows")
		return nil, fmt.Errorf("error iterating rows: %w", err)
	}

	return results, nil
}

// MetricQueryRequest represents a query request for metrics
type MetricQueryRequest struct {
	StartTime  time.Time
	EndTime    time.Time
	MetricName string
	Dimension  string
	Limit      int
}

// AggregateDaily aggregates metrics for a specific day
func (r *MetricsRepository) AggregateDaily(ctx context.Context, date time.Time, metricName string) ([]*Metric, error) {
	startOfDay := time.Date(date.Year(), date.Month(), date.Day(), 0, 0, 0, 0, date.Location())
	endOfDay := startOfDay.Add(24 * time.Hour)

	query := `
		SELECT 
			toStartOfDay(timestamp) as day,
			metric_name,
			dimension,
			SUM(value) as total_value,
			COUNT(*) as count
		FROM metrics
		WHERE timestamp >= ? 
			AND timestamp < ?
			AND metric_name = ?
		GROUP BY day, metric_name, dimension
		ORDER BY day DESC, dimension
	`

	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	rows, err := r.conn.Query(ctx, query, startOfDay, endOfDay, metricName)
	if err != nil {
		r.log.Error().Err(err).Msg("Failed to aggregate daily metrics")
		return nil, fmt.Errorf("failed to aggregate daily metrics: %w", err)
	}
	defer rows.Close()

	var results []*Metric
	for rows.Next() {
		var metric Metric
		var count int64
		if err := rows.Scan(&metric.Timestamp, &metric.MetricName, &metric.Dimension, &metric.Value, &count); err != nil {
			r.log.Error().Err(err).Msg("Failed to scan row")
			continue
		}
		results = append(results, &metric)
	}

	if err := rows.Err(); err != nil {
		r.log.Error().Err(err).Msg("Error iterating rows")
		return nil, fmt.Errorf("error iterating rows: %w", err)
	}

	return results, nil
}
