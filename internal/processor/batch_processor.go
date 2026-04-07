package processor

import (
	"context"
	"fmt"
	"time"

	"github.com/rs/zerolog"

	"devops-fall2025-23b031152/internal/repository"
	"devops-fall2025-23b031152/pkg/metrics"
	"devops-fall2025-23b031152/pkg/workerpool"
)

// BatchProcessorConfig holds configuration for batch processing
type BatchProcessorConfig struct {
	WorkerCount   int
	EnableMetrics bool
}

// BatchProcessor handles daily historical aggregations
type BatchProcessor struct {
	config      BatchProcessorConfig
	metricsRepo *repository.MetricsRepository
	workerPool  *workerpool.WorkerPool
	log         zerolog.Logger
}

// NewBatchProcessor creates a new batch processor
func NewBatchProcessor(
	config BatchProcessorConfig,
	metricsRepo *repository.MetricsRepository,
	log zerolog.Logger,
) *BatchProcessor {
	if config.WorkerCount <= 0 {
		config.WorkerCount = 5
	}

	return &BatchProcessor{
		config:      config,
		metricsRepo: metricsRepo,
		workerPool:  workerpool.NewWorkerPool(config.WorkerCount, 100),
		log:         log,
	}
}

// Start starts the batch processor
func (bp *BatchProcessor) Start() {
	bp.workerPool.Start()
	bp.log.Info().
		Int("workers", bp.config.WorkerCount).
		Msg("Batch processor started")
}

// ProcessDailyAggregation processes daily aggregation for a specific date
func (bp *BatchProcessor) ProcessDailyAggregation(ctx context.Context, date time.Time, metricName string) error {
	start := time.Now()

	bp.log.Info().
		Time("date", date).
		Str("metric_name", metricName).
		Msg("Starting daily aggregation")

	// Query aggregated metrics from ClickHouse
	aggregatedMetrics, err := bp.metricsRepo.AggregateDaily(ctx, date, metricName)
	if err != nil {
		bp.log.Error().Err(err).
			Time("date", date).
			Msg("Failed to aggregate daily metrics")
		return fmt.Errorf("failed to aggregate daily metrics: %w", err)
	}

	if len(aggregatedMetrics) == 0 {
		bp.log.Info().
			Time("date", date).
			Msg("No metrics found for aggregation")
		return nil
	}

	// Store aggregated results with a different metric name
	summaryMetricName := fmt.Sprintf("%s_daily_summary", metricName)
	summaryMetrics := make([]*repository.Metric, 0, len(aggregatedMetrics))

	for _, metric := range aggregatedMetrics {
		summaryMetrics = append(summaryMetrics, &repository.Metric{
			Timestamp:  metric.Timestamp,
			MetricName: summaryMetricName,
			Dimension:  metric.Dimension,
			Value:      metric.Value,
		})
	}

	// Store in ClickHouse
	if err := bp.metricsRepo.BatchInsertMetrics(ctx, summaryMetrics); err != nil {
		bp.log.Error().Err(err).
			Time("date", date).
			Msg("Failed to store daily aggregation")
		return fmt.Errorf("failed to store daily aggregation: %w", err)
	}

	duration := time.Since(start)
	bp.log.Info().
		Time("date", date).
		Int("count", len(summaryMetrics)).
		Dur("duration", duration).
		Msg("Daily aggregation completed")

	if bp.config.EnableMetrics {
		metrics.ProcessingLatencySeconds.Observe(duration.Seconds())
	}

	return nil
}

// Stop stops the batch processor
func (bp *BatchProcessor) Stop() {
	bp.log.Info().Msg("Stopping batch processor...")
	bp.workerPool.Stop()
	bp.log.Info().Msg("Batch processor stopped")
}
