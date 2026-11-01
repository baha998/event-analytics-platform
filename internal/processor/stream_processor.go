package processor

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/rs/zerolog"

	"devops-fall2025-23b031152/internal/repository"
	"devops-fall2025-23b031152/pkg/aggregation"
	"devops-fall2025-23b031152/pkg/kafka"
	"devops-fall2025-23b031152/pkg/metrics"
	"devops-fall2025-23b031152/pkg/workerpool"
)

// StreamProcessorConfig holds configuration for stream processing
type StreamProcessorConfig struct {
	WindowDuration     time.Duration
	FlushInterval      time.Duration
	WorkerCount        int
	EnableMetrics      bool
	MetricName         string
	AggregationType    aggregation.AggregationType
	DimensionExtractor func(*kafka.Event) string // Extracts dimension from event
}

// StreamProcessor handles real-time event stream processing
type StreamProcessor struct {
	config      StreamProcessorConfig
	aggregator  *aggregation.Aggregator
	metricsRepo *repository.MetricsRepository
	workerPool  *workerpool.WorkerPool
	log         zerolog.Logger
	mu          sync.RWMutex
	flushTicker *time.Ticker
	ctx         context.Context
	cancel      context.CancelFunc
	wg          sync.WaitGroup
}

// NewStreamProcessor creates a new stream processor
func NewStreamProcessor(
	config StreamProcessorConfig,
	metricsRepo *repository.MetricsRepository,
	log zerolog.Logger,
) *StreamProcessor {
	if config.WindowDuration <= 0 {
		config.WindowDuration = 1 * time.Minute
	}
	if config.FlushInterval <= 0 {
		config.FlushInterval = 1 * time.Minute
	}
	if config.WorkerCount <= 0 {
		config.WorkerCount = 5
	}

	ctx, cancel := context.WithCancel(context.Background())

	processor := &StreamProcessor{
		config:      config,
		aggregator:  aggregation.NewAggregator(),
		metricsRepo: metricsRepo,
		workerPool:  workerpool.NewWorkerPool(config.WorkerCount, 1000),
		log:         log,
		ctx:         ctx,
		cancel:      cancel,
	}

	return processor
}

// Start starts the stream processor
func (sp *StreamProcessor) Start() {
	sp.workerPool.Start()
	sp.flushTicker = time.NewTicker(sp.config.FlushInterval)

	// Start flush goroutine
	sp.wg.Add(1)
	go sp.flushLoop()

	sp.log.Info().
		Dur("window_duration", sp.config.WindowDuration).
		Dur("flush_interval", sp.config.FlushInterval).
		Int("workers", sp.config.WorkerCount).
		Msg("Stream processor started")
}

// ProcessEvent processes a single event
func (sp *StreamProcessor) ProcessEvent(ctx context.Context, event *kafka.Event) error {
	if event.EventType != "click" && sp.config.MetricName == "click_count" {
		return nil
	}
	start := time.Now()

	// Parse timestamp
	timestamp, err := time.Parse(time.RFC3339, event.Timestamp)
	if err != nil {
		sp.log.Error().Err(err).Str("timestamp", event.Timestamp).Msg("Failed to parse timestamp")
		if sp.config.EnableMetrics {
			metrics.KafkaErrorsTotal.WithLabelValues("events", "parse_error").Inc()
			metrics.AggregationErrorsTotal.Inc()
		}
		return fmt.Errorf("failed to parse timestamp: %w", err)
	}

	// Extract dimension
	dimension := sp.config.DimensionExtractor(event)
	if dimension == "" {
		dimension = "unknown"
	}

	// Extract value (for SUM/AVG aggregations)
	value := 1.0 // Default for COUNT
	if event.EventType == "transaction" {
		// For transactions, you might extract amount from event
		// For now, use 1.0
		value = 1.0
	}

	// Add to aggregator
	sp.aggregator.AddEvent(timestamp, dimension, value, sp.config.WindowDuration)

	// Record metrics
	if sp.config.EnableMetrics {
		metrics.EventsProcessedTotal.Inc()
		metrics.EventsProcessedByType.WithLabelValues(event.EventType).Inc()
		metrics.ProcessingLatencySeconds.Observe(time.Since(start).Seconds())
	}

	return nil
}

// flushLoop periodically flushes expired windows
func (sp *StreamProcessor) flushLoop() {
	defer sp.wg.Done()

	for {
		select {
		case <-sp.ctx.Done():
			// Flush all remaining windows on shutdown
			sp.flushAllWindows()
			return
		case <-sp.flushTicker.C:
			sp.flushExpiredWindows()
		}
	}
}

// flushExpiredWindows flushes all expired windows
func (sp *StreamProcessor) flushExpiredWindows() {
	expired := sp.aggregator.GetExpiredWindows(sp.config.WindowDuration)

	for _, windowKey := range expired {
		sp.flushWindow(windowKey)
	}
}

// flushWindow flushes a specific window
func (sp *StreamProcessor) flushWindow(windowKey string) {
	results := sp.aggregator.FlushWindow(windowKey, sp.config.AggregationType, sp.config.MetricName)

	if len(results) == 0 {
		return
	}

	// Convert to metrics
	metricsToStore := make([]*repository.Metric, 0, len(results))
	for _, result := range results {
		metricsToStore = append(metricsToStore, &repository.Metric{
			Timestamp:  result.WindowStart,
			MetricName: result.MetricName,
			Dimension:  result.Dimension,
			Value:      result.Value,
		})
	}

	// Submit to worker pool for async storage
	job := &StoreMetricsJob{
		metrics:     metricsToStore,
		metricsRepo: sp.metricsRepo,
		log:         sp.log,
	}

	if err := sp.workerPool.Submit(job); err != nil {
		sp.log.Error().Err(err).Str("window_key", windowKey).Msg("Failed to submit flush job")
	}
}

// flushAllWindows flushes all remaining windows
func (sp *StreamProcessor) flushAllWindows() {
	windowKeys := sp.aggregator.GetWindowKeys()

	for _, windowKey := range windowKeys {
		sp.flushWindow(windowKey)
	}
}

// Stop stops the stream processor
func (sp *StreamProcessor) Stop() {
	sp.log.Info().Msg("Stopping stream processor...")

	sp.cancel()
	if sp.flushTicker != nil {
		sp.flushTicker.Stop()
	}

	sp.wg.Wait()
	sp.workerPool.Stop()

	sp.log.Info().Msg("Stream processor stopped")
}

// StoreMetricsJob is a job for storing metrics
type StoreMetricsJob struct {
	metrics     []*repository.Metric
	metricsRepo *repository.MetricsRepository
	log         zerolog.Logger
}

func (j *StoreMetricsJob) Execute(ctx context.Context) error {
	err := j.metricsRepo.BatchInsertMetrics(ctx, j.metrics)
	if err != nil {
		j.log.Error().Err(err).Msg("Failed to store window metrics")
	}
	return err
}
