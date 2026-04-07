package service

import (
	"context"
	"time"

	"github.com/rs/zerolog"

	"devops-fall2025-23b031152/internal/processor"
	"devops-fall2025-23b031152/internal/repository"
	"devops-fall2025-23b031152/pkg/aggregation"
	"devops-fall2025-23b031152/pkg/kafka"
)

// ProcessingService integrates stream and batch processors
type ProcessingService struct {
	streamProcessor *processor.StreamProcessor
	batchProcessor  *processor.BatchProcessor
	cronScheduler   *processor.CronScheduler
	log             zerolog.Logger
}

// NewProcessingService creates a new processing service
func NewProcessingService(
	streamProc *processor.StreamProcessor,
	batchProc *processor.BatchProcessor,
	cronSched *processor.CronScheduler,
	log zerolog.Logger,
) *ProcessingService {
	return &ProcessingService{
		streamProcessor: streamProc,
		batchProcessor:  batchProc,
		cronScheduler:   cronSched,
		log:             log,
	}
}

// Start starts all processors
func (ps *ProcessingService) Start() {
	ps.streamProcessor.Start()
	ps.batchProcessor.Start()
	ps.cronScheduler.Start()
	ps.log.Info().Msg("Processing service started")
}

// Stop stops all processors
func (ps *ProcessingService) Stop() {
	ps.log.Info().Msg("Stopping processing service...")
	ps.cronScheduler.Stop()
	ps.batchProcessor.Stop()
	ps.streamProcessor.Stop()
	ps.log.Info().Msg("Processing service stopped")
}

// ProcessKafkaEventBatch processes a batch of Kafka events
func (ps *ProcessingService) ProcessKafkaEventBatch(events []*kafka.Event) error {
	ctx := context.Background()

	for _, event := range events {
		if err := ps.streamProcessor.ProcessEvent(ctx, event); err != nil {
			ps.log.Error().Err(err).Str("event_id", event.EventID).Msg("Failed to process event")
			return err
		}
	}

	return nil
}

// SetupClickStreamProcessor sets up a stream processor for click events
func SetupClickStreamProcessor(
	metricsRepo *repository.MetricsRepository,
	windowDuration time.Duration,
	workerCount int,
	log zerolog.Logger,
) *processor.StreamProcessor {
	config := processor.StreamProcessorConfig{
		WindowDuration:  windowDuration,
		FlushInterval:   windowDuration,
		WorkerCount:     workerCount,
		EnableMetrics:   true,
		MetricName:      "click_count",
		AggregationType: aggregation.AggregationCount,
		DimensionExtractor: func(event *kafka.Event) string {
			// Group by page_url
			return event.PageURL
		},
	}

	return processor.NewStreamProcessor(config, metricsRepo, log)
}

// SetupTransactionStreamProcessor sets up a stream processor for transaction events
func SetupTransactionStreamProcessor(
	metricsRepo *repository.MetricsRepository,
	windowDuration time.Duration,
	workerCount int,
	log zerolog.Logger,
) *processor.StreamProcessor {
	config := processor.StreamProcessorConfig{
		WindowDuration:  windowDuration,
		FlushInterval:   windowDuration,
		WorkerCount:     workerCount,
		EnableMetrics:   true,
		MetricName:      "transaction_sum",
		AggregationType: aggregation.AggregationSum,
		DimensionExtractor: func(event *kafka.Event) string {
			// Group by user_id
			return event.UserID
		},
	}

	return processor.NewStreamProcessor(config, metricsRepo, log)
}
