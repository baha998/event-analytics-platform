package examples

import (
	"context"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/rs/zerolog"

	"devops-fall2025-23b031152/internal/processor"
	"devops-fall2025-23b031152/internal/repository"
	"devops-fall2025-23b031152/pkg/aggregation"
	"devops-fall2025-23b031152/pkg/kafka"
)

// ExampleStreamProcessing demonstrates real-time click event processing
func ExampleStreamProcessing(clickHouseConn clickhouse.Conn, logger zerolog.Logger) {
	// Create metrics repository
	metricsRepo := repository.NewMetricsRepository(clickHouseConn, logger)

	// Configure stream processor for click events
	streamConfig := processor.StreamProcessorConfig{
		WindowDuration:  1 * time.Minute,
		FlushInterval:   1 * time.Minute,
		WorkerCount:     5,
		EnableMetrics:   true,
		MetricName:      "click_count",
		AggregationType: aggregation.AggregationCount,
		DimensionExtractor: func(event *kafka.Event) string {
			// Group by page_url
			return event.PageURL
		},
	}

	// Create and start stream processor
	streamProc := processor.NewStreamProcessor(streamConfig, metricsRepo, logger)
	streamProc.Start()
	defer streamProc.Stop()

	// Simulate processing events
	ctx := context.Background()
	for i := 0; i < 100; i++ {
		event := &kafka.Event{
			EventID:   fmt.Sprintf("event-%d", i),
			EventType: "click",
			UserID:    fmt.Sprintf("user-%d", i%10),
			PageURL:   fmt.Sprintf("/page-%d", i%5),
			Timestamp: time.Now().Format(time.RFC3339),
		}

		if err := streamProc.ProcessEvent(ctx, event); err != nil {
			logger.Error().Err(err).Msg("Failed to process event")
		}

		// Small delay to simulate real-time processing
		time.Sleep(10 * time.Millisecond)
	}

	logger.Info().Msg("Stream processing example completed")
}

// ExampleBatchProcessing demonstrates daily transaction summary processing
func ExampleBatchProcessing(clickHouseConn clickhouse.Conn, logger zerolog.Logger) {
	// Create metrics repository
	metricsRepo := repository.NewMetricsRepository(clickHouseConn, logger)

	// Configure batch processor
	batchConfig := processor.BatchProcessorConfig{
		WorkerCount:   5,
		EnableMetrics: true,
	}

	// Create batch processor
	batchProc := processor.NewBatchProcessor(batchConfig, metricsRepo, logger)
	batchProc.Start()
	defer batchProc.Stop()

	// Process yesterday's transaction summaries
	ctx := context.Background()
	yesterday := time.Now().AddDate(0, 0, -1)

	if err := batchProc.ProcessDailyAggregation(ctx, yesterday, "transaction_sum"); err != nil {
		logger.Error().Err(err).Msg("Failed to process daily aggregation")
		return
	}

	logger.Info().Msg("Batch processing example completed")
}

// ExampleCronScheduledBatchProcessing demonstrates cron-scheduled batch processing
func ExampleCronScheduledBatchProcessing(clickHouseConn clickhouse.Conn, logger zerolog.Logger) {
	// Create metrics repository
	metricsRepo := repository.NewMetricsRepository(clickHouseConn, logger)

	// Configure batch processor
	batchConfig := processor.BatchProcessorConfig{
		WorkerCount:   5,
		EnableMetrics: true,
	}

	batchProc := processor.NewBatchProcessor(batchConfig, metricsRepo, logger)
	batchProc.Start()
	defer batchProc.Stop()

	// Create cron scheduler
	scheduler := processor.NewCronScheduler(batchProc, logger)

	// Schedule daily aggregation at 1 AM every day
	// Cron format: "0 1 * * *" (minute hour day month weekday)
	if err := scheduler.ScheduleDailyAggregation("0 1 * * *", "transaction_sum"); err != nil {
		logger.Error().Err(err).Msg("Failed to schedule daily aggregation")
		return
	}

	// Schedule another job for click metrics at 2 AM
	if err := scheduler.ScheduleDailyAggregation("0 2 * * *", "click_count"); err != nil {
		logger.Error().Err(err).Msg("Failed to schedule click aggregation")
		return
	}

	// Start scheduler
	scheduler.Start()
	defer scheduler.Stop()

	logger.Info().Msg("Cron scheduled batch processing example started")
	
	// Keep running (in real application, this would be part of main loop)
	time.Sleep(5 * time.Second)
}

// ExampleMultipleAggregations demonstrates different aggregation types
func ExampleMultipleAggregations(clickHouseConn clickhouse.Conn, logger zerolog.Logger) {
	metricsRepo := repository.NewMetricsRepository(clickHouseConn, logger)

	// COUNT aggregation by page_url
	countConfig := processor.StreamProcessorConfig{
		WindowDuration:  1 * time.Minute,
		FlushInterval:   1 * time.Minute,
		WorkerCount:     5,
		EnableMetrics:   true,
		MetricName:      "click_count_by_page",
		AggregationType: aggregation.AggregationCount,
		DimensionExtractor: func(event *kafka.Event) string {
			return event.PageURL
		},
	}
	countProcessor := processor.NewStreamProcessor(countConfig, metricsRepo, logger)
	countProcessor.Start()
	defer countProcessor.Stop()

	// SUM aggregation by user_id (for transaction amounts)
	sumConfig := processor.StreamProcessorConfig{
		WindowDuration:  1 * time.Minute,
		FlushInterval:   1 * time.Minute,
		WorkerCount:     5,
		EnableMetrics:   true,
		MetricName:      "transaction_sum_by_user",
		AggregationType: aggregation.AggregationSum,
		DimensionExtractor: func(event *kafka.Event) string {
			return event.UserID
		},
	}
	sumProcessor := processor.NewStreamProcessor(sumConfig, metricsRepo, logger)
	sumProcessor.Start()
	defer sumProcessor.Stop()

	// AVG aggregation by page_url
	avgConfig := processor.StreamProcessorConfig{
		WindowDuration:  1 * time.Minute,
		FlushInterval:   1 * time.Minute,
		WorkerCount:     5,
		EnableMetrics:   true,
		MetricName:      "avg_time_by_page",
		AggregationType: aggregation.AggregationAvg,
		DimensionExtractor: func(event *kafka.Event) string {
			return event.PageURL
		},
	}
	avgProcessor := processor.NewStreamProcessor(avgConfig, metricsRepo, logger)
	avgProcessor.Start()
	defer avgProcessor.Stop()

	logger.Info().Msg("Multiple aggregations example started")
}

