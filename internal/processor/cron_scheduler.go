package processor

import (
	"context"
	"time"

	"github.com/robfig/cron/v3"
	"github.com/rs/zerolog"
)

// CronScheduler manages cron jobs for batch processing
type CronScheduler struct {
	cron      *cron.Cron
	batchProc *BatchProcessor
	log       zerolog.Logger
	ctx       context.Context
	cancel    context.CancelFunc
}

// NewCronScheduler creates a new cron scheduler
func NewCronScheduler(batchProc *BatchProcessor, log zerolog.Logger) *CronScheduler {
	ctx, cancel := context.WithCancel(context.Background())

	// Create cron with seconds support
	c := cron.New(cron.WithSeconds())

	return &CronScheduler{
		cron:      c,
		batchProc: batchProc,
		log:       log,
		ctx:       ctx,
		cancel:    cancel,
	}
}

// ScheduleDailyAggregation schedules a daily aggregation job
func (cs *CronScheduler) ScheduleDailyAggregation(schedule string, metricName string) error {
	_, err := cs.cron.AddFunc(schedule, func() {
		// Process yesterday's data
		yesterday := time.Now().AddDate(0, 0, -1)

		cs.log.Info().
			Time("date", yesterday).
			Str("metric_name", metricName).
			Msg("Starting scheduled daily aggregation")

		if err := cs.batchProc.ProcessDailyAggregation(cs.ctx, yesterday, metricName); err != nil {
			cs.log.Error().Err(err).
				Time("date", yesterday).
				Str("metric_name", metricName).
				Msg("Failed to process daily aggregation")
		}
	})

	if err != nil {
		return err
	}

	cs.log.Info().
		Str("schedule", schedule).
		Str("metric_name", metricName).
		Msg("Scheduled daily aggregation job")

	return nil
}

// Start starts the cron scheduler
func (cs *CronScheduler) Start() {
	cs.cron.Start()
	cs.log.Info().Msg("Cron scheduler started")
}

// Stop stops the cron scheduler
func (cs *CronScheduler) Stop() {
	cs.log.Info().Msg("Stopping cron scheduler...")
	cs.cancel()
	ctx := cs.cron.Stop()
	<-ctx.Done()
	cs.log.Info().Msg("Cron scheduler stopped")
}
