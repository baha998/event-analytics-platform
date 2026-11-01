package service

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/rs/zerolog"

	"devops-fall2025-23b031152/internal/repository"
	"devops-fall2025-23b031152/pkg/kafka"
	"devops-fall2025-23b031152/pkg/metrics"
)

// EventRepository defines the data store operations
type EventRepository interface {
	InsertEvent(ctx context.Context, event *repository.Event) error
	QueryEvents(ctx context.Context, req *repository.QueryRequest) ([]*repository.QueryResult, error)
}

// EventProducer defines message broker operations
type EventProducer interface {
	SendEvent(event *kafka.Event) error
}

// EventService handles event business logic
type EventService struct {
	repo     EventRepository
	producer EventProducer
	log      zerolog.Logger
}

// NewEventService creates a new event service
func NewEventService(repo EventRepository, producer EventProducer, log zerolog.Logger) *EventService {
	return &EventService{
		repo:     repo,
		producer: producer,
		log:      log,
	}
}

// CreateSource creates a source and generates events
func (s *EventService) ValidateSource(req *repository.SourceRequest) error {
	if req == nil || req.Topic == "" {
		return fmt.Errorf("topic is required")
	}
	if req.Count < 1 || req.Count > 100 {
		return fmt.Errorf("count must be between 1 and 100")
	}
	if req.Interval < 0 || req.Interval > 1000 {
		return fmt.Errorf("interval must be between 0 and 1000 ms")
	}
	if p, ok := s.producer.(interface{ Topic() string }); ok && req.Topic != p.Topic() {
		return fmt.Errorf("topic must be %s", p.Topic())
	}
	if (req.Count-1)*req.Interval > 10000 {
		return fmt.Errorf("generation duration must not exceed 10 seconds")
	}
	return nil
}
func (s *EventService) CreateSource(ctx context.Context, req *repository.SourceRequest) error {
	if err := s.ValidateSource(req); err != nil {
		return err
	}
	return s.generateAndSendEvents(ctx, req.Topic, req.Count, req.Interval)
}

// generateAndSendEvents returns broker errors to the caller.
func (s *EventService) generateAndSendEvents(ctx context.Context, topic string, count, intervalMs int) error {
	s.log.Info().
		Str("topic", topic).
		Int("count", count).
		Msg("Starting event generation")

	pages := []string{"/home", "/product", "/about", "/contact", "/pricing", "/blog", "/features"}
	userIDs := []string{"user1001", "user1002", "user1003", "user1004", "user1005", "user1006", "user1007", "user1008", "user1009", "user1010"}

	successCount := 0

	for i := 0; i < count; i++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		event := &kafka.Event{
			EventID:   uuid.New().String(),
			EventType: "click",
			UserID:    userIDs[i%len(userIDs)],
			PageURL:   pages[i%len(pages)],
			Timestamp: time.Now().Format(time.RFC3339),
		}

		if err := s.producer.SendEvent(event); err != nil {
			s.log.Error().Err(err).Str("event_id", event.EventID).Msg("Failed to send event")
			metrics.KafkaMessageProcessingErrors.WithLabelValues(topic).Inc()
			return err
		}

		successCount++

		if i < count-1 && intervalMs > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(time.Duration(intervalMs) * time.Millisecond):
			}
		}
	}

	s.log.Info().
		Str("topic", topic).
		Int("success", successCount).
		Msg("Event generation completed")
	return nil
}

// ProcessEvent processes an event from Kafka
func (s *EventService) ProcessEvent(ctx context.Context, eventData []byte) error {
	start := time.Now()

	// Parse event
	event, err := kafka.UnmarshalEvent(eventData)
	if err != nil {
		s.log.Error().Err(err).Msg("Invalid event data")
		metrics.KafkaMessageProcessingErrors.WithLabelValues("events").Inc()
		return fmt.Errorf("invalid event: %w", err)
	}

	timestamp, err := time.Parse(time.RFC3339, event.Timestamp)
	if err != nil {
		return err
	}

	// Convert to repository event for ClickHouse storage
	repoEvent := &repository.Event{
		EventID:   event.EventID,
		Timestamp: timestamp,
		PageURL:   event.PageURL,
		UserID:    event.UserID,
		EventType: event.EventType,
		Source:    "kafka",
	}

	// Insert into ClickHouse
	if err := s.repo.InsertEvent(ctx, repoEvent); err != nil {
		s.log.Error().Err(err).Str("event_id", event.EventID).Msg("Failed to insert event")
		metrics.KafkaMessageProcessingErrors.WithLabelValues("events").Inc()
		return fmt.Errorf("failed to insert event: %w", err)
	}

	metrics.EventsInserted.Inc()
	metrics.ClickHouseQueriesTotal.WithLabelValues("insert").Inc()
	metrics.ClickHouseQueryDuration.WithLabelValues("insert").Observe(time.Since(start).Seconds())

	s.log.Debug().
		Str("event_id", event.EventID).
		Str("page_url", event.PageURL).
		Msg("Event processed successfully")

	return nil
}

// QueryEvents queries events based on request
func (s *EventService) QueryEvents(ctx context.Context, req *repository.QueryRequest) ([]*repository.QueryResult, error) {
	start := time.Now()

	results, err := s.repo.QueryEvents(ctx, req)
	if err != nil {
		s.log.Error().Err(err).Msg("Failed to query events")
		metrics.ClickHouseQueriesTotal.WithLabelValues("query").Inc()
		return nil, fmt.Errorf("failed to query events: %w", err)
	}

	metrics.ClickHouseQueriesTotal.WithLabelValues("query").Inc()
	metrics.ClickHouseQueryDuration.WithLabelValues("query").Observe(time.Since(start).Seconds())

	s.log.Debug().Int("count", len(results)).Msg("Query executed successfully")

	return results, nil
}
