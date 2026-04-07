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
func (s *EventService) CreateSource(ctx context.Context, req *repository.SourceRequest) error {
	if req.Count == 0 {
		req.Count = 10
	}
	if req.Interval == 0 {
		req.Interval = 100
	}

	// Generate and send events asynchronously
	go s.generateAndSendEvents(req.Topic, req.Count, req.Interval)

	s.log.Info().
		Str("topic", req.Topic).
		Int("count", req.Count).
		Msg("Source creation initiated")

	return nil
}

// generateAndSendEvents generates and sends events to Kafka
func (s *EventService) generateAndSendEvents(topic string, count, intervalMs int) {
	s.log.Info().
		Str("topic", topic).
		Int("count", count).
		Msg("Starting event generation")

	pages := []string{"/home", "/product", "/about", "/contact", "/pricing", "/blog", "/features"}
	userIDs := []string{"user1001", "user1002", "user1003", "user1004", "user1005", "user1006", "user1007", "user1008", "user1009", "user1010"}

	successCount := 0
	errorCount := 0

	for i := 0; i < count; i++ {
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
			errorCount++
			continue
		}

		metrics.KafkaMessagesProduced.WithLabelValues(topic).Inc()
		successCount++

		if i < count-1 && intervalMs > 0 {
			time.Sleep(time.Duration(intervalMs) * time.Millisecond)
		}
	}

	s.log.Info().
		Str("topic", topic).
		Int("success", successCount).
		Int("errors", errorCount).
		Msg("Event generation completed")
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

	// Convert to repository event for ClickHouse storage
	repoEvent := &repository.Event{
		EventID:   event.EventID,
		Timestamp: parseTimestamp(event.Timestamp),
		PageURL:   event.PageURL,
		UserID:    parseUserID(event.UserID),
		Source:    "kafka",
	}

	// Insert into ClickHouse
	if err := s.repo.InsertEvent(ctx, repoEvent); err != nil {
		s.log.Error().Err(err).Str("event_id", event.EventID).Msg("Failed to insert event")
		metrics.KafkaMessageProcessingErrors.WithLabelValues("events").Inc()
		return fmt.Errorf("failed to insert event: %w", err)
	}

	metrics.EventsInserted.Inc()
	metrics.KafkaMessagesConsumed.WithLabelValues("events").Inc()
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

// Helper functions
func parseTimestamp(timestampStr string) int64 {
	t, err := time.Parse(time.RFC3339, timestampStr)
	if err != nil {
		return time.Now().Unix()
	}
	return t.Unix()
}

func parseUserID(userIDStr string) int {
	// Simple conversion - in production, use proper parsing
	// For now, return a hash-based integer
	hash := 0
	for _, c := range userIDStr {
		hash = hash*31 + int(c)
	}
	if hash < 0 {
		hash = -hash
	}
	return hash
}
