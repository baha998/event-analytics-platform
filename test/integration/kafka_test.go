package integration

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"devops-fall2025-23b031152/pkg/kafka"
)

func TestKafka_ProducerConsumer_Integration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping Kafka integration test")
	}

	ctx := context.Background()

	// Start Kafka container
	kafkaContainer, brokers := setupKafkaContainer(t, ctx)
	defer kafkaContainer.Terminate(ctx)

	// Create producer
	producer, err := kafka.NewProducer(kafka.ProducerConfig{
		Brokers:       brokers,
		Topic:         "test-events",
		MaxRetries:    3,
		RetryBackoff:  100 * time.Millisecond,
		EnableMetrics: false,
	}, createTestLogger())
	require.NoError(t, err)
	defer producer.Close()

	// Send test events
	testEvents := []*kafka.Event{
		{
			EventID:   "test-1",
			EventType: "click",
			UserID:    "user-123",
			PageURL:   "/test",
			Timestamp: time.Now().Format(time.RFC3339),
		},
		{
			EventID:   "test-2",
			EventType: "transaction",
			UserID:    "user-456",
			PageURL:   "/checkout",
			Timestamp: time.Now().Format(time.RFC3339),
		},
	}

	// Produce messages
	err = producer.SendEvents(testEvents)
	assert.NoError(t, err)

	// Create consumer
	receivedEvents := make([]*kafka.Event, 0)
	handler := func(events []*kafka.Event) error {
		receivedEvents = append(receivedEvents, events...)
		return nil
	}

	consumer, err := kafka.NewConsumer(kafka.ConsumerConfig{
		Brokers:       brokers,
		Topics:        []string{"test-events"},
		GroupID:       "test-group",
		BatchSize:     10,
		BatchTimeout:  1 * time.Second,
		MaxRetries:    3,
		RetryBackoff:  100 * time.Millisecond,
		EnableMetrics: false,
	}, handler, createTestLogger())
	require.NoError(t, err)
	defer consumer.Close()

	// Start consumer
	err = consumer.Start()
	require.NoError(t, err)

	// Wait for messages to be consumed
	time.Sleep(3 * time.Second)

	// Verify
	assert.GreaterOrEqual(t, len(receivedEvents), len(testEvents))

	// Verify event data
	eventIDs := make(map[string]bool)
	for _, event := range receivedEvents {
		eventIDs[event.EventID] = true
	}
	assert.True(t, eventIDs["test-1"])
	assert.True(t, eventIDs["test-2"])
}

func TestKafka_MessageValidation_Integration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping Kafka integration test")
	}

	ctx := context.Background()

	kafkaContainer, brokers := setupKafkaContainer(t, ctx)
	defer kafkaContainer.Terminate(ctx)

	producer, err := kafka.NewProducer(kafka.ProducerConfig{
		Brokers:       brokers,
		Topic:         "validation-test",
		MaxRetries:    1,
		RetryBackoff:  100 * time.Millisecond,
		EnableMetrics: false,
	}, createTestLogger())
	require.NoError(t, err)
	defer producer.Close()

	tests := []struct {
		name      string
		event     *kafka.Event
		expectErr bool
	}{
		{
			name: "valid event",
			event: &kafka.Event{
				EventID:   "valid-1",
				EventType: "click",
				UserID:    "user-1",
				PageURL:   "/page",
				Timestamp: time.Now().Format(time.RFC3339),
			},
			expectErr: false,
		},
		{
			name: "invalid event type",
			event: &kafka.Event{
				EventID:   "invalid-1",
				EventType: "invalid-type",
				UserID:    "user-1",
				PageURL:   "/page",
				Timestamp: time.Now().Format(time.RFC3339),
			},
			expectErr: true,
		},
		{
			name: "missing required fields",
			event: &kafka.Event{
				EventID: "invalid-2",
				// Missing other required fields
			},
			expectErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := producer.SendEvent(tt.event)
			if tt.expectErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestKafka_ErrorHandling_Integration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping Kafka integration test")
	}

	ctx := context.Background()

	kafkaContainer, brokers := setupKafkaContainer(t, ctx)
	defer kafkaContainer.Terminate(ctx)

	// Test with handler that returns error
	errorHandler := func(events []*kafka.Event) error {
		return assert.AnError
	}

	consumer, err := kafka.NewConsumer(kafka.ConsumerConfig{
		Brokers:       brokers,
		Topics:        []string{"error-test"},
		GroupID:       "error-group",
		BatchSize:     5,
		BatchTimeout:  1 * time.Second,
		MaxRetries:    2,
		RetryBackoff:  100 * time.Millisecond,
		EnableMetrics: false,
	}, errorHandler, createTestLogger())
	require.NoError(t, err)
	defer consumer.Close()

	// Send test event
	producer, err := kafka.NewProducer(kafka.ProducerConfig{
		Brokers:       brokers,
		Topic:         "error-test",
		MaxRetries:    1,
		EnableMetrics: false,
	}, createTestLogger())
	require.NoError(t, err)
	defer producer.Close()

	event := &kafka.Event{
		EventID:   "error-test-1",
		EventType: "click",
		UserID:    "user-1",
		PageURL:   "/test",
		Timestamp: time.Now().Format(time.RFC3339),
	}

	err = producer.SendEvent(event)
	assert.NoError(t, err)

	// Start consumer (should retry on error)
	err = consumer.Start()
	assert.NoError(t, err)

	// Wait for retry attempts
	time.Sleep(2 * time.Second)

	// Consumer should handle retries gracefully
}

// Helper function to setup Kafka testcontainer
func setupKafkaContainer(t *testing.T, ctx context.Context) (testcontainers.Container, []string) {
	req := testcontainers.ContainerRequest{
		Image:        "confluentinc/cp-kafka:7.5.0",
		ExposedPorts: []string{"9093/tcp"},
		Env: map[string]string{
			"KAFKA_BROKER_ID":                        "1",
			"KAFKA_LISTENERS":                        "PLAINTEXT://0.0.0.0:9093,BROKER://0.0.0.0:9092",
			"KAFKA_ADVERTISED_LISTENERS":             "PLAINTEXT://localhost:9093,BROKER://kafka:9092",
			"KAFKA_LISTENER_SECURITY_PROTOCOL_MAP":   "BROKER:PLAINTEXT,PLAINTEXT:PLAINTEXT",
			"KAFKA_INTER_BROKER_LISTENER_NAME":       "BROKER",
			"KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR": "1",
			"KAFKA_ZOOKEEPER_CONNECT":                "zookeeper:2181",
		},
		WaitingFor: wait.ForLog("started (kafka.server.KafkaServer)").WithStartupTimeout(60 * time.Second),
	}

	kafkaContainer, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	require.NoError(t, err)

	host, err := kafkaContainer.Host(ctx)
	require.NoError(t, err)

	port, err := kafkaContainer.MappedPort(ctx, "9093")
	require.NoError(t, err)

	brokers := []string{host + ":" + port.Port()}

	// Wait for Kafka to be ready
	time.Sleep(10 * time.Second)

	return kafkaContainer, brokers
}

func createTestLogger() zerolog.Logger {
	// Return test logger
	return zerolog.Nop()
}
