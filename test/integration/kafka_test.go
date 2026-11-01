package integration

import (
	"devops-fall2025-23b031152/pkg/kafka"
	"github.com/google/uuid"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
	"os"
	"testing"
	"time"
)

func TestKafka_ProducerConsumer_Integration(t *testing.T) {
	if testing.Short() {
		t.Skip("requires docker compose up")
	}
	broker := os.Getenv("KAFKA_TEST_BROKER")
	if broker == "" {
		broker = "localhost:29092"
	}
	topic := "integration-" + uuid.NewString()
	producer, err := kafka.NewProducer(kafka.ProducerConfig{Brokers: []string{broker}, Topic: topic}, zerolog.Nop())
	require.NoError(t, err)
	defer producer.Close()
	event := &kafka.Event{EventID: uuid.NewString(), EventType: "click", UserID: "integration-user", PageURL: "/test", Timestamp: time.Now().UTC().Format(time.RFC3339)}
	require.NoError(t, producer.SendEvent(event))
	received := make(chan string, 10)
	consumer, err := kafka.NewConsumer(kafka.ConsumerConfig{Brokers: []string{broker}, Topics: []string{topic}, GroupID: topic, BatchSize: 1}, func(events []*kafka.Event) error {
		for _, event := range events {
			received <- event.EventID
		}
		return nil
	}, zerolog.Nop())
	require.NoError(t, err)
	defer consumer.Close()
	require.NoError(t, consumer.Start())
	select {
	case id := <-received:
		require.Equal(t, event.EventID, id)
	case <-time.After(45 * time.Second):
		t.Fatal("event was not consumed")
	}
}
