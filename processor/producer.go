package processor

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
)

// KafkaProducer handles Kafka message production
type KafkaProducer struct {
	writer *kafka.Writer
}

// NewKafkaProducer creates a new Kafka producer
func NewKafkaProducer(writer *kafka.Writer) *KafkaProducer {
	return &KafkaProducer{
		writer: writer,
	}
}

// Close closes the Kafka producer
func (kp *KafkaProducer) Close() error {
	if kp.writer != nil {
		return kp.writer.Close()
	}
	return nil
}

// SourceRequest represents the request body for the /api/v1/sources endpoint
type SourceRequest struct {
	Topic    string `json:"topic" binding:"required"`
	Count    int    `json:"count"`    // Number of events to generate (default: 10)
	Interval int    `json:"interval"` // Interval between events in milliseconds (default: 100)
}

// ClickEvent represents a user click event
type ClickEvent struct {
	EventID   string `json:"event_id"`
	Timestamp int64  `json:"timestamp"`
	PageURL   string `json:"page_url"`
	UserID    int    `json:"user_id"`
}

// CreateSourceHandler handles POST /api/v1/sources endpoint
// It accepts a JSON body and asynchronously sends dummy events to Kafka
func CreateSourceHandler(producer *KafkaProducer) gin.HandlerFunc {
	return func(c *gin.Context) {
		var req SourceRequest
		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{
				"error":   "Invalid request body",
				"details": err.Error(),
			})
			return
		}

		// Set defaults
		if req.Count == 0 {
			req.Count = 10
		}
		if req.Interval == 0 {
			req.Interval = 100
		}

		// Validate topic
		if req.Topic == "" {
			c.JSON(http.StatusBadRequest, gin.H{
				"error": "Topic is required",
			})
			return
		}

		// Start asynchronous event generation
		go producer.generateAndSendEvents(req.Topic, req.Count, req.Interval)

		// Immediate response
		c.JSON(http.StatusAccepted, gin.H{
			"message": fmt.Sprintf("Started sending %d events to topic '%s'", req.Count, req.Topic),
			"topic":   req.Topic,
			"count":   req.Count,
		})
	}
}

// generateAndSendEvents generates and sends dummy click events to Kafka
func (kp *KafkaProducer) generateAndSendEvents(topic string, count, intervalMs int) {
	log.Printf("Starting to send %d events to topic '%s'", count, topic)

	// Sample page URLs for generating dummy data
	pages := []string{"/home", "/product", "/about", "/contact", "/pricing", "/blog", "/features"}
	userIDs := []int{1001, 1002, 1003, 1004, 1005, 1006, 1007, 1008, 1009, 1010}

	successCount := 0
	errorCount := 0

	for i := 0; i < count; i++ {
		// Generate dummy click event
		event := ClickEvent{
			EventID:   uuid.New().String(),
			Timestamp: time.Now().Unix(),
			PageURL:   pages[i%len(pages)],
			UserID:    userIDs[i%len(userIDs)],
		}

		// Serialize to JSON
		eventJSON, err := json.Marshal(event)
		if err != nil {
			log.Printf("Error marshaling event: %v", err)
			errorCount++
			continue
		}

		// Send to Kafka
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		err = kp.writer.WriteMessages(ctx, kafka.Message{
			Topic: topic,
			Key:   []byte(event.EventID),
			Value: eventJSON,
			Headers: []kafka.Header{
				{Key: "source", Value: []byte("go-api")},
				{Key: "version", Value: []byte("1.0")},
			},
		})
		cancel()

		if err != nil {
			log.Printf("Error sending event to Kafka: %v", err)
			errorCount++
		} else {
			successCount++
			log.Printf("Event sent [%d/%d]: %s -> %s (user: %d)",
				i+1, count, event.EventID, event.PageURL, event.UserID)
		}

		// Sleep between events (if not the last one)
		if i < count-1 && intervalMs > 0 {
			time.Sleep(time.Duration(intervalMs) * time.Millisecond)
		}
	}

	log.Printf("Finished sending events to topic '%s': %d success, %d errors",
		topic, successCount, errorCount)
}

// SendEvent sends a single event to Kafka (can be used for real-time ingestion)
func (kp *KafkaProducer) SendEvent(topic string, event ClickEvent) error {
	eventJSON, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("failed to marshal event: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = kp.writer.WriteMessages(ctx, kafka.Message{
		Topic: topic,
		Key:   []byte(event.EventID),
		Value: eventJSON,
		Headers: []kafka.Header{
			{Key: "source", Value: []byte("go-api")},
			{Key: "version", Value: []byte("1.0")},
		},
	})

	if err != nil {
		return fmt.Errorf("failed to write message to Kafka: %w", err)
	}

	return nil
}
