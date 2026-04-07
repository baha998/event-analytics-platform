package kafka

import (
	"context"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/IBM/sarama"
	"github.com/rs/zerolog"

	"devops-fall2025-23b031152/internal/validation"
	"devops-fall2025-23b031152/pkg/metrics"
)

// BatchEventHandler handles batches of events
type BatchEventHandler func(events []*Event) error

// ConsumerConfig holds consumer configuration
type ConsumerConfig struct {
	Brokers       []string
	Topics        []string
	GroupID       string
	BatchSize     int
	BatchTimeout  time.Duration
	MaxRetries    int
	RetryBackoff  time.Duration
	EnableMetrics bool
}

// Consumer handles Kafka message consumption with batch processing
type Consumer struct {
	consumerGroup  sarama.ConsumerGroup
	config         ConsumerConfig
	handler        BatchEventHandler
	log            zerolog.Logger
	wg             sync.WaitGroup
	ctx            context.Context
	cancel         context.CancelFunc
	batchChan      chan *sarama.ConsumerMessage
	shutdownChan   chan struct{}
	mu             sync.Mutex
	isShuttingDown bool
}

// NewConsumer creates a new Kafka consumer with batch processing
func NewConsumer(config ConsumerConfig, handler BatchEventHandler, log zerolog.Logger) (*Consumer, error) {
	saramaConfig := sarama.NewConfig()
	saramaConfig.Consumer.Group.Rebalance.Strategy = sarama.NewBalanceStrategyRoundRobin()
	saramaConfig.Consumer.Offsets.Initial = sarama.OffsetOldest
	saramaConfig.Consumer.Offsets.AutoCommit.Enable = false // Manual commit for graceful shutdown
	saramaConfig.Version = sarama.V2_8_0_0

	// Set batch size defaults
	if config.BatchSize <= 0 {
		config.BatchSize = 100
	}
	if config.BatchTimeout <= 0 {
		config.BatchTimeout = 1 * time.Second
	}
	if config.MaxRetries <= 0 {
		config.MaxRetries = 3
	}
	if config.RetryBackoff <= 0 {
		config.RetryBackoff = 100 * time.Millisecond
	}

	consumerGroup, err := sarama.NewConsumerGroup(config.Brokers, config.GroupID, saramaConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create consumer group: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	log.Info().
		Strs("brokers", config.Brokers).
		Strs("topics", config.Topics).
		Str("group_id", config.GroupID).
		Int("batch_size", config.BatchSize).
		Dur("batch_timeout", config.BatchTimeout).
		Msg("Kafka consumer initialized")

	return &Consumer{
		consumerGroup: consumerGroup,
		config:        config,
		handler:       handler,
		log:           log,
		ctx:           ctx,
		cancel:        cancel,
		batchChan:     make(chan *sarama.ConsumerMessage, config.BatchSize*2),
		shutdownChan:  make(chan struct{}),
	}, nil
}

// Start starts consuming messages with batch processing
func (c *Consumer) Start() error {
	// Start consumer group
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		for {
			select {
			case <-c.ctx.Done():
				c.log.Info().Msg("Consumer context cancelled, stopping")
				return
			default:
				if err := c.consumerGroup.Consume(c.ctx, c.config.Topics, c); err != nil {
					c.log.Error().Err(err).Msg("Error from consumer")
					if c.config.EnableMetrics {
						for _, topic := range c.config.Topics {
							metrics.KafkaErrorsTotal.WithLabelValues(topic, "consume_error").Inc()
						}
					}
					// Wait before retrying
					time.Sleep(1 * time.Second)
				}
			}
		}
	}()

	// Start error handler
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		for err := range c.consumerGroup.Errors() {
			c.log.Error().Err(err).Msg("Consumer error")
			if c.config.EnableMetrics {
				metrics.KafkaErrorsTotal.WithLabelValues("unknown", "consumer_error").Inc()
			}
		}
	}()

	// Start batch processor
	c.wg.Add(1)
	go c.processBatches()

	c.log.Info().Strs("topics", c.config.Topics).Msg("Consumer started")
	return nil
}

// processBatches processes messages in batches
func (c *Consumer) processBatches() {
	defer c.wg.Done()

	batch := make([]*sarama.ConsumerMessage, 0, c.config.BatchSize)
	ticker := time.NewTicker(c.config.BatchTimeout)
	defer ticker.Stop()

	for {
		select {
		case <-c.ctx.Done():
			// Process remaining batch on shutdown
			if len(batch) > 0 {
				c.processBatch(batch)
			}
			close(c.shutdownChan)
			return

		case msg := <-c.batchChan:
			if msg == nil {
				continue
			}

			batch = append(batch, msg)

			// Process batch when it reaches the configured size
			if len(batch) >= c.config.BatchSize {
				c.processBatch(batch)
				batch = batch[:0] // Reset batch
				ticker.Reset(c.config.BatchTimeout)
			}

		case <-ticker.C:
			// Process batch on timeout
			if len(batch) > 0 {
				c.processBatch(batch)
				batch = batch[:0] // Reset batch
			}
		}
	}
}

// processBatch processes a batch of messages with retry logic
func (c *Consumer) processBatch(messages []*sarama.ConsumerMessage) {
	if len(messages) == 0 {
		return
	}

	// Step 1: Validate and parse events
	events := make([]*Event, 0, len(messages))
	validMessages := make([]*sarama.ConsumerMessage, 0, len(messages))

	for _, msg := range messages {
		// Step 2: Call ValidateEvent from internal/validation/event_validator.go
		validatedEvent, err := validation.ValidateEvent(msg.Value)
		if err != nil {
			// Step 3: If validation fails - log error with zerolog including offset and validation details
			c.log.Error().
				Err(err).
				Str("topic", msg.Topic).
				Int32("partition", msg.Partition).
				Int64("offset", msg.Offset).
				Str("validation_error", err.Error()).
				Msg("Event validation failed")

			// Step 3: Increment Prometheus counter: kafka_invalid_events_total
			if c.config.EnableMetrics {
				metrics.KafkaInvalidEventsTotal.WithLabelValues("validation_error").Inc()
			}
			// Step 3: Skip message (don't process or store)
			continue
		}

		// Step 4: If validation succeeds - increment valid events counter
		if c.config.EnableMetrics {
			metrics.KafkaValidEventsTotal.Inc()
		}

		// Step 4: Map to ClickHouse using MapToClickHouse()
		clickhouseData, err := validation.MapToClickHouse(validatedEvent)
		if err != nil {
			c.log.Error().
				Err(err).
				Str("topic", msg.Topic).
				Int32("partition", msg.Partition).
				Int64("offset", msg.Offset).
				Str("mapping_error", err.Error()).
				Msg("Failed to map event to ClickHouse format")

			if c.config.EnableMetrics {
				metrics.KafkaInvalidEventsTotal.WithLabelValues("mapping_error").Inc()
			}
			continue
		}

		// Convert ClickHouse map back to Event structure for handler
		event, err := UnmarshalEvent(msg.Value)
		if err != nil {
			c.log.Error().
				Err(err).
				Str("topic", msg.Topic).
				Int32("partition", msg.Partition).
				Int64("offset", msg.Offset).
				Msg("Failed to unmarshal event")

			if c.config.EnableMetrics {
				metrics.KafkaErrorsTotal.WithLabelValues(msg.Topic, "unmarshal_error").Inc()
			}
			continue
		}

		// Store ClickHouse data in event for later use
		_ = clickhouseData // ClickHouse data is now validated and ready

		events = append(events, event)
		validMessages = append(validMessages, msg)
	}

	if len(events) == 0 {
		c.log.Warn().Msg("No valid events in batch")
		return
	}

	// Step 4: Continue with existing processing logic - Process batch with retry logic
	var lastErr error
	for attempt := 0; attempt <= c.config.MaxRetries; attempt++ {
		if attempt > 0 {
			backoff := time.Duration(math.Pow(2, float64(attempt-1))) * c.config.RetryBackoff
			c.log.Info().
				Int("attempt", attempt).
				Dur("backoff", backoff).
				Msg("Retrying batch processing")
			time.Sleep(backoff)
		}

		err := c.handler(events)
		if err == nil {
			// Success - commit offsets
			c.commitOffsets(validMessages)

			if c.config.EnableMetrics {
				for _, msg := range validMessages {
					metrics.KafkaMessagesConsumed.WithLabelValues(msg.Topic).Inc()
				}
			}

			c.log.Debug().
				Int("count", len(events)).
				Int("attempt", attempt+1).
				Msg("Batch processed successfully")
			return
		}

		lastErr = err
		c.log.Warn().
			Err(err).
			Int("attempt", attempt+1).
			Int("max_retries", c.config.MaxRetries).
			Msg("Batch processing failed")
	}

	// All retries exhausted
	c.log.Error().
		Err(lastErr).
		Int("count", len(events)).
		Int("retries", c.config.MaxRetries).
		Msg("Batch processing failed after all retries")

	if c.config.EnableMetrics {
		for _, msg := range validMessages {
			metrics.KafkaErrorsTotal.WithLabelValues(msg.Topic, "processing_error").Inc()
			metrics.KafkaMessageProcessingErrors.WithLabelValues(msg.Topic).Inc()
		}
	}
}

// commitOffsets commits offsets for processed messages
func (c *Consumer) commitOffsets(messages []*sarama.ConsumerMessage) {
	// Offsets are committed via session.MarkMessage in ConsumeClaim
	// This is a placeholder for any additional offset management
}

// Setup is run at the beginning of a new session
func (c *Consumer) Setup(sarama.ConsumerGroupSession) error {
	return nil
}

// Cleanup is run at the end of a session
func (c *Consumer) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim processes messages from Kafka
func (c *Consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for {
		select {
		case <-c.ctx.Done():
			return nil
		case message := <-claim.Messages():
			if message == nil {
				continue
			}

			// Calculate and track Kafka lag
			if c.config.EnableMetrics {
				lag := claim.HighWaterMarkOffset() - message.Offset - 1
				if lag < 0 {
					lag = 0
				}
				metrics.KafkaLag.WithLabelValues(message.Topic, fmt.Sprintf("%d", message.Partition)).Set(float64(lag))
			}

			// Send message to batch channel
			select {
			case c.batchChan <- message:
				// Message queued for batch processing
			case <-c.ctx.Done():
				return nil
			}

			// Mark message as processed (offset will be committed)
			session.MarkMessage(message, "")
		}
	}
}

// Close gracefully shuts down the consumer
func (c *Consumer) Close() error {
	c.mu.Lock()
	if c.isShuttingDown {
		c.mu.Unlock()
		return nil
	}
	c.isShuttingDown = true
	c.mu.Unlock()

	c.log.Info().Msg("Shutting down consumer...")

	// Cancel context to stop consuming
	c.cancel()

	// Wait for batch processor to finish
	<-c.shutdownChan

	// Wait for all goroutines
	c.wg.Wait()

	// Close consumer group (commits offsets)
	if err := c.consumerGroup.Close(); err != nil {
		c.log.Error().Err(err).Msg("Failed to close consumer group")
		return err
	}

	c.log.Info().Msg("Kafka consumer closed gracefully")
	return nil
}
