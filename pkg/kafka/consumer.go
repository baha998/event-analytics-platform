package kafka

import (
	"context"
	"fmt"
	"sync"
	"time"

	"devops-fall2025-23b031152/internal/validation"
	"devops-fall2025-23b031152/pkg/metrics"
	"github.com/IBM/sarama"
	"github.com/rs/zerolog"
)

type BatchEventHandler func([]*Event) error

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

// Consumer processes each partition sequentially. Failed batches block that
// partition; offsets are committed only after the handler succeeds.
type Consumer struct {
	consumerGroup sarama.ConsumerGroup
	config        ConsumerConfig
	handler       BatchEventHandler
	log           zerolog.Logger
	ctx           context.Context
	cancel        context.CancelFunc
	wg            sync.WaitGroup
	closeOnce     sync.Once
	closeErr      error
}

func NewConsumer(config ConsumerConfig, handler BatchEventHandler, log zerolog.Logger) (*Consumer, error) {
	if config.BatchSize <= 0 {
		config.BatchSize = 100
	}
	if config.BatchTimeout <= 0 {
		config.BatchTimeout = time.Second
	}
	if config.RetryBackoff <= 0 {
		config.RetryBackoff = 100 * time.Millisecond
	}
	cfg := sarama.NewConfig()
	cfg.Version = sarama.V2_8_0_0
	cfg.Consumer.Offsets.Initial = sarama.OffsetOldest
	cfg.Consumer.Offsets.AutoCommit.Enable = false
	group, err := sarama.NewConsumerGroup(config.Brokers, config.GroupID, cfg)
	if err != nil {
		return nil, fmt.Errorf("create consumer group: %w", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &Consumer{consumerGroup: group, config: config, handler: handler, log: log, ctx: ctx, cancel: cancel}, nil
}

func (c *Consumer) Start() error {
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		for c.ctx.Err() == nil {
			if err := c.consumerGroup.Consume(c.ctx, c.config.Topics, c); err != nil {
				c.log.Error().Err(err).Msg("Consumer session failed")
				select {
				case <-c.ctx.Done():
					return
				case <-time.After(time.Second):
				}
			}
		}
	}()
	return nil
}

func (c *Consumer) Setup(sarama.ConsumerGroupSession) error   { return nil }
func (c *Consumer) Cleanup(sarama.ConsumerGroupSession) error { return nil }

func (c *Consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	batch := make([]*sarama.ConsumerMessage, 0, c.config.BatchSize)
	ticker := time.NewTicker(c.config.BatchTimeout)
	defer ticker.Stop()
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		if err := c.processBatch(session.Context(), batch); err != nil {
			return err
		}
		for _, msg := range batch {
			session.MarkMessage(msg, "")
		}
		session.Commit()
		if c.config.EnableMetrics {
			lag := claim.HighWaterMarkOffset() - batch[len(batch)-1].Offset - 1
			if lag < 0 {
				lag = 0
			}
			metrics.KafkaLag.WithLabelValues(claim.Topic(), fmt.Sprint(claim.Partition())).Set(float64(lag))
		}
		batch = batch[:0]
		return nil
	}
	for {
		select {
		case <-session.Context().Done():
			return nil // Uncommitted messages are replayed.
		case <-c.ctx.Done():
			return nil
		case msg, ok := <-claim.Messages():
			if !ok {
				return nil
			}
			batch = append(batch, msg)
			if len(batch) >= c.config.BatchSize {
				if err := flush(); err != nil {
					return err
				}
			}
		case <-ticker.C:
			if err := flush(); err != nil {
				return err
			}
		}
	}
}

func (c *Consumer) processBatch(ctx context.Context, messages []*sarama.ConsumerMessage) error {
	events := make([]*Event, 0, len(messages))
	for _, msg := range messages {
		if _, err := validation.ValidateEvent(msg.Value); err != nil {
			// Malformed messages are deliberately discarded with an observable counter.
			c.log.Error().Err(err).Str("topic", msg.Topic).Int64("offset", msg.Offset).Msg("Discarding invalid event")
			if c.config.EnableMetrics {
				metrics.KafkaInvalidEventsTotal.WithLabelValues("validation_error").Inc()
			}
			continue
		}
		event, err := UnmarshalEvent(msg.Value)
		if err != nil {
			return err
		}
		events = append(events, event)
		if c.config.EnableMetrics {
			metrics.KafkaValidEventsTotal.Inc()
		}
	}
	if len(events) == 0 {
		return nil
	}
	backoff := c.config.RetryBackoff
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := c.ctx.Err(); err != nil {
			return err
		}
		if err := c.handler(events); err == nil {
			if c.config.EnableMetrics {
				for _, msg := range messages {
					metrics.KafkaMessagesConsumed.WithLabelValues(msg.Topic).Inc()
				}
			}
			return nil
		} else {
			c.log.Error().Err(err).Msg("Batch failed; retaining offsets and retrying")
			if c.config.EnableMetrics {
				metrics.KafkaErrorsTotal.WithLabelValues(messages[0].Topic, "storage_error").Inc()
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-c.ctx.Done():
			return c.ctx.Err()
		case <-time.After(backoff):
		}
		if backoff < 5*time.Second {
			backoff *= 2
		}
	}
}

func (c *Consumer) Close() error {
	c.closeOnce.Do(func() {
		c.cancel()
		c.wg.Wait()
		c.closeErr = c.consumerGroup.Close()
	})
	return c.closeErr
}
