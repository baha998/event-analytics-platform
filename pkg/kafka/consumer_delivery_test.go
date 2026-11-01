package kafka

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/google/uuid"
	"github.com/rs/zerolog"
)

type deliverySession struct {
	ctx       context.Context
	marked    atomic.Int32
	committed atomic.Int32
}

func (s *deliverySession) Claims() map[string][]int32                  { return nil }
func (s *deliverySession) MemberID() string                            { return "test" }
func (s *deliverySession) GenerationID() int32                         { return 1 }
func (s *deliverySession) MarkOffset(string, int32, int64, string)     {}
func (s *deliverySession) ResetOffset(string, int32, int64, string)    {}
func (s *deliverySession) MarkMessage(*sarama.ConsumerMessage, string) { s.marked.Add(1) }
func (s *deliverySession) Commit()                                     { s.committed.Add(1) }
func (s *deliverySession) Context() context.Context                    { return s.ctx }

type deliveryClaim struct{ messages chan *sarama.ConsumerMessage }

func (c deliveryClaim) Topic() string                            { return "events" }
func (c deliveryClaim) Partition() int32                         { return 0 }
func (c deliveryClaim) InitialOffset() int64                     { return 0 }
func (c deliveryClaim) HighWaterMarkOffset() int64               { return 1 }
func (c deliveryClaim) Messages() <-chan *sarama.ConsumerMessage { return c.messages }

func TestConsumerAcknowledgesOnlyAfterSuccessfulStorage(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	session := &deliverySession{ctx: ctx}
	claim := deliveryClaim{messages: make(chan *sarama.ConsumerMessage, 1)}
	payload, err := MarshalEvent(&Event{EventID: uuid.NewString(), EventType: "click", UserID: "user", PageURL: "/home", Timestamp: time.Now().UTC().Format(time.RFC3339)})
	if err != nil {
		t.Fatal(err)
	}
	claim.messages <- &sarama.ConsumerMessage{Topic: "events", Value: payload}
	var attempts atomic.Int32
	consumer := &Consumer{ctx: ctx, log: zerolog.Nop(), config: ConsumerConfig{BatchSize: 1, BatchTimeout: time.Second, RetryBackoff: time.Millisecond}}
	consumer.handler = func([]*Event) error {
		if session.marked.Load() != 0 || session.committed.Load() != 0 {
			t.Error("acknowledged before storage succeeded")
		}
		if attempts.Add(1) == 1 {
			return errors.New("database unavailable")
		}
		close(claim.messages)
		return nil
	}
	if err := consumer.ConsumeClaim(session, claim); err != nil {
		t.Fatal(err)
	}
	if attempts.Load() != 2 || session.marked.Load() != 1 || session.committed.Load() != 1 {
		t.Fatal("expected retry followed by one acknowledged message")
	}
}

func TestConsumerRetainsOffsetsWhenStorageFails(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	session := &deliverySession{ctx: ctx}
	claim := deliveryClaim{messages: make(chan *sarama.ConsumerMessage, 1)}
	payload, _ := MarshalEvent(&Event{EventID: uuid.NewString(), EventType: "click", UserID: "user", PageURL: "/home", Timestamp: time.Now().UTC().Format(time.RFC3339)})
	claim.messages <- &sarama.ConsumerMessage{Topic: "events", Value: payload}
	consumer := &Consumer{ctx: ctx, log: zerolog.Nop(), config: ConsumerConfig{BatchSize: 1, BatchTimeout: time.Second, RetryBackoff: time.Millisecond}, handler: func([]*Event) error { cancel(); return errors.New("database unavailable") }}
	_ = consumer.ConsumeClaim(session, claim)
	if session.marked.Load() != 0 || session.committed.Load() != 0 {
		t.Fatal("failed storage advanced offsets")
	}
}
