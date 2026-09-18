package kafka

import (
	"context"
	"fmt"

	"github.com/segmentio/kafka-go"

	"github.com/sahilalamgir/durable-agent-execution-runtime/internal/events"
)

// EventPublisher publishes a single event envelope to run-events, blocking
// until the broker acknowledges with acks=all, or returning an error. On
// error, the event MAY or MAY NOT be in the log (an ambiguous write —
// system-spec.md EC-1/EC-2 explain how replay handles that safely).
//
// Publish makes exactly one attempt. It deliberately has no retry/backoff
// of its own: that lives in internal/agent's Loop instead, one level up, so
// AC-6 can be tested against a plain in-memory fake here with no
// Kafka-specific retry logic anywhere in tests.
type EventPublisher interface {
	Publish(ctx context.Context, e events.Envelope) error
}

// kafkaEventPublisher is the production EventPublisher.
type kafkaEventPublisher struct {
	writer *kafka.Writer
}

// NewEventPublisher builds a kafkaEventPublisher against brokers, targeting
// TopicRunEvents with the settings system-spec.md's Publisher section
// requires: RequiredAcks=RequireAll (wait for the full ISR, not
// fire-and-forget), Async=false, BatchSize=1 (so a synchronous single-event
// write doesn't stall behind kafka-go's default 1s BatchTimeout),
// AllowAutoTopicCreation=false (EnsureTopics owns topic creation, so
// creation never races), MaxAttempts=3, and a Balancer that delegates to
// PartitionFor so every producer partitions a run_id identically.
func NewEventPublisher(brokers []string) *kafkaEventPublisher {
	return &kafkaEventPublisher{
		writer: &kafka.Writer{
			Addr:                   kafka.TCP(brokers...),
			Topic:                  TopicRunEvents,
			RequiredAcks:           kafka.RequireAll,
			Async:                  false,
			BatchSize:              1,
			Balancer:               kafka.BalancerFunc(balanceByPartitionFor),
			AllowAutoTopicCreation: false,
			MaxAttempts:            3,
		},
	}
}

// balanceByPartitionFor adapts PartitionFor to kafka.Balancer's signature so
// it can be assigned directly to Writer.Balancer via kafka.BalancerFunc.
func balanceByPartitionFor(msg kafka.Message, partitions ...int) int {
	return PartitionFor(string(msg.Key), len(partitions))
}

// Publish encodes e and writes it, keyed by run_id, waiting for the
// broker's acks=all acknowledgement. It makes exactly one attempt; callers
// that need retry-with-backoff (the worker does, per FR-8/EC-1) implement
// it around this call, always resubmitting the identical envelope.
func (p *kafkaEventPublisher) Publish(ctx context.Context, e events.Envelope) error {
	b, err := events.Encode(e)
	if err != nil {
		return fmt.Errorf("encoding event %s (seq %d) for publish: %w", e.EventType, e.SequenceNumber, err)
	}
	if err := p.writer.WriteMessages(ctx, kafka.Message{
		Key:   []byte(e.RunID),
		Value: b,
	}); err != nil {
		return fmt.Errorf("publishing event %s (seq %d): %w", e.EventType, e.SequenceNumber, err)
	}
	return nil
}

// Close flushes and closes the underlying writer. It is not part of the
// EventPublisher interface (fakes in tests have nothing to close) — only
// production callers doing a clean shutdown need it.
func (p *kafkaEventPublisher) Close() error {
	if err := p.writer.Close(); err != nil {
		return fmt.Errorf("closing kafka writer: %w", err)
	}
	return nil
}
