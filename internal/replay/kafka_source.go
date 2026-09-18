package replay

import (
	"context"
	"fmt"

	"github.com/segmentio/kafka-go"

	"github.com/sahilalamgir/durable-agent-execution-runtime/internal/events"
	dkafka "github.com/sahilalamgir/durable-agent-execution-runtime/internal/kafka"
)

// KafkaEventSource implements EventSource directly against Kafka: the
// authoritative replay source (D-2). Postgres's EventReader (internal/store)
// is for inspection and cross-checking only.
type KafkaEventSource struct {
	brokers []string
}

// NewKafkaEventSource builds a KafkaEventSource against brokers. It panics
// if brokers is empty: this is a startup configuration error (CLAUDE.md's
// "panic only during startup/config loading" rule), and failing loudly here
// beats an obscure index-out-of-range panic on the first Events call.
func NewKafkaEventSource(brokers []string) *KafkaEventSource {
	if len(brokers) == 0 {
		panic("replay: NewKafkaEventSource requires at least one broker")
	}
	return &KafkaEventSource{brokers: brokers}
}

// Events implements FR-19's algorithm: dial the partition runID hashes to,
// read its first offset and high-watermark, and scan from first up to
// hwm-1 (the last offset an acks=all write is guaranteed to have moved past
// once acknowledged), keeping only messages whose key equals runID, in
// offset order. It returns ErrRunNotFound if the partition is empty or no
// message in it matches runID.
func (s *KafkaEventSource) Events(ctx context.Context, runID string) ([]events.Envelope, error) {
	partition := dkafka.PartitionFor(runID, dkafka.RunEventsPartitions)

	// Try every broker, not just brokers[0]: a single unreachable broker
	// shouldn't fail replay when the rest of a healthy cluster could still
	// serve this partition's leader.
	var conn *kafka.Conn
	var dialErr error
	for _, broker := range s.brokers {
		conn, dialErr = kafka.DialLeader(ctx, "tcp", broker, dkafka.TopicRunEvents, partition)
		if dialErr == nil {
			break
		}
	}
	if conn == nil {
		return nil, fmt.Errorf("dialing partition %d leader for run %s (tried %d broker(s)): %w", partition, runID, len(s.brokers), dialErr)
	}
	defer conn.Close()

	first, hwm, err := conn.ReadOffsets()
	if err != nil {
		return nil, fmt.Errorf("reading offsets for partition %d: %w", partition, err)
	}
	if hwm == first {
		return nil, ErrRunNotFound
	}

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   s.brokers,
		Topic:     dkafka.TopicRunEvents,
		Partition: partition,
		// No GroupID: this is a one-shot scan of a fixed offset range, not
		// a durable consumer — committing an offset here would be
		// meaningless and could confuse a real consumer group.
	})
	defer reader.Close()
	if err := reader.SetOffset(first); err != nil {
		return nil, fmt.Errorf("setting reader offset to %d on partition %d: %w", first, partition, err)
	}

	var out []events.Envelope
	for {
		msg, err := reader.ReadMessage(ctx)
		if err != nil {
			return nil, fmt.Errorf("reading message from partition %d: %w", partition, err)
		}
		if string(msg.Key) == runID {
			env, err := events.Decode(msg.Value)
			if err != nil {
				return nil, fmt.Errorf("decoding message at partition=%d offset=%d: %w", partition, msg.Offset, err)
			}
			out = append(out, env)
		}
		if msg.Offset >= hwm-1 {
			break
		}
	}

	if len(out) == 0 {
		return nil, ErrRunNotFound
	}
	return out, nil
}
