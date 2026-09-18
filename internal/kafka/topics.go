// Package kafka wraps segmentio/kafka-go with the topic layout, partitioning
// rule, and publisher settings this project depends on for ordering and
// durability. Nothing outside this package should construct a *kafka.Writer
// or *kafka.Reader directly against run-events — every setting here is
// load-bearing (see system-spec.md's Publisher required-settings table).
package kafka

import (
	"context"
	"fmt"

	"github.com/segmentio/kafka-go"
)

const (
	// TopicRunEvents is the one topic this phase produces to and consumes
	// from: the durable, ordered log of every run's events.
	TopicRunEvents = "run-events"
	// RunEventsPartitions is permanent once events exist: changing it sends
	// a run_id to a different partition and breaks the Kafka replay scan
	// (system-spec.md's Constraints section). Changing it requires a spec
	// change plus a topic rebuild, not just editing this constant.
	RunEventsPartitions = 6
	// ProjectorConsumerGroup is the consumer group cmd/projector uses. It is
	// a constant, not configurable, because there must only ever be one
	// logical projector reading run-events (EC-14 tolerates accidental
	// duplicates, but there is no reason to name the group differently per
	// deployment).
	ProjectorConsumerGroup = "projector"
)

// EnsureTopics idempotently creates TopicRunEvents with RunEventsPartitions
// partitions, replication factor 1, cleanup.policy=delete, and
// retention.ms=-1 (infinite — this topic is the source of truth, not a
// transient queue). Conn.CreateTopics is already a no-op against an
// existing topic, so this is safe to call on every worker/projector
// startup.
//
// CreateTopics silently no-ops on a mismatched config against an existing
// topic rather than erroring (e.g. a topic created by hand with 3
// partitions), so EnsureTopics additionally reads back the topic's actual
// partition count and fails, naming expected vs. actual, if it doesn't
// match RunEventsPartitions. This is the only way to implement EC-11.
func EnsureTopics(ctx context.Context, brokers []string) error {
	if len(brokers) == 0 {
		return fmt.Errorf("ensuring topics: no brokers configured")
	}

	conn, err := kafka.DialContext(ctx, "tcp", brokers[0])
	if err != nil {
		return fmt.Errorf("dialing kafka broker %s: %w", brokers[0], err)
	}
	defer conn.Close()

	if err := conn.CreateTopics(kafka.TopicConfig{
		Topic:             TopicRunEvents,
		NumPartitions:     RunEventsPartitions,
		ReplicationFactor: 1,
		ConfigEntries: []kafka.ConfigEntry{
			{ConfigName: "cleanup.policy", ConfigValue: "delete"},
			{ConfigName: "retention.ms", ConfigValue: "-1"},
		},
	}); err != nil {
		return fmt.Errorf("creating topic %s: %w", TopicRunEvents, err)
	}

	partitions, err := conn.ReadPartitions(TopicRunEvents)
	if err != nil {
		return fmt.Errorf("reading back partitions for topic %s: %w", TopicRunEvents, err)
	}
	if len(partitions) != RunEventsPartitions {
		return fmt.Errorf("topic %s has %d partition(s), want %d (created by hand with a different partition count?)", TopicRunEvents, len(partitions), RunEventsPartitions)
	}
	return nil
}
