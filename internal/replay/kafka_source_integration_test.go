//go:build integration

// This test exercises KafkaEventSource against a real Kafka broker. It is
// not run by `go test ./...`; run it explicitly with
// `go test -tags=integration ./internal/replay/...` against
// `docker compose up`.
package replay

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/sahilalamgir/durable-agent-execution-runtime/internal/events"
	dkafka "github.com/sahilalamgir/durable-agent-execution-runtime/internal/kafka"
)

var integrationBrokers = []string{"localhost:9092"}

func TestKafkaEventSourceEvents(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := dkafka.EnsureTopics(ctx, integrationBrokers); err != nil {
		t.Fatalf("EnsureTopics() error = %v", err)
	}

	pub := dkafka.NewEventPublisher(integrationBrokers)
	defer pub.Close()

	runID := fmt.Sprintf("kafka-source-test-%d", time.Now().UnixNano())
	start, err := events.NewEnvelope(runID, "local-dev", 0, events.RunStartedPayload{
		WorkloadType: "repo-maintenance-agent",
		Input:        []byte(`{"prompt":"test"}`),
		MaxSteps:     10,
	}, time.Now())
	if err != nil {
		t.Fatalf("NewEnvelope() error = %v", err)
	}
	if err := pub.Publish(ctx, start); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	src := NewKafkaEventSource(integrationBrokers)
	got, err := src.Events(ctx, runID)
	if err != nil {
		t.Fatalf("Events() error = %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("Events() returned %d envelope(s), want 1", len(got))
	}
	if got[0].EventID != start.EventID {
		t.Fatalf("Events()[0].EventID = %q, want %q", got[0].EventID, start.EventID)
	}
}

func TestKafkaEventSourceRunNotFound(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := dkafka.EnsureTopics(ctx, integrationBrokers); err != nil {
		t.Fatalf("EnsureTopics() error = %v", err)
	}

	src := NewKafkaEventSource(integrationBrokers)
	_, err := src.Events(ctx, fmt.Sprintf("does-not-exist-%d", time.Now().UnixNano()))
	if !errors.Is(err, ErrRunNotFound) {
		t.Fatalf("Events() error = %v, want errors.Is(_, ErrRunNotFound)", err)
	}
}
