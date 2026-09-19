package agent

import (
	"context"
	"fmt"
	"sync"

	"github.com/sahilalamgir/durable-agent-execution-runtime/internal/events"
)

// fakePublisher is an in-memory kafka.EventPublisher for tests: it records
// every envelope it's asked to publish, and can be told to fail on a
// specific call number and/or a specific event type, so AC-4/AC-5/AC-6 can
// be tested as plain unit tests with no Kafka involved at all.
type fakePublisher struct {
	mu sync.Mutex

	// published records every envelope Publish was asked to publish,
	// including ones it failed. Each entry index is that call's 1-based
	// call number.
	published []events.Envelope
	calls     int

	// failCall, if non-zero, makes Publish fail on exactly that 1-based
	// call number (every other call succeeds).
	failCall int
	// failEventType, if non-empty, makes every Publish call for that event
	// type fail.
	failEventType events.EventType

	// rec, if set, receives "Publish(<EventType>)" for every acknowledged
	// publish, so a test can check ordering against other fakes.
	rec *recorder
}

func (f *fakePublisher) Publish(ctx context.Context, e events.Envelope) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.calls++
	f.published = append(f.published, e)

	if f.failCall != 0 && f.calls == f.failCall {
		return fmt.Errorf("fakePublisher: forced failure on call %d", f.calls)
	}
	if f.failEventType != "" && e.EventType == f.failEventType {
		return fmt.Errorf("fakePublisher: forced failure for event type %s", e.EventType)
	}
	f.rec.add("Publish(" + string(e.EventType) + ")")
	return nil
}

// snapshot returns a copy of every envelope published so far (including
// failed attempts — a retried event appears once per attempt, all with the
// same event_id and sequence_number), safe to read after Run has returned.
func (f *fakePublisher) snapshot() []events.Envelope {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]events.Envelope(nil), f.published...)
}
