package store

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/sahilalamgir/durable-agent-execution-runtime/internal/events"
	"github.com/sahilalamgir/durable-agent-execution-runtime/internal/replay"
)

// EventReader implements replay.EventSource against Postgres: for
// inspection and cross-checking only (D-2, FR-20). Worker logic must never
// use this to decide what to do next — only replay.NewKafkaEventSource is
// authoritative.
type EventReader struct {
	pool *pgxpool.Pool
}

// NewEventReader builds an EventReader around pool.
func NewEventReader(pool *pgxpool.Pool) *EventReader {
	return &EventReader{pool: pool}
}

// Events implements replay.EventSource per FR-20's query: every event for
// runID, in sequence_number order. It returns replay.ErrRunNotFound if
// there are none — including when the projector simply hasn't caught up
// yet (EC-13): that looks identical to "run doesn't exist" from Postgres's
// point of view, and is not an error in either case.
func (r *EventReader) Events(ctx context.Context, runID string) ([]events.Envelope, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT event_id, run_id, tenant_id, sequence_number, event_type, payload, occurred_at, idempotency_key
		FROM events
		WHERE run_id = $1
		ORDER BY sequence_number
	`, runID)
	if err != nil {
		return nil, fmt.Errorf("querying events for run %s: %w", runID, err)
	}
	defer rows.Close()

	var out []events.Envelope
	for rows.Next() {
		var e events.Envelope
		var eventType string
		if err := rows.Scan(&e.EventID, &e.RunID, &e.TenantID, &e.SequenceNumber, &eventType, &e.Payload, &e.OccurredAt, &e.IdempotencyKey); err != nil {
			return nil, fmt.Errorf("scanning event row for run %s: %w", runID, err)
		}
		e.EventType = events.EventType(eventType)
		out = append(out, e)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("reading event rows for run %s: %w", runID, err)
	}
	if len(out) == 0 {
		return nil, replay.ErrRunNotFound
	}
	return out, nil
}
