package store

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/sahilalamgir/durable-agent-execution-runtime/internal/events"
	"github.com/sahilalamgir/durable-agent-execution-runtime/internal/replay"
)

// Projection is internal/store's write side: the ONLY code path that
// writes to Postgres (CLAUDE.md's Critical Rules). cmd/projector is its
// only caller.
type Projection struct {
	pool *pgxpool.Pool
}

// NewProjection builds a Projection around pool.
func NewProjection(pool *pgxpool.Pool) *Projection {
	return &Projection{pool: pool}
}

// Migrate applies the embedded schema migration (FR-24).
func (p *Projection) Migrate(ctx context.Context) error {
	return migrate(ctx, p.pool)
}

// ApplyEvent runs FR-23's single transaction: insert e into events —
// naming (run_id, sequence_number) as the conflict target explicitly,
// rather than a bare ON CONFLICT DO NOTHING, so an unrelated event_id
// collision surfaces as ErrConflictingEvent instead of being silently
// swallowed — and, only if that inserted a new row, upsert e's run into
// runs (FR-25). inserted=false means e already existed at that
// sequence_number (a redelivery, EC-2/EC-6): no error, and runs is not
// touched a second time.
func (p *Projection) ApplyEvent(ctx context.Context, e events.Envelope) (inserted bool, err error) {
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return false, fmt.Errorf("beginning transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }() // no-op once Commit has succeeded.

	tag, err := tx.Exec(ctx, `
		INSERT INTO events (event_id, run_id, tenant_id, sequence_number, event_type, payload, occurred_at, idempotency_key)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		ON CONFLICT (run_id, sequence_number) DO NOTHING
	`, e.EventID, e.RunID, e.TenantID, e.SequenceNumber, string(e.EventType), []byte(e.Payload), e.OccurredAt, e.IdempotencyKey)
	if err != nil {
		return false, fmt.Errorf("inserting event %s (run_id=%s seq=%d): %w", e.EventID, e.RunID, e.SequenceNumber, err)
	}

	if tag.RowsAffected() == 0 {
		return false, p.checkExistingEvent(ctx, tx, e)
	}

	if err := p.upsertRun(ctx, tx, e); err != nil {
		return false, err
	}

	if err := tx.Commit(ctx); err != nil {
		return false, fmt.Errorf("committing transaction: %w", err)
	}
	return true, nil
}

// checkExistingEvent handles the ON CONFLICT DO NOTHING case: it looks up
// which event_id currently occupies (run_id, sequence_number) and either
// commits a no-op (redelivery: same event_id) or reports ErrConflictingEvent
// (a different event_id genuinely collided at that sequence_number).
func (p *Projection) checkExistingEvent(ctx context.Context, tx pgx.Tx, e events.Envelope) error {
	var existingEventID string
	err := tx.QueryRow(ctx, `SELECT event_id FROM events WHERE run_id = $1 AND sequence_number = $2`, e.RunID, e.SequenceNumber).Scan(&existingEventID)
	if err != nil {
		return fmt.Errorf("checking existing event at run_id=%s seq=%d: %w", e.RunID, e.SequenceNumber, err)
	}
	if existingEventID != e.EventID {
		return fmt.Errorf("run_id=%s seq=%d already holds event_id=%s, got event_id=%s: %w", e.RunID, e.SequenceNumber, existingEventID, e.EventID, replay.ErrConflictingEvent)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("committing no-op redelivery transaction: %w", err)
	}
	return nil
}

// upsertRun applies FR-25's runs mapping. One statement handles both
// "RunStarted inserts the row" and every other event's update: the VALUES
// list is only ever actually used the first time (RunStarted is always the
// first event for any run_id, per replay's own invariants), and the
// ON CONFLICT DO UPDATE clause handles every event after that.
func (p *Projection) upsertRun(ctx context.Context, tx pgx.Tx, e events.Envelope) error {
	payload, err := events.DecodePayload(e)
	if err != nil {
		return fmt.Errorf("decoding payload for runs upsert: %w", err)
	}

	var workloadType string
	if started, ok := payload.(events.RunStartedPayload); ok {
		workloadType = started.WorkloadType
	}

	status := statusForEvent(e.EventType)
	var step *int
	if s, ok := stepFromPayload(payload); ok {
		step = &s
	}

	_, err = tx.Exec(ctx, `
		INSERT INTO runs (run_id, tenant_id, workload_type, status, current_step, created_at, updated_at, last_event_id)
		VALUES ($1, $2, $3, $4, COALESCE($5, 0), $6, $6, $7)
		ON CONFLICT (run_id) DO UPDATE SET
			status = EXCLUDED.status,
			current_step = COALESCE($5, runs.current_step),
			updated_at = EXCLUDED.updated_at,
			last_event_id = EXCLUDED.last_event_id
	`, e.RunID, e.TenantID, workloadType, status, step, e.OccurredAt, e.EventID)
	if err != nil {
		return fmt.Errorf("upserting run %s: %w", e.RunID, err)
	}
	return nil
}
