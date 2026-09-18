//go:build integration

// These tests exercise this package against a real Postgres instance. They
// are not run by `go test ./...`; run them explicitly with
// `go test -tags=integration ./internal/store/...` against
// `docker compose up`.
package store

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/sahilalamgir/durable-agent-execution-runtime/internal/events"
	"github.com/sahilalamgir/durable-agent-execution-runtime/internal/replay"
)

const integrationDSN = "postgres://dae:dae@localhost:5432/dae?sslmode=disable"

func mustPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	pool, err := pgxpool.New(context.Background(), integrationDSN)
	if err != nil {
		t.Fatalf("pgxpool.New() error = %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

func TestProjectionApplyEventInsertsAndUpsertsRun(t *testing.T) {
	pool := mustPool(t)
	proj := NewProjection(pool)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := proj.Migrate(ctx); err != nil {
		t.Fatalf("Migrate() error = %v", err)
	}

	runID := uuid.New().String()
	env, err := events.NewEnvelope(runID, "local-dev", 0, events.RunStartedPayload{
		WorkloadType: "repo-maintenance-agent",
		Input:        []byte(`{"prompt":"test"}`),
		MaxSteps:     10,
	}, time.Now())
	if err != nil {
		t.Fatalf("NewEnvelope() error = %v", err)
	}

	inserted, err := proj.ApplyEvent(ctx, env)
	if err != nil {
		t.Fatalf("ApplyEvent() error = %v", err)
	}
	if !inserted {
		t.Fatalf("ApplyEvent() inserted = false, want true")
	}

	// Redelivery: same event_id, same sequence_number -> inserted=false, no error.
	inserted, err = proj.ApplyEvent(ctx, env)
	if err != nil {
		t.Fatalf("ApplyEvent() redelivery error = %v", err)
	}
	if inserted {
		t.Fatalf("ApplyEvent() redelivery inserted = true, want false")
	}

	// Conflict: same sequence_number, different event_id -> ErrConflictingEvent.
	conflicting, err := events.NewEnvelope(runID, "local-dev", 0, events.RunStartedPayload{
		WorkloadType: "repo-maintenance-agent",
		Input:        []byte(`{"prompt":"different"}`),
		MaxSteps:     10,
	}, time.Now())
	if err != nil {
		t.Fatalf("NewEnvelope() error = %v", err)
	}
	if _, err := proj.ApplyEvent(ctx, conflicting); !errors.Is(err, replay.ErrConflictingEvent) {
		t.Fatalf("ApplyEvent(conflicting) error = %v, want errors.Is(_, ErrConflictingEvent)", err)
	}

	reader := NewEventReader(pool)
	got, err := reader.Events(ctx, runID)
	if err != nil {
		t.Fatalf("Events() error = %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("Events() returned %d row(s), want 1", len(got))
	}
}

func TestEventReaderReturnsErrRunNotFound(t *testing.T) {
	pool := mustPool(t)
	proj := NewProjection(pool)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := proj.Migrate(ctx); err != nil {
		t.Fatalf("Migrate() error = %v", err)
	}

	reader := NewEventReader(pool)
	_, err := reader.Events(ctx, fmt.Sprintf("does-not-exist-%d", time.Now().UnixNano()))
	if !errors.Is(err, replay.ErrRunNotFound) {
		t.Fatalf("Events() error = %v, want errors.Is(_, ErrRunNotFound)", err)
	}
}
