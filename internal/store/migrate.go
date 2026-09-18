// Package store is Postgres's derived, rebuildable projection of run-events
// (system-spec.md's dual-write decision, D-2). Only cmd/projector imports
// Projection (the write side); anything reading Postgres for inspection or
// cross-checking uses EventReader instead. No worker code may import the
// write side (CLAUDE.md's Critical Rules, FR-26).
package store

import (
	"context"
	_ "embed"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed migrations/001_init.sql
var initSchema string

// migrate applies the embedded schema migration. It is idempotent (every
// statement is CREATE TABLE IF NOT EXISTS), so it is safe to run on every
// projector startup (FR-24). No migration library is used: one embedded SQL
// file doesn't justify one (system-spec.md's Constraints section).
func migrate(ctx context.Context, pool *pgxpool.Pool) error {
	if _, err := pool.Exec(ctx, initSchema); err != nil {
		return fmt.Errorf("running schema migration: %w", err)
	}
	return nil
}
