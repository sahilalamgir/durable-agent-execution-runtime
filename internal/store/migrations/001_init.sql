-- system-spec.md's Postgres Schema, verbatim, with IF NOT EXISTS so this
-- migration is safe to run on every projector startup (FR-24).
CREATE TABLE IF NOT EXISTS events (
  id BIGSERIAL PRIMARY KEY,
  event_id UUID UNIQUE NOT NULL,
  run_id UUID NOT NULL,
  tenant_id TEXT NOT NULL,
  sequence_number INT NOT NULL,
  event_type TEXT NOT NULL,
  payload JSONB NOT NULL,
  occurred_at TIMESTAMPTZ NOT NULL,
  idempotency_key TEXT,
  UNIQUE (run_id, sequence_number)
);

CREATE TABLE IF NOT EXISTS runs (
  run_id UUID PRIMARY KEY,
  tenant_id TEXT NOT NULL,
  workload_type TEXT NOT NULL,
  status TEXT NOT NULL,
  current_step INT NOT NULL DEFAULT 0,
  created_at TIMESTAMPTZ NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL,
  last_event_id UUID
);
