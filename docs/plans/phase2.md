Phase 2: Event Journal (Kafka + Postgres) — Implementation Plan

Context

Phase 1 gave you a working single-process agent loop, but all of its state — conversation history, step counter, which tools already ran — lives in a Go variable in one process's memory. If the process dies mid-run, the run is simply gone: nothing records that clone_repo already succeeded, nothing tells a future worker where to resume.

Phase 2 makes every step of a run durable before anything acts on it, and proves — by actually killing the worker process and replaying — that the exact in-memory state can be reconstructed from the durable record alone, with zero reliance on anything that survived in memory. This is the foundation every later guarantee sits on: idempotent tools (Phase 3), crash recovery across a worker pool (Phase 4), and the chaos test (Phase 8) all depend on there being a durable, ordered, replayable history to recover from.

Two new processes get built: cmd/worker (Phase 1's lont to Kafka before/after each step) and cmd/projector (anew process whose only job is reading those events and inserting them into Postgres). Kafka is the source of truth; Postgres is a ived, rebuildable, cross-check-only projection (thec, and it's load-bearing — see the "Why Kafka isauthoritative" explanation from the tutoring pass).  
This plan is grounded in the actual Phase 0/1 code and .claude/specs/phase2-event-journal.md (already reviewed and approved — this planlements it, it doesn't relitigate it). One open deresolved with you: the Publish retry/backoff loop (5attempts, 200ms→2s, same envelope) lives inside agent.Loop, not inside kafkaEventPublisher — this is what makes AC-6 testable as a in unit test through the fake publisher, with no K needed in tests.
ld order
tom-up, with a go test checkpoint after each stageest-risk code in this phase) surfaces as a failing tabletest — not a confusing failure three layers up in a live worker run against real Kafka.

1. go.mod — add google/uuid, jackc/pgx/v5 (+ pgxpool). go mod tidy && go build ./... should still pass (nothing uses them yet). internal/events — envelope + payload types + codects/... → AC-1.
2. internal/tools — add HasSideEffect() bool to Tool + the 4 mocks; add DAE_MOCK_TOOL_DELAY. Existing registry_test.go still passes. internal/kafka — PartitionFor, EnsureTopics, Eventer. Unit test PartitionFor only; everything needing abroker is //go:build integration. internal/replay — RunState, Apply, Fold, Messages(EventSource. This is the heaviest checkpoint — do notproceed to step 6 until go test ./internal/replay/... is fully green (AC-2, AC-3, AC-7). This package is both the riskiest code and the thing everything downstream depends on.
3. internal/agent — rewire Loop/NewLoop to journal via the fold. go test ./internal/agent/... → AC-4, AC-5, AC-6; existing TestLoopRun cases still pass unchanged.
4. cmd/worker — env vars, run_id generation, EnsureTopics, exit codes, signal handling, crash injection. go build, then go list -deps
   ./cmd/worker | grep jackc/pgx prints nothing (AC-8mpose smoke run happens here.
5. internal/store — embedded migration, Projection, EventReader. Pure mapping helpers (statusForEvent, stepFromPayload) unit-tested
   without a DB; everything touching pgxpool is integ
6. cmd/projector — integration-only; manual run for AC-10.
7. internal/replay/kafka_source.go (NewKafkaEventSourallel with steps 7–9 since it only depends oninternal/kafka, not internal/store.
8. scripts/replay — depends on steps 5, 8, 10. Manua onward).
9. docker-compose.yml volumes + CLAUDE.md update — do this before step 7's first manual run, so durability across restarts is actually
   demonstrable, not forgotten until the end.

Package-by-package plan

internal/events

- types.go — EventType + 9 constants; Payload interfa); the 9 payload structs with exact spec'd JSON tags.
- envelope.go — Envelope; NewEnvelope(runID, tenantID, seq, p, now) (marshals payload, truncates now to µs + forces UTC per FR-4);
  Encode/Decode/DecodePayload.
- errors.go — ErrUnknownEventType, ErrMissingField, ErrInvalidPayload.
- Implementation note: Decode must distinguish "fieldfield present but zero-valued" (e.g. a missingoccurred_at vs. the zero time.Time both look like IsZero() after a naive unmarshal). Decode via a presence-checking pass (e.g.
  unmarshal into map[string]json.RawMessage first) be so FR-3's "missing required field" errors are correct.
- codec_test.go — AC-1: 9 round-trip cases + 4 error cases (unknown type, missing event_id, missing run_id, invalid JSON).

internal/tools (edits to existing package)

- Add HasSideEffect() bool to the Tool interface; clone_repo/run_tests → false, apply_fix/open_pr → true.
- New delay.go: a helper reading DAE_MOCK_TOOL_DELAY default 0) per call (not cached at init, per CLAUDE.md'sno-package-level-mutable-state rule) and sleeping that long respecting ctx. Each mock's Execute calls it first.
- One env var controls all 4 mocks' delay uniformly —l crash-timing tests (AC-11 only ever needs one toolslowed at a time; the others being slower too doesn't break anything).

internal/kafka

- topics.go — TopicRunEvents, RunEventsPartitions = 6, ProjectorConsumerGroup; EnsureTopics(ctx, brokers) error — dial + CreateTopics (tolerate "already exists"), then ReadPartitions toatches the constant, failing with expected-vs-actual
