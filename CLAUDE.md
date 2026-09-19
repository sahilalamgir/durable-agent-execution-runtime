# CLAUDE.md

## Project Context

This is a durable execution runtime for long-running, multi-step AI agent tasks. The goal is to guarantee that if the worker running an agent task crashes mid-task, another worker can resume it exactly where it stopped — without re-executing any real-world side effect that already happened (e.g. no duplicate PR, no duplicate email). The agent itself (a simple repo-maintenance bot) is a deliberately simple demo workload; the thing being built and evaluated is the reliability layer underneath it: event-sourced state, idempotent tool execution under crash/replay, a Kafka-based worker pool, and a Kubernetes-based chaos test proving the guarantee holds.

Full contracts, schemas, and edge cases live in `.claude/specs/system-spec.md`. **Read that file before implementing anything involving events, gRPC messages, Kafka topics, or Redis keys — don't invent a shape here or in code without it being reflected there first.** This file covers _how the codebase should look and behave_, not _what the contracts are_.

## Architecture

Phases 0–3 are real code now; everything else below is still a proposed layout. Update this section again once each later phase lands and its part of the layout has proven itself — don't let it silently go stale.

```
/cmd
  /worker            → worker process entrypoint: runs the journaled agent loop to completion (Phase 2), fences side-effecting tools through Redis and can resume a crashed run via DAE_RESUME_RUN_ID (Phase 3); consumes run-commands starting Phase 4
  /projector         → consumes run-events and is the ONLY writer to Postgres (Phase 2)
  /controlplane      → gRPC control-plane server entrypoint (Phase 5, proposed)
/internal
  /agent             → the LLM-call → tool-decision loop, journaling every step through internal/kafka before acting on it (Phase 2); one `drive` loop serves both a fresh run and Resume (Phase 3)
  /events            → event envelope + per-type payload structs, (de)serialization (Phase 2)
  /kafka             → producer wrapper, topic name constants, partitioning (Phase 2)
  /replay            → the pure fold (RunState, Apply/Fold, Messages/Digest, TerminalPayload) + Kafka/Postgres EventSource implementations (Phase 2/3)
  /store             → Postgres access — cmd/projector is the ONLY writer here (see Critical Rules); internal/store.EventReader is a read-only cross-check source (Phase 2)
  /tools             → tool implementations (mocked early, real repo-maintenance tools in Phase 9); the mock side-effect Ledger lives here (Phase 3)
  /idempotency       → Redis fencing: Key/ArgsHash, IdemRecord, Store (+RedisStore, Lua CAS), and Guard.Run, the one decision-table code path for live and recovery (Phase 3)
  /grpcserver        → generated protobuf code + control-plane service implementation (Phase 5, proposed)
  /metrics           → Prometheus instrumentation (Phase 7, proposed)
/proto               → .proto source files (Phase 5, proposed)
/deploy
  /docker            → Dockerfiles (Phase 6+, proposed)
  /k8s               → K8s manifests, KEDA ScaledObject (Phase 6+, proposed)
/scripts
  /connectivity      → Phase 0 Kafka/Redis connectivity check
  /replay            → FR-21 CLI: reconstructs and prints a run's state from Kafka or Postgres (Phase 2)
  /effects           → counts side effects in the mock ledger; exits 1 on any duplicate idempotency_key (Phase 3, reused by Phase 8)
  → chaos test harness lands here in Phase 8 (proposed)
.claude/specs/       → system spec + phase specs
docs/decisions/      → phase-by-phase decision log (human-written, not Claude Code's job)
```

## Code Style

No prior Go conventions exist for this project, so these are sensible idiomatic defaults — expect to edit this section the first time Claude Code does something here twice that you don't like ("correct once, then codify").

- Always run through `gofmt`/`goimports`; never hand-format.
- Wrap errors with context using `fmt.Errorf("doing X: %w", err)` — never return a bare error with no indication of what was being attempted.
- Don't `panic` except during startup/config loading, where failing loudly and immediately is correct.
- Every function doing I/O (Kafka, Redis, Postgres, gRPC) takes `context.Context` as its first argument. This isn't a style nicety here — cancellation propagation is load-bearing for `CancelRun` and for timing out a Redis check cleanly under the fail-closed rule.
- No global mutable state. Dependencies (Kafka client, Redis client, DB pool) are passed in explicitly via struct fields, not package-level variables — this is what makes idempotency logic testable in isolation.
- JSON field names in structs use `snake_case` via struct tags, matching the field names in the spec exactly. Go field names stay `PascalCase`.
- Event types are a typed string enum, not raw strings scattered through the code:
  ```go
  type EventType string
  const (
      EventRunStarted   EventType = "RunStarted"
      EventToolInvoked  EventType = "ToolInvoked"
      // ...
  )
  ```
- Prefer table-driven tests wherever there are multiple cases to cover — this will matter a lot for idempotency-state transitions and event-type handling.
- Keep functions short and single-purpose. If a function needs a comment explaining what its middle section does, that section is probably its own function.

## Preferred Libraries

- Kafka: `segmentio/kafka-go`
- Redis: `redis/go-redis/v9`
- Postgres: `jackc/pgx` (more actively maintained than `lib/pq` — flagging this as a pick, not a strong requirement; change it here if you have a reason to)
- gRPC: `google.golang.org/grpc` + `google.golang.org/protobuf`
- Metrics: `prometheus/client_golang`

Don't add a new dependency without a one-line reason in the commit/PR description. Keep the dependency surface small — the point of this project is that you can explain every piece of it, not that it showcases a large toolchain.

## Commands

- `docker compose up` — start Kafka, Redis, Postgres locally (named volumes keep Kafka's log and Postgres's data across `down`/`up`; `docker compose down -v` is the full reset)
- `go run ./scripts/connectivity` — run the Phase 0 Kafka/Redis connectivity check
- `ANTHROPIC_API_KEY=... go run ./cmd/worker` — run a single worker; it journals every step to `run-events` before acting on it (Phase 2), generates and prints its own `run_id`, and runs the hardcoded fake repo-maintenance task to completion. Every side-effecting tool goes through the Redis fence first (Phase 3). Exit codes: 0 completed, 1 failed/startup failure (incl. Redis unreachable), 2 resume run not found, 3 publish failed, 4 fence unavailable or tool outcome unknown (resumable, no terminal event), 137 injected crash. Env: `DAE_KAFKA_BROKERS` (default `localhost:9092`), `DAE_REDIS_ADDR` (default `localhost:6379`), `DAE_IDEMPOTENCY_TTL` (default `24h`, must exceed `DAE_TOOL_TIMEOUT`+90s: 30s stale grace + 1m expiry margin), `DAE_TOOL_TIMEOUT` (default `2m`; use `20s` for manual crash tests), `DAE_MOCK_LEDGER_PATH` (default `.dae/mock-ledger.jsonl`), `DAE_MOCK_TOOL_DELAY`, `DAE_RESUME_RUN_ID`, `DAE_CRASH_AFTER_SEQ` and `DAE_CRASH_AT` (crash-testing only)
- `ANTHROPIC_API_KEY=... DAE_RESUME_RUN_ID=<uuid> go run ./cmd/worker` — replay that run from Kafka and continue it from its `NextAction`. Only resume after the original process is dead (assumption A-1; Phase 4's lease enforces it). Unset `DAE_CRASH_AT` before resuming, or it fires again if the resumed run reaches the same point
- `DAE_CRASH_AT=<tool>:after_claim|after_execute|after_resolve` — exit(137) the first time that side-effecting tool reaches that point (claimed before `Execute` / `Execute` returned before resolve / resolved before `ToolResulted` is published)
- `go run ./scripts/effects [--ledger <path>] [--run-id <uuid>]` — print one row per idempotency key from the mock ledger plus `entries= distinct_keys= duplicates=`; exits 1 if any key has two or more entries (a duplicated side effect), 2 if the ledger is missing, 3 if it is unreadable
- `go run ./cmd/projector` — the only process that writes to Postgres; consumes `run-events` as consumer group `projector` and keeps `events`/`runs` current. Env: `DAE_KAFKA_BROKERS`, `DAE_POSTGRES_DSN` (default `postgres://dae:dae@localhost:5432/dae?sslmode=disable`), `DAE_PROJECTOR_CRASH_BEFORE_OFFSET_COMMIT` (crash-testing only)
- `go run ./scripts/replay --run-id <uuid> --source kafka` — reconstruct and print a run's state from its events (`--source postgres` for cross-checking only; Kafka is always authoritative, D-2)
- `go run ./cmd/controlplane` — run the control plane (Phase 5, not built yet)
- `go test ./...` — run all unit tests (no infrastructure required); `go test -tags=integration ./...` additionally runs the Kafka/Postgres integration tests, which need `docker compose up`
- `go list -deps ./cmd/worker | grep jackc/pgx` — must print nothing (AC-8/FR-26): this is a manual/CI check, not a Go test, since a build-time dependency graph isn't something `go test` can assert on directly

## Learning Goals & Process

This project exists so I (Sahil) genuinely learn distributed systems, concurrency, and this stack — not just to produce working code. Follow these rules every session:

- Before implementing anything involving a concept I likely haven't used before — event sourcing, Kafka (topics/partitions/consumer groups/rebalancing), Redis fencing/locking, Kubernetes, KEDA, gRPC, Docker networking, or any concurrency/race-condition reasoning — stop and explain it first, with a concrete example from this project, no code yet. Only proceed to implementation once I confirm I understand it.
- Never let me approve a plan touching idempotency, replay, or concurrency without first asking me to restate the mechanism back in my own words.
- At the end of each phase, remind me to write `docs/decisions/phase-N.md` myself, in my own words. Don't write it for me — instead, ask me guiding questions (what was the hard tradeoff, what did you pick, what would you tell an interviewer) to help me write it.

## Critical Rules

- IMPORTANT: Workers never write to Postgres directly. Only the projector service (consuming `run-events`) writes to the `events` table. Kafka is the source of truth; Postgres is a derived, rebuildable projection.
- IMPORTANT: No tool call with a real side effect executes without a Redis fencing check passing first. If the fencing check itself fails (Redis unreachable, timeout), fail **closed** — do not execute the tool, retry the check instead.
- IMPORTANT: The idempotency key must be claimed (written to Redis) _before_ the tool executes, not after it returns. Writing the result only after success leaves a gap where a crash mid-tool-call is invisible to recovery.
- IMPORTANT: Every Kafka topic partitions by `run_id`. This guarantees a single run's events/commands stay ordered and are never split across workers mid-run — never change the partition key without updating the spec first.
- Run cancellation is cooperative: checked between steps only, never interrupts an in-flight tool call.
- Any new event type, gRPC message field, or Redis key pattern gets added to `.claude/specs/system-spec.md` first, then implemented — not the other way around.

## Current Status

Update this table as phases complete — it's how a fresh Claude Code session knows what already exists vs. what's still planned, without you re-explaining it every time.

| Phase | Description                        | Status      |
| ----- | ----------------------------------- | ----------- |
| 0     | Local environment (Docker Compose)  | Done        |
| 1     | Agent loop, no durability          | Done        |
| 2     | Event journal (Kafka + Postgres)   | In progress |
| 3     | Idempotent tool execution (Redis)  | In progress |
| 4     | Kafka worker pool + approval flow  | Not started |
| 5     | gRPC control plane                 | Not started |
| 6     | Kubernetes + KEDA                  | Not started |
| 7     | Prometheus + Grafana               | Not started |
| 8     | Chaos test                         | Not started |
| 9     | Demo repo-maintenance agent        | Not started |
