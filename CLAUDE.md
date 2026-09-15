# CLAUDE.md

## Project Context

This is a durable execution runtime for long-running, multi-step AI agent tasks. The goal is to guarantee that if the worker running an agent task crashes mid-task, another worker can resume it exactly where it stopped — without re-executing any real-world side effect that already happened (e.g. no duplicate PR, no duplicate email). The agent itself (a simple repo-maintenance bot) is a deliberately simple demo workload; the thing being built and evaluated is the reliability layer underneath it: event-sourced state, idempotent tool execution under crash/replay, a Kafka-based worker pool, and a Kubernetes-based chaos test proving the guarantee holds.

Full contracts, schemas, and edge cases live in `.claude/specs/system-spec.md`. **Read that file before implementing anything involving events, gRPC messages, Kafka topics, or Redis keys — don't invent a shape here or in code without it being reflected there first.** This file covers _how the codebase should look and behave_, not _what the contracts are_.

## Architecture

No code exists yet, so this is a proposed layout for Phase 0/1. Update this section once real code exists and the layout has proven itself — don't let it silently go stale.

```
/cmd
  /worker            → worker process entrypoint (consumes run-commands, executes agent loop)
  /controlplane      → gRPC control-plane server entrypoint
/internal
  /agent             → the LLM-call → tool-decision loop itself
  /events            → event envelope + per-type payload structs, (de)serialization
  /idempotency       → Redis fencing/claim logic (Phase 3+)
  /kafka             → producer/consumer wrappers, topic name constants
  /store             → Postgres access — the projector is the ONLY writer here (see Critical Rules)
  /grpcserver        → generated protobuf code + control-plane service implementation
  /tools             → tool implementations (mocked early, real repo-maintenance tools in Phase 9)
  /metrics           → Prometheus instrumentation
/proto               → .proto source files
/deploy
  /docker            → Dockerfiles
  /k8s               → K8s manifests, KEDA ScaledObject (Phase 6+)
/scripts             → chaos test harness, local dev helper scripts
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

Placeholder — fill these in for real once Phase 0 exists, don't leave this stale:

- `docker-compose up` — start Kafka, Redis, Postgres locally
- `go run ./scripts/connectivity` — run the Phase 0 Kafka/Redis connectivity check
- `go run ./cmd/worker` — run a single worker
- `go run ./cmd/controlplane` — run the control plane
- `go test ./...` — run all tests

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
| ----- | ---------------------------------- | ----------- |
| 0     | Local environment (Docker Compose) | Not started |
| 1     | Agent loop, no durability          | Not started |
| 2     | Event journal (Kafka + Postgres)   | Not started |
| 3     | Idempotent tool execution (Redis)  | Not started |
| 4     | Kafka worker pool + approval flow  | Not started |
| 5     | gRPC control plane                 | Not started |
| 6     | Kubernetes + KEDA                  | Not started |
| 7     | Prometheus + Grafana               | Not started |
| 8     | Chaos test                         | Not started |
| 9     | Demo repo-maintenance agent        | Not started |
