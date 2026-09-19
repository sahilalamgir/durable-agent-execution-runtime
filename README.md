# Durable Agent Execution Runtime

**A fault-tolerant runtime for long-running AI agent tasks: if the worker crashes mid-task, another worker resumes exactly where it stopped, and no real-world side effect (a PR, an email) ever happens twice.**

![Go](https://img.shields.io/badge/Go-1.22+-00ADD8?logo=go&logoColor=white)
![Kafka](https://img.shields.io/badge/Kafka-event%20log-231F20?logo=apachekafka)
![Redis](https://img.shields.io/badge/Redis-fencing-DC382D?logo=redis&logoColor=white)
![Postgres](https://img.shields.io/badge/Postgres-projection-4169E1?logo=postgresql&logoColor=white)
![Kubernetes](https://img.shields.io/badge/Kubernetes-KEDA-326CE5?logo=kubernetes&logoColor=white)
![License](https://img.shields.io/badge/license-MIT-green)

---

## Why this exists

"Call an LLM in a loop with tools" is easy. Making that loop **survive the death of the machine running it** is not. Agent tasks run for minutes to hours, call tools with irreversible side effects, and may wait days for human approval. A naive agent that crashes and restarts either loses its progress or, worse, repeats an action: two pull requests, two emails, two charges.

This project builds the reliability layer _underneath_ an agent. The agent itself (a simple repo-maintenance bot) is a deliberately small demo workload. The runtime is the point.

## The guarantee

> If a worker is killed at any moment, another worker resumes the run from its last durable step, and every side-effecting tool call takes effect **at most once**.

This is verified, not assumed: a Kubernetes chaos test submits many concurrent runs, kills random worker pods throughout, and asserts that every run reaches a terminal state with zero duplicated side effects.

## How it works

```
                 ┌────────────┐  SubmitRun / Approve / Cancel
   client ─gRPC─▶│ Control    │──────────────┐
                 │ Plane      │              ▼
                 └─────┬──────┘      ┌──────────────┐
                       │ reads       │ run-commands │  (Kafka, key = run_id)
                       ▼             └───────┬──────┘
                 ┌────────────┐              ▼
                 │  Postgres  │      ┌─────────────────┐      ┌─────────┐
                 │ projection │      │ Worker pool     │◀────▶│  Redis  │
                 └────────────┘      │ (KEDA-scaled)   │      │ fencing │
                       ▲             └──────┬──────────┘      └─────────┘
                       │                    │ journal every step
                       │ sole writer        ▼
                ┌──────┴──────┐      ┌─────────────┐
                │  Projector  │◀──── │ run-events  │  (Kafka: source of truth)
                └─────────────┘      └─────────────┘
```

Four ideas carry the design:

1. **Event sourcing.** Every step (`RunStarted`, `LLMResponded`, `ToolInvoked`, `ToolResulted`, `AwaitingApproval`, …) is appended to a Kafka log _before_ the worker acts on it. A run's state is a pure fold over its events, so any worker can rebuild it with no surviving memory.
2. **Idempotent tool execution.** Before a side-effecting tool runs, the worker atomically _claims_ an idempotency key in Redis (`SET NX`); the result is recorded after. A crash mid-call leaves a claimed-but-unresolved key that a recovering worker must reconcile with the external system rather than blindly retry. If Redis is unreachable, the system **fails closed**: it never executes unchecked.
3. **Single source of truth.** Kafka is authoritative. Workers never write to Postgres; a single projector consumes the log and maintains a queryable, rebuildable projection (deduplicated by `UNIQUE(run_id, sequence_number)`), which avoids the dual-write problem without a transactional outbox.
4. **Partition by `run_id`.** Every topic keys on `run_id`, so one run's events stay ordered and are never split across workers mid-run. Rebalancing and crash recovery are the same code path: replay, then resume.

## Key design decisions

| Decision               | Choice                                                         | Why                                                    |
| ---------------------- | -------------------------------------------------------------- | ------------------------------------------------------ |
| Source of truth        | Kafka `run-events` (infinite retention)                        | Ordered, durable, replayable; Postgres is derived      |
| Dual-write problem     | Workers write only to Kafka; projector is sole Postgres writer | No outbox needed; dedup via unique constraint          |
| Idempotency key        | `idem:{run_id}:{step}:{tool_use_id}`                           | Distinguishes two identical tool calls in one LLM turn |
| Claim ordering         | Claim key **before** executing the tool                        | Writing only after success leaves crashes invisible    |
| Redis failure          | Fail closed, retry, never execute unchecked                    | Failing open defeats the purpose                       |
| Unresolved stale claim | Reconcile with the external system, or fail for manual review  | Idempotency alone can't know if an effect landed       |
| Cancellation           | Cooperative, checked between steps                             | Never interrupts an in-flight side effect              |
| Approval gates         | Worker publishes `AwaitingApproval` and releases the run       | No held threads/connections while a human decides      |

Design specs live in [`.claude/specs/`](.claude/specs) and phase-by-phase decision logs in [`docs/decisions/`](docs/decisions).

## Tech stack

| Concern                        | Technology                                     |
| ------------------------------ | ---------------------------------------------- |
| Language                       | Go                                             |
| Event log & work queue         | Apache Kafka (`segmentio/kafka-go`)            |
| Idempotency / leases / budgets | Redis (`go-redis/v9`)                          |
| Queryable projection           | PostgreSQL (`pgx`)                             |
| Control plane API              | gRPC + Protocol Buffers                        |
| Orchestration & autoscaling    | Kubernetes (`kind`) + KEDA (Kafka-lag scaling) |
| Observability                  | Prometheus + Grafana                           |
| Agent LLM                      | Anthropic Messages API                         |

## Quick start

**Prerequisites:** Go, Docker, and an `ANTHROPIC_API_KEY`.

```bash
# 1. Start Kafka, Redis, Postgres
docker compose up -d

# 2. Verify connectivity
go run ./scripts/connectivity

# 3. Start the projector (the only Postgres writer)
go run ./cmd/projector

# 4. Run a worker: journals every step, prints its run_id
ANTHROPIC_API_KEY=... go run ./cmd/worker

# 5. Reconstruct the run purely from its events
go run ./scripts/replay --run-id <uuid> --source kafka
```

### Try a crash

```bash
# Worker exits abruptly after journaling event #5
DAE_CRASH_AFTER_SEQ=5 ANTHROPIC_API_KEY=... go run ./cmd/worker

# State is rebuilt from the log alone: no memory of the dead process is needed
go run ./scripts/replay --run-id <uuid> --source kafka
```

### Run the full stack on Kubernetes

```bash
# Local cluster with KEDA autoscaling on Kafka consumer lag
kind create cluster
kubectl apply -f deploy/k8s/

# Submit a run through the gRPC control plane
go run ./cmd/controlplane
```

The control plane exposes `SubmitRun`, `GetRunStatus`, `CancelRun`, and `ApproveRun`. Worker pods scale up under backlog and back down when the queue drains; Prometheus scrapes `/metrics` and Grafana shows throughput, latency percentiles, and duplicate-call counts.

### Chaos test

```bash
# 100 concurrent runs while randomly killing worker pods every 10s
go run ./scripts/chaos --runs 100 --kill-interval 10s
```

The harness then verifies that every run reached a terminal state, that zero side effects were duplicated, and reports the measured resume-time distribution after each kill.

## Testing

```bash
go test ./...                       # unit tests, no infrastructure needed
go test -tags=integration ./...     # Kafka/Postgres integration tests (needs docker compose up)
```

Table-driven tests cover event handling and idempotency state transitions. A build-time check enforces the architectural rule that workers never touch Postgres:

```bash
go list -deps ./cmd/worker | grep jackc/pgx   # must print nothing
```

## Project layout

```
cmd/
  worker/        journaled agent loop; Kafka consumer
  projector/     consumes run-events; sole writer to Postgres
  controlplane/  gRPC server
internal/
  agent/         LLM-call → tool-decision loop, journals before acting
  events/        event envelope + typed payloads
  kafka/         producers, topic constants, partitioning
  replay/        pure fold: events → run state
  store/         Postgres access (projector-only writes)
  tools/         tool implementations (mocked, then real)
  idempotency/   Redis claim/fence logic
scripts/         connectivity check, replay CLI, chaos harness
deploy/          Dockerfiles, K8s manifests, KEDA config
```

## Demo workload

A repo-maintenance agent exercises the runtime end to end: it clones a repo, runs its tests, generates and applies a fix via the LLM, and opens exactly one pull request. Killing its worker at any point mid-task never produces a duplicate PR.

## What this project demonstrates

- Event sourcing and deterministic replay of long-running workflows
- Exactly-once _effects_ on top of at-least-once delivery
- Distributed coordination: consumer groups, partitioning, rebalancing, leases, fencing
- Failure-mode analysis (crash windows, fail-closed behavior, split-brain, dual writes)
- Operating the system: containers, Kubernetes, autoscaling on queue lag, metrics, and chaos testing

## License

[MIT](LICENSE)
