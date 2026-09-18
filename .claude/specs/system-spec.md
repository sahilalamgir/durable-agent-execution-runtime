# Durable Agent Execution Runtime — Spec

## Problem Statement

Sahil's resume currently reads as an AI-wrapper product builder (GenText, RemoteCasa, GenSheets, AlzGuard) with no demonstrated systems-level depth — no concurrency work, no correctness-under-failure story, no operational experience running a distributed system. "Called an LLM in a loop with tools" has become a commodity skill in SWE recruiting; it does not differentiate a candidate for competitive Winter/Summer 2027 SWE internships. What's missing is proof that he can build the infrastructure layer _underneath_ an agent, not just the agent itself — specifically, a system where a long-running, multi-step, tool-calling agent task survives the crash of the machine running it and resumes correctly, without re-executing real-world side effects. Without this, he continues to compete on a commoditized narrative during recruiting cycles where that narrative is increasingly table stakes rather than a differentiator.

## Functional Requirements

FR-1: The system must provide a local development environment (via `docker-compose`) that brings up Kafka, Redis, and Postgres with a single command, networked together and reachable from a Go process on the host.

FR-2: The system must implement a single-process agent loop (LLM call → tool selection → tool invocation → repeat until a terminal condition) against a mocked tool, with no durability, as a baseline before durability is added.

FR-3: The system must record every step of a run as an immutable, ordered event (`RunStarted`, `LLMResponded`, `ToolInvoked`, `ToolResulted`, `AwaitingApproval`, `RunApproved`, `RunCompleted`, `RunFailed`, `RunCancelled`) to an append-only log, and must be able to reconstruct a run's current state purely by replaying its events — with no dependency on any in-memory state surviving from the original execution.

FR-4: The system must persist events durably in two places that agree with each other: Kafka (`run-events` topic, the ordered log) and Postgres (the `events` table, queryable by arbitrary filters) — see API Contracts for the write-ordering rule that keeps these consistent.

FR-5: Before executing any tool call that has a real-world side effect, the system must check a Redis-based idempotency/fencing key derived from `run_id + step + tool_name + canonicalized_args`. If a result is already recorded under that key, the tool must NOT be re-invoked — the recorded result must be returned instead.

FR-6: The system must run multiple worker processes as a Kafka consumer group against the `run-commands` topic, such that each run is processed by exactly one worker at a time and no two workers concurrently execute steps for the same run.

FR-7: The system must support human-in-the-loop approval gates: a worker awaiting approval must publish an `AwaitingApproval` event and fully release its claim on the run (no held thread, no open connection) rather than blocking. On approval, the run must be re-enqueued and resumable by any available worker via replay, without re-executing the step that triggered the gate.

FR-8: The system must expose a gRPC control-plane API with `SubmitRun`, `GetRunStatus`, `CancelRun`, and `ApproveRun` RPCs (see API Contracts).

FR-9: All services must be containerized (Dockerfiles) and deployable to a local Kubernetes cluster (`kind`), with a Deployment and Service for the worker pool.

FR-10: The system must autoscale the worker pool using KEDA, scaling on Kafka consumer-group lag for the `run-commands` topic (not CPU), such that pod count increases under backlog and decreases once the queue drains.

FR-11: The system must expose Prometheus metrics (`runs_started_total`, `step_latency_seconds` histogram, `tool_call_duplicates_total`, `active_runs_gauge`) on a `/metrics` endpoint, and must have a Grafana dashboard visualizing throughput, latency percentiles, and duplicate-call count over time.

FR-12: The system must include a chaos-test harness that submits N concurrent runs, kills random worker pods on a timed interval throughout the run, and afterward verifies: (a) every run reached a terminal state, (b) zero duplicated real-world tool side effects occurred, (c) resume-time-after-kill is measured and recorded from real logs/metrics.

FR-13: The system must support a demo workload (repo-maintenance agent): given a repo URL and issue description, clone the repo, run its test suite, generate and apply a fix via a real LLM API, and open a pull request — with idempotency guarantees such that killing its worker mid-task never results in a duplicate PR.

FR-14 (stretch, cuttable): Tool execution involving shell commands or file edits must be sandboxed via gVisor such that a misbehaving action cannot reach the network or escape its sandbox.

## API Contracts

### Design decision (gap-filling assumption)

_The brief specifies that every event is "written to Postgres and published to Kafka" but doesn't state which happens first or how the two stay consistent under partial failure (the classic dual-write problem). This spec resolves it as follows, since it's load-bearing for Phase 2: Kafka's `run-events` topic is the single source of truth. Workers publish there first. A dedicated, singly-instanced projector service consumes `run-events` and inserts into Postgres, relying on the `UNIQUE(run_id, sequence_number)` constraint as a natural dedup guard against re-delivery. Postgres is a queryable projection, never written to directly by workers. This avoids needing a full transactional outbox, since Kafka already gives you the durable log._

### gRPC Control Plane

```protobuf
service RunControlPlane {
  rpc SubmitRun(SubmitRunRequest) returns (SubmitRunResponse);
  rpc GetRunStatus(GetRunStatusRequest) returns (GetRunStatusResponse);
  rpc CancelRun(CancelRunRequest) returns (CancelRunResponse);
  rpc ApproveRun(ApproveRunRequest) returns (ApproveRunResponse);
}
```

#### `SubmitRun`

**Input:**

```json
{
  "tenant_id": "string — identifies the caller for budget tracking",
  "workload_type": "string — e.g. 'repo-maintenance-agent'",
  "input": "bytes — JSON-encoded, workload-specific (e.g. repo URL + issue text)",
  "max_steps": "int32 — hard cap on steps before forced RunFailed"
}
```

**Output:**

```json
{ "run_id": "string (UUID)", "accepted": "bool" }
```

**Notes:** Returns immediately after the `RunStarted` command is durably published to `run-commands` — does not wait for a worker to pick it up.

#### `GetRunStatus`

**Input:**

```json
{ "run_id": "string (UUID)" }
```

**Output:**

```json
{
  "run_id": "string",
  "status": "enum: PENDING | RUNNING | AWAITING_APPROVAL | COMPLETED | FAILED | CANCELLED",
  "current_step": "int32",
  "last_event_at": "timestamp (RFC3339)"
}
```

**Notes:** Reads from the Postgres projection, not from live worker state. Returns `NOT_FOUND` (gRPC status code) if `run_id` doesn't exist.

#### `CancelRun`

**Input:**

```json
{ "run_id": "string (UUID)", "reason": "string, optional" }
```

**Output:**

```json
{ "accepted": "bool" }
```

**Notes:** Publishes a `cancel` command to `run-commands`; cancellation is cooperative — the worker checks for it between steps, not mid-tool-call (see EC-9).

#### `ApproveRun`

**Input:**

```json
{
  "run_id": "string (UUID)",
  "approved_by": "string — identifier of the approving human",
  "decision": "enum: APPROVE | REJECT",
  "notes": "string, optional"
}
```

**Output:**

```json
{ "accepted": "bool" }
```

**Notes:** Only valid when run status is `AWAITING_APPROVAL`; otherwise returns `FAILED_PRECONDITION`.

### Kafka Topic Contracts

| Topic               | Producer      | Consumer                                | Partition key |
| ------------------- | ------------- | --------------------------------------- | ------------- |
| `run-commands`      | control plane | workers (consumer group `workers`)      | `run_id`      |
| `run-events`        | workers       | Postgres projector, Prometheus exporter | `run_id`      |
| `approval-requests` | workers       | approval UI / CLI                       | `run_id`      |

_Partitioning by `run_id` on every topic is what guarantees a single run's events/commands are always processed in order and never split across workers mid-run._

`run-events` topic config (decided in Phase 2, load-bearing for replay — don't change without updating this spec): 6 partitions, replication factor 1, `cleanup.policy=delete`, `retention.ms=-1` (infinite retention — this topic is the source of truth, not a transient queue, so it must never expire data via the default 7-day retention).

**`run-commands` message:**

```json
{
  "command": "enum: start | cancel | resume",
  "run_id": "string (UUID)",
  "tenant_id": "string",
  "issued_at": "timestamp",
  "payload": "object — command-specific (e.g. SubmitRun input for 'start')"
}
```

**`run-events` message (the core event envelope):**

```json
{
  "event_id": "string (UUID) — unique per event, for dedup",
  "run_id": "string (UUID)",
  "tenant_id": "string",
  "sequence_number": "int — monotonic per run_id, starts at 0",
  "event_type": "enum: RunStarted | LLMResponded | ToolInvoked | ToolResulted | AwaitingApproval | RunApproved | RunCompleted | RunFailed | RunCancelled",
  "payload": "object — shape depends on event_type, see below",
  "occurred_at": "timestamp — assigned by the worker, never the client; UTC, truncated to microsecond precision so it round-trips unchanged through Postgres's TIMESTAMPTZ",
  "idempotency_key": "string | null — present only on ToolInvoked events with side effects"
}
```

Per-`event_type` payload shapes:

- `RunStarted`: `{ workload_type, input, max_steps }`
- `LLMResponded`: `{ step, stop_reason, content, is_terminal }` — _amended in Phase 2: the original `{ chosen_tool, tool_args }` shape assumed one tool call per LLM response, but Anthropic's API allows a single response to contain several `tool_use` blocks in one turn. `content` is the response's content-block array, encoded verbatim as returned by the Messages API (text and tool_use blocks, including each tool_use's `id`), so replay can rebuild the exact assistant turn and match each later `tool_result` to the right `tool_use_id`._
- `ToolInvoked`: `{ step, tool_use_id, tool_name, tool_args, idempotency_key, has_side_effect }` — _amended in Phase 2 to add `tool_use_id`, needed to match this invocation back to the specific `tool_use` block in `LLMResponded.content` (a single step's response can request more than one tool)._
- `ToolResulted`: `{ step, tool_use_id, tool_name, result, status: "success"|"error", was_replayed_from_cache }` — _amended in Phase 2 to add `tool_use_id`, for the same reason as `ToolInvoked`._
- `AwaitingApproval`: `{ step, reason, approval_payload }`
- `RunApproved`: `{ step, approved_by, decision, notes }`
- `RunCompleted`: `{ final_output, total_steps }`
- `RunFailed`: `{ step, error_class, error_message, retryable }`
- `RunCancelled`: `{ step, cancelled_by, reason }`

**`approval-requests` message:**

```json
{
  "run_id": "string",
  "step": "int",
  "approval_payload": "object",
  "requested_at": "timestamp"
}
```

### Postgres Schema

```sql
CREATE TABLE events (
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

CREATE TABLE runs (
  run_id UUID PRIMARY KEY,
  tenant_id TEXT NOT NULL,
  workload_type TEXT NOT NULL,
  status TEXT NOT NULL,
  current_step INT NOT NULL DEFAULT 0,
  created_at TIMESTAMPTZ NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL,
  last_event_id UUID
);
```

`runs` is a materialized projection kept current by the same projector that writes `events`, so `GetRunStatus` doesn't require a full replay on every call.

### Redis Key Contracts

| Key pattern                                     | Purpose                            | Operation                                                                |
| ----------------------------------------------- | ---------------------------------- | ------------------------------------------------------------------------ |
| `idem:{run_id}:{step}:{sha256(tool_name+args)}` | Idempotency fencing                | `SET key value NX EX <ttl>`; value = JSON-encoded `ToolResulted` payload |
| `lease:{run_id}`                                | Worker ownership / crash detection | `SET key worker_id NX EX <lease_ttl>`, renewed by heartbeat              |
| `budget:{tenant_id}:{hour_bucket}`              | Token budget enforcement           | Atomic check-and-increment via Lua script                                |

_Assumption: idempotency key TTL defaults to 24h (must exceed the longest plausible run duration, including time spent `AWAITING_APPROVAL`). This should be a configurable constant, not hardcoded, since approval waits could legitimately exceed 24h._

_Flagged during Phase 2 (unresolved, Phase 3 must decide before implementing fencing): `run_id:step:sha256(tool_name+args)` collides when a single step's `LLMResponded` requests the same tool with identical args twice in one turn (two identical `tool_use` blocks) — both would hash to the same key even though they're distinct invocations with distinct `tool_use_id`s. Phase 3's spec should decide whether to fold `tool_use_id` (or a block index) into the key._

## Constraints

- **Technical:** Go for all services. Docker + `docker-compose` for local infra. Kafka via `segmentio/kafka-go`. Redis for fencing/leases/budgets. Postgres for the queryable projection. gRPC (`protoc`-generated) for the control plane. Kubernetes via `kind` for local cluster testing; KEDA for lag-based autoscaling. `prometheus/client_golang` + Grafana for observability. gVisor is optional/stretch and explicitly the first thing cut if behind schedule.
- **Performance & scale:** No hard SLA was specified. _Assumption: the chaos test (FR-12) targets a scale that's crash-recoverable and observable on a single laptop's `kind` cluster — tens of concurrent runs (e.g., N=50–100), not production-scale load. The actual numbers reported on the resume should come from what was really measured, not a target set in advance._
- **Timeline/resourcing:** Solo developer, phased build (~27–37 focused days across 10 phases per the original phase breakdown, Phase 9 allowed to overlap earlier phases). AI assistance (Claude Code) is used for boilerplate, scaffolding, and concept tutoring; the developer personally owns and must be able to explain unaided: idempotency/replay logic, every architectural tradeoff, and chaos-test result verification. This is a process constraint that affects what "done" means for FR-3, FR-5, and FR-12 specifically — passing the acceptance test isn't sufficient if the developer can't trace through the logic by hand.
- **Security/compliance:** None formal. gVisor sandboxing (FR-14) is the only security-adjacent requirement and is explicitly stretch/cuttable.

## Edge Cases & Error Handling

EC-1: Worker crashes _after_ a tool's real-world side effect fires but _before_ the `ToolResulted` event and idempotency key are durably written → On resume, the fencing key is absent, so the system cannot know the side effect already happened. This is the one gap idempotency alone can't close. _Mitigation to design for: write the idempotency key (as a "claimed" placeholder) immediately before invoking the tool, and only fill in the result after — so a crash mid-flight leaves a claimed-but-unresolved key that a recovering worker must explicitly reconcile (e.g., check the external system's state, or flag for manual review) rather than blindly retry._

EC-2: Redis is unreachable when a worker checks the idempotency key before a side-effecting tool call → Fail **closed**: do not execute the tool; retry the check with backoff or fail the step as `retryable: true`. Failing open would defeat the entire purpose of the project.

EC-3: Kafka consumer-group rebalance occurs while a worker is mid-step on a run → The reassigned partition's new owner must not assume the run is untouched; it must replay from the last committed event before resuming, exactly as if recovering from a crash. Rebalance and crash-recovery should be the same code path, not two.

EC-4: `SubmitRun` is called twice with the same logical request (e.g., client retry after a timeout) → No dedup key is defined in the current contract; this produces two distinct `run_id`s and two real executions. _Flagging as a real gap: if this matters for the demo agent, `SubmitRun` should accept an optional client-supplied idempotency token._

EC-5: A run sits in `AWAITING_APPROVAL` and is never approved or rejected → No automatic timeout in the MVP; the run waits indefinitely and is visible as such via `GetRunStatus`. _Assumption, stated because the brief doesn't specify: a TTL-based auto-reject is reasonable future work, not required for the acceptance bar._

EC-6: The LLM call itself fails or returns malformed/unparseable tool-call arguments → Recorded as a `RunFailed` event with `error_class` set (not silently retried), unless the failure is classified `retryable` (e.g., rate limit, transient network), in which case the step is retried with backoff up to a configurable cap before failing the run.

EC-7: `CancelRun` is called while a worker is mid-tool-call → Cancellation is checked between steps only, never interrupts an in-flight tool invocation (which could itself have a side effect). The run finishes its current step, then observes the cancel command and emits `RunCancelled`.

EC-8: `GetRunStatus` is called with a `run_id` that never existed → Returns gRPC `NOT_FOUND`, not an empty/default status object.

EC-9: The same tool is legitimately meant to run twice with different arguments in the same run (e.g., two different PRs in one run) vs. a true duplicate retry with identical arguments → Distinguished because the idempotency key includes `step` and `tool_args`, not just `tool_name` — different args at the same step, or the same args at a different step, produce different keys and are not treated as duplicates.

EC-10: KEDA scales workers to zero during a lull, then a burst of `SubmitRun` calls arrives → Commands queue durably in `run-commands` (Kafka retains them) until KEDA observes the lag and scales pods up; no commands are dropped, but there's a cold-start latency window that should show up in the Grafana latency panel, not be hidden.

EC-11: The chaos-test script kills the worker pod that's also acting as the Postgres projector (if not run as a separate deployment) → Must not lose events: the projector's consumer offset commits only after a successful Postgres insert, so a killed projector simply resumes from its last committed offset with no event loss, at the cost of at-least-once delivery (handled by the `UNIQUE(run_id, sequence_number)` constraint).

EC-12: Two workers briefly believe they both own the same run's lease (e.g., due to a lease TTL expiring right as the original owner was still alive but slow) → The Redis lease key's `SET NX` semantics mean only one worker's renewal succeeds; the loser must stop processing immediately upon failing to renew, even mid-step, to avoid split-brain execution. This is the specific race the fencing mechanism (not just the lease) is there to catch even if the lease check itself has a gap.

## Acceptance Criteria

- [ ] AC-1: `docker-compose up` brings up Kafka, Redis, and Postgres; a Go script can produce/consume a test Kafka message and set/get a Redis key. _(FR-1)_
- [ ] AC-2: The single-process agent loop runs a fake task to completion against a mocked tool, printing each step, with no durability layer involved. _(FR-2)_
- [ ] AC-3: Killing the process mid-run and restarting it, then calling replay, reconstructs the exact correct run state from stored events alone — no live memory used. _(FR-3, FR-4)_
- [ ] AC-4: Manually replaying an already-completed step a second time does not re-invoke the underlying tool; a counter or log line proves the recorded result was returned instead. _(FR-5)_
- [ ] AC-5: With 3 worker processes running and 10 runs submitted, work is distributed across all three workers and no run is processed twice concurrently. _(FR-6)_
- [ ] AC-6: A run that hits an approval gate releases its worker (verified: no thread/connection held), and after `ApproveRun` is called, any available worker resumes it via replay without re-executing the gated step. _(FR-7)_
- [ ] AC-7: A CLI client can call `SubmitRun`, `GetRunStatus`, and `CancelRun` purely via gRPC and get correct responses, including `NOT_FOUND` for an unknown `run_id`. _(FR-8, EC-8)_
- [ ] AC-8: All services run as containers on a local `kind` cluster via a Deployment + Service. _(FR-9)_
- [ ] AC-9: Submitting a burst of runs visibly increases worker pod count via KEDA, and pod count drops again once the queue drains — observed in `kubectl get pods` and/or the Grafana dashboard. _(FR-10, EC-10)_
- [ ] AC-10: A live Grafana dashboard shows throughput, latency percentiles, and a duplicate-tool-call counter while load runs. _(FR-11)_
- [ ] AC-11: Running the chaos-test harness against N concurrent runs while randomly killing worker pods results in: every run reaching a terminal state, zero duplicated real-world side effects, and a recorded (not estimated) resume-time distribution. _(FR-12, EC-3, EC-11, EC-12)_
- [ ] AC-12: The repo-maintenance demo agent clones a repo, runs tests, applies a fix via a real LLM call, and opens exactly one PR end-to-end; killing its worker mid-task and letting it resume produces zero duplicate PRs. _(FR-13, EC-1)_
- [ ] AC-13: (Stretch) Tool execution runs inside a gVisor sandbox and is verified unable to reach the network or escape the sandbox boundary. _(FR-14)_
- [ ] AC-14: Redis becoming unreachable during an idempotency check causes the step to fail/retry rather than executing the tool unchecked. _(EC-2)_
- [ ] AC-15: A `CancelRun` issued mid-tool-call does not interrupt the in-flight call; the run cancels cleanly after that step completes. _(EC-7)_
