# Phase 2: Event Journal (Kafka + Postgres) — Spec

_Scope: Phase 2 only. Background contracts live in `.claude/specs/system-spec.md` (FR-3, FR-4, the dual-write design decision, EC-11, AC-3). Where this phase needs a contract the system spec doesn't have yet, it's listed under **API Contracts → System-spec amendments**. Per CLAUDE.md, those amendments get applied to `system-spec.md` **before** implementation starts._

## Problem Statement

The Phase 1 agent loop keeps everything in memory: the conversation history, the step counter, which tools already ran. If the process dies at step 4 of 6, the run is simply gone. Nothing records that `clone_repo` and `apply_fix` already ran, and nothing can tell a future worker where to pick up. Every later guarantee depends on the run's history outliving the process: idempotent tools (Phase 3), crash recovery across a worker pool (Phase 4), and the chaos test (Phase 8). Without a durable, ordered, replayable record of every step, there's nothing to recover *from*. Phase 2 builds that record and proves, by actually killing the process, that a run's exact state can be rebuilt from the record alone.

## Functional Requirements

### Events and serialization

FR-1: The system must define a Go `EventType` typed string enum covering all 9 event types in the system spec. It must also define one payload struct per type, with JSON tags that match the spec's (amended) field names exactly.

FR-2: The system must define one event envelope struct matching the `run-events` message contract. The payload must be carried as `json.RawMessage` in the envelope and decoded into its typed struct by a single `DecodePayload` function that switches on `event_type`.

FR-3: Decoding must reject, with a typed error: an unknown `event_type`, a missing required envelope field (`event_id`, `run_id`, `event_type`, `occurred_at`), and a payload that doesn't unmarshal into its type's struct. Unknown extra JSON fields are tolerated, not rejected.

FR-4: `occurred_at` must be set by the worker at event creation, in UTC, truncated to microsecond precision. This matches Postgres `TIMESTAMPTZ` precision, so a round trip through Postgres doesn't change the value (see EC-12).

### Worker publishing (write-ahead)

FR-5: The worker must publish each event to the `run-events` topic with message key = `run_id` and message value = the JSON-encoded envelope. It must wait for a broker acknowledgement (`RequiredAcks = RequireAll`, synchronous write) before it takes the action the event describes or follows.

FR-6: The worker must assign `sequence_number` itself, starting at 0 for `RunStarted` and increasing by exactly 1 per event within a run, with no gaps.

FR-7: The worker's event order for a run must be: `RunStarted` → (`LLMResponded` → for each `tool_use` block in that response, in content order: `ToolInvoked` → `ToolResulted`)\* → exactly one terminal event (`RunCompleted` or `RunFailed`).

FR-8: `ToolInvoked` must be acknowledged by Kafka **before** `Tool.Execute` is called. `LLMResponded` must be acknowledged before any tool from that response runs. `ToolResulted` must be acknowledged before the next LLM call. If any publish fails, the worker must not take the next action (see EC-1, EC-2).

FR-9: The worker must build every event for a `tool_use` block, including a synthetic `ToolInvoked`/`ToolResulted` pair with `status: "error"` for an unknown tool name or malformed arguments. That way every `tool_use_id` in an `LLMResponded` has exactly one matching `ToolResulted`.

FR-10: The worker must map Phase 1 loop outcomes to terminal events like this:
- a natural stop → `RunCompleted`
- max steps exceeded → `RunFailed{error_class: "max_steps_exceeded", retryable: false}`
- `stop_reason = max_tokens` → `RunFailed{error_class: "llm_output_truncated", retryable: false}` _(assumption, see Open decision D-3)_
- `stop_reason = tool_use` with no `tool_use` blocks → `RunFailed{error_class: "malformed_llm_response", retryable: false}`
- an LLM API error after the SDK's built-in retries → `RunFailed{error_class: "llm_call_failed", retryable: <true for 429/5xx/network, false otherwise>}`

FR-11: On SIGINT/SIGTERM, the worker must stop without publishing any terminal event. A shutdown is a crash from the run's point of view, and the run must stay resumable (see EC-9).

### State and replay

FR-12: The system must provide a pure fold, `replay.Apply(state RunState, ev events.Envelope) (RunState, error)`, and a wrapper, `replay.Fold(events []events.Envelope) (RunState, error)`. Together they compute a run's full state from its events with no I/O, no LLM calls, no tool calls, and no clock reads. `Apply` must not mutate its input state (no shared backing arrays between input and output slices).

FR-13: The live worker loop must hold run state **only** as a `RunState` advanced through `replay.Apply` on each acknowledged event. The conversation history sent to the LLM must come from `RunState.Messages()`. No separate in-memory copy of history may exist. This way live execution and replay share one code path by construction. _(Design decision D-1.)_

FR-14: `RunState` must expose: `RunID`, `TenantID`, `Status`, `CurrentStep`, `MaxSteps`, `NextSequence`, `LastEventID`, `LastEventAt`, `Messages()`, `PendingToolCalls`, `InFlightTool`, `NextAction`, `FinalOutput`, and `Failure`. The exact shape is under API Contracts.

FR-15: `RunState.NextAction` must be computed by the fold and must be one of: `CALL_LLM`, `EXECUTE_TOOL` (the next unstarted tool in the current step), `RESOLVE_IN_FLIGHT_TOOL` (a `ToolInvoked` with no `ToolResulted`, which Phase 3 decides how to handle), `FAIL_MAX_STEPS`, or `NONE` (the run is terminal).

FR-16: The fold must enforce these invariants and return a typed error when one breaks: the first event is `RunStarted` with `sequence_number` 0; sequence numbers have no gaps; every event has the same `run_id`; no event comes after a terminal event; every `ToolInvoked`/`ToolResulted` references a `tool_use_id` from the current step's `LLMResponded`; a `ToolResulted` must follow its `ToolInvoked`; there's at most one `ToolInvoked` per `tool_use_id`. If an event's `sequence_number` and `event_id` both match an event already applied, it's a redelivery and is skipped silently. A repeated `sequence_number` with a **different** `event_id` is an error.

FR-17: `RunState.Digest()` must return a hex SHA-256 of a canonical JSON encoding of the state (sorted keys, messages re-encoded through the SDK types). Two states that are semantically the same must get the same digest, whether the events came from Kafka or from Postgres.

FR-18: After each acknowledged event, the worker must print one line: `seq=<n> event=<EventType> step=<k> next_action=<NextAction> state_digest=<hex>`. Before any other output, it must also print `run_id=<uuid>`.

### Event sources

FR-19: The system must provide a Kafka event source that reads a run's events by scanning the one partition its `run_id` hashes to. The scan runs from the partition's first offset up to the high-watermark captured when the replay starts. It keeps only messages whose key equals `run_id`, in offset order. This is the **authoritative** replay source. _(Design decision D-2.)_

FR-20: The system must provide a Postgres event source that reads `SELECT … FROM events WHERE run_id = $1 ORDER BY sequence_number`. It's for inspection and cross-checking only. Worker logic must never use it to decide what to do next.

FR-21: The system must provide `go run ./scripts/replay --run-id <uuid> [--source kafka|postgres] [--up-to-seq N]`. It prints the reconstructed `RunState` fields (except the full message bodies, which it summarizes as role + block types) and `state_digest`. It exits 2 if the run isn't found and 1 on an invariant error.

### Projector

FR-22: The system must provide a separate process, `cmd/projector`, that consumes `run-events` as consumer group `projector` and is the **only** code path that writes to Postgres.

FR-23: For each message, the projector must, strictly one message at a time: (1) decode it; (2) in **one** Postgres transaction, `INSERT INTO events … ON CONFLICT DO NOTHING` and, only if that inserted a row, upsert the `runs` row for that event; (3) commit the transaction; (4) commit the Kafka offset for that message. It must use `FetchMessage` + `CommitMessages`, never `ReadMessage`, which auto-commits before the insert.

FR-24: The projector must apply the embedded schema migration at startup, idempotently (`CREATE TABLE IF NOT EXISTS …`), before it consumes anything.

FR-25: The projector must update `runs` using this mapping: `RunStarted` inserts the row (`status=RUNNING`, `current_step=0`, `workload_type` from payload, `created_at = occurred_at`). `LLMResponded`/`ToolInvoked`/`ToolResulted`/`RunApproved` → `RUNNING`. `AwaitingApproval` → `AWAITING_APPROVAL`. `RunCompleted` → `COMPLETED`. `RunFailed` → `FAILED`. `RunCancelled` → `CANCELLED`. Every event sets `updated_at = occurred_at` and `last_event_id = event_id`, and sets `current_step = payload.step` when the payload has one.

FR-26: The worker binary must not depend on the Postgres driver at all. `go list -deps ./cmd/worker` must not include `github.com/jackc/pgx`.

### Infrastructure

FR-27: Worker and projector must both call a shared, idempotent `EnsureTopics` at startup. It creates `run-events` with `6` partitions, replication factor 1, `cleanup.policy=delete`, and `retention.ms=-1`. It then **verifies** the existing topic's partition count equals the constant and fails fast if it doesn't.

FR-28: `docker-compose.yml` must add named volumes for Kafka's log dir and Postgres's data dir, so events survive `docker compose down` / `up`. `docker compose down -v` stays the full reset.

FR-29: The mock tools must accept an optional delay, set with env var `DAE_MOCK_TOOL_DELAY` (a Go duration, default `0`). `Execute` sleeps that long, respecting `ctx`, so a manual `kill -9` can reliably land between `ToolInvoked` and `ToolResulted`.

FR-30: The worker must accept an optional `DAE_CRASH_AFTER_SEQ=N` env var. It calls `os.Exit(137)` immediately after event N is acknowledged and applied, before printing that event's log line. This makes it possible to test each crash point repeatably. The projector must accept `DAE_PROJECTOR_CRASH_BEFORE_OFFSET_COMMIT=N`, which exits right after the Postgres transaction for the N-th consumed message commits and before its Kafka offset commit.

## API Contracts

### Design decisions made by this spec (confirm during review)

- **D-1: Live state goes through the fold.** The worker doesn't keep a Phase 1-style `messages` slice alongside the events. It publishes an event, waits for the ack, applies the event to `RunState`, and asks `RunState` for the history. _Why:_ if live state and replayed state come from two code paths, they can drift apart without anyone noticing, and replay would "work" in tests while producing a different conversation than the one the LLM actually saw. With one path, replay is correct as long as serialization round-trips. _Rejected alternative:_ keep the Phase 1 loop as-is and add a publish call at each step. That's less refactoring, but replay correctness then depends on the event-building code and the loop agreeing forever.
- **D-2: Kafka is the authoritative replay source; Postgres is a cross-check.** _Why:_ the projector can lag. Suppose the last 2 events for a run are acknowledged in Kafka but not yet projected. A worker replaying from Postgres would see an older state, assign `sequence_number`s that are already taken, and write conflicting events. Scanning up to the high-watermark returns every acknowledged event, because an `acks=all` write is only acknowledged once the high-watermark has moved past it. _Cost:_ replay scans a whole partition, so its cost grows with total history in that partition. That's fine at laptop scale. Snapshots or storing Kafka offsets in Postgres are explicitly future work.
- **D-3: `max_tokens` truncation → `RunFailed`** (see FR-10). This matches the Phase 1 decision that a truncated run "should not be treated as a clean completion", and it needs no schema change. _Alternative:_ `RunCompleted` plus a new `truncated: bool` payload field.
- **D-4: Resuming execution is out of scope.** Phase 2 rebuilds state and shows what `NextAction` would be. It doesn't act on it. _Why:_ resuming without Phase 3's Redis fencing would re-run whatever tool was in flight at crash time. That's exactly the duplicate side effect this project exists to prevent. `RESOLVE_IN_FLIGHT_TOOL` is the handoff point to Phase 3.

### System-spec amendments (apply to `system-spec.md` before implementing)

The system spec's `LLMResponded` payload (`{ step, chosen_tool, tool_args, is_terminal }`) can't describe what Phase 1 already does. One LLM response can contain several `tool_use` blocks, and replay has to rebuild the exact assistant message, including `tool_use` IDs, so the rebuilt `tool_result` blocks match. Changes:

| Event          | System spec today                                                   | Amended                                                                                                                                                  |
| -------------- | ------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `RunStarted`   | `{ workload_type, input, max_steps }`                               | unchanged; `input` for this workload is `{ "prompt": string }`                                                                                           |
| `LLMResponded` | `{ step, chosen_tool, tool_args, is_terminal }`                     | `{ step, stop_reason, content, is_terminal }`: `content` is the response's content-block array, verbatim JSON as returned by the Messages API |
| `ToolInvoked`  | `{ step, tool_name, tool_args, idempotency_key, has_side_effect }`  | adds `tool_use_id`                                                                                                                                       |
| `ToolResulted` | `{ step, tool_name, result, status, was_replayed_from_cache }`      | adds `tool_use_id`                                                                                                                                       |

Also add to the system spec: `run-events` topic config (6 partitions, `retention.ms=-1`, `cleanup.policy=delete`) and a note that `occurred_at` has microsecond precision.

_Phase 3 note (not decided here):_ the system spec's idempotency key, `run_id:step:sha256(tool_name+args)`, gives the same key for two identical `tool_use` blocks in one step. Phase 3 should decide whether to add `tool_use_id` or a block index to the key.

### Event envelope (`internal/events`)

```go
type EventType string

const (
	EventRunStarted       EventType = "RunStarted"
	EventLLMResponded     EventType = "LLMResponded"
	EventToolInvoked      EventType = "ToolInvoked"
	EventToolResulted     EventType = "ToolResulted"
	EventAwaitingApproval EventType = "AwaitingApproval"
	EventRunApproved      EventType = "RunApproved"
	EventRunCompleted     EventType = "RunCompleted"
	EventRunFailed        EventType = "RunFailed"
	EventRunCancelled     EventType = "RunCancelled"
)

type Envelope struct {
	EventID        string          `json:"event_id"`        // UUIDv4
	RunID          string          `json:"run_id"`          // UUIDv4
	TenantID       string          `json:"tenant_id"`       // Phase 2: constant "local-dev"
	SequenceNumber int64           `json:"sequence_number"` // 0-based, gapless per run
	EventType      EventType       `json:"event_type"`
	Payload        json.RawMessage `json:"payload"`
	OccurredAt     time.Time       `json:"occurred_at"`     // UTC, truncated to µs
	IdempotencyKey *string         `json:"idempotency_key"` // always null in Phase 2
}

// Payload is implemented by every per-type payload struct.
type Payload interface{ EventType() EventType }

func NewEnvelope(runID, tenantID string, seq int64, p Payload, now time.Time) (Envelope, error)
func Encode(e Envelope) ([]byte, error)
func Decode(b []byte) (Envelope, error)          // validates required envelope fields + known type
func DecodePayload(e Envelope) (Payload, error)  // switch on e.EventType
```

Payload structs (JSON tags shown as field names):

```go
type RunStartedPayload   struct { WorkloadType string; Input json.RawMessage; MaxSteps int }
type LLMRespondedPayload struct { Step int; StopReason string; Content json.RawMessage; IsTerminal bool }
type ToolInvokedPayload  struct { Step int; ToolUseID, ToolName string; ToolArgs json.RawMessage; IdempotencyKey *string; HasSideEffect bool }
type ToolResultedPayload struct { Step int; ToolUseID, ToolName, Result, Status string; WasReplayedFromCache bool } // Status: "success"|"error"
type AwaitingApprovalPayload struct { Step int; Reason string; ApprovalPayload json.RawMessage }
type RunApprovedPayload  struct { Step int; ApprovedBy, Decision, Notes string }
type RunCompletedPayload struct { FinalOutput string; TotalSteps int }
type RunFailedPayload    struct { Step int; ErrorClass, ErrorMessage string; Retryable bool }
type RunCancelledPayload struct { Step int; CancelledBy, Reason string }
```

**Example `run-events` message** (key = `"7f1c…"`):

```json
{
  "event_id": "c2a9e3de-5f1b-4a0e-9b7a-2f4f8f0d1e11",
  "run_id": "7f1c4b8e-2d3a-4f6b-9c1e-0a2b3c4d5e6f",
  "tenant_id": "local-dev",
  "sequence_number": 1,
  "event_type": "LLMResponded",
  "payload": {
    "step": 1,
    "stop_reason": "tool_use",
    "content": [
      { "type": "text", "text": "I'll clone the repo first." },
      { "type": "tool_use", "id": "toolu_01A", "name": "clone_repo",
        "input": { "repo_url": "https://github.com/example/widgets.git" } }
    ],
    "is_terminal": false
  },
  "occurred_at": "2026-09-17T18:04:05.123456Z",
  "idempotency_key": null
}
```

Canonical event sequence for the Phase 1 happy path (6 LLM steps):

```
seq 0  RunStarted
seq 1  LLMResponded(step 1, tool_use clone_repo)
seq 2  ToolInvoked(step 1, clone_repo)      seq 3  ToolResulted(step 1, clone_repo, success)
seq 4  LLMResponded(step 2, tool_use run_tests)
seq 5  ToolInvoked(step 2, run_tests)       seq 6  ToolResulted(step 2, "2 tests failed…")
…                                            (apply_fix, run_tests, open_pr)
seq 16 LLMResponded(step 6, end_turn)
seq 17 RunCompleted(total_steps 6)
```

### Publisher (`internal/kafka`)

```go
const (
	TopicRunEvents          = "run-events"
	RunEventsPartitions     = 6
	ProjectorConsumerGroup  = "projector"
)

type EventPublisher interface {
	// Publish blocks until the broker acknowledges with acks=all, or returns an error.
	// On error, the event MAY or MAY NOT be in the log (ambiguous write).
	Publish(ctx context.Context, e events.Envelope) error
}

func NewEventPublisher(brokers []string) *kafkaEventPublisher
func EnsureTopics(ctx context.Context, brokers []string) error
func PartitionFor(runID string, numPartitions int) int // the ONLY partitioning function; used by writer and reader
```

Required `kafka.Writer` settings. These are load-bearing, and the kafka-go defaults are unsafe for this use:

| Field                    | Value                                                       | kafka-go default & why it's wrong here                                                       |
| ------------------------ | ----------------------------------------------------------- | -------------------------------------------------------------------------------------------- |
| `RequiredAcks`           | `kafka.RequireAll`                                          | `RequireNone`: fire-and-forget; "published" would mean nothing                               |
| `Async`                  | `false`                                                     | `false` (keep it)                                                                             |
| `BatchSize`              | `1`                                                         | `100` with a `1s` `BatchTimeout`: every sync single-event write would stall ~1s              |
| `Balancer`               | `kafka.BalancerFunc` delegating to `PartitionFor`           | round-robin if unset; would split a run across partitions                                    |
| `AllowAutoTopicCreation` | `false`                                                     | topics are created by `EnsureTopics` (Phase 0 decision: auto-creation races)                 |
| `MaxAttempts`            | `3`                                                         | `10`                                                                                          |

`PartitionFor` wraps `(&kafka.Hash{}).Balance` (FNV-1a, Sarama-compatible, **not** Java-client murmur2). _A non-Go producer in a later phase would have to use the same hash._

The worker retries `Publish` on failure with the **same** envelope (same `event_id`, same `sequence_number`), with backoff 200ms → 2s, for up to 5 attempts total, then exits non-zero. It never builds a new envelope to retry.

### Replay (`internal/replay`)

```go
type RunStatus string   // RUNNING | AWAITING_APPROVAL | COMPLETED | FAILED | CANCELLED
type NextAction string  // CALL_LLM | EXECUTE_TOOL | RESOLVE_IN_FLIGHT_TOOL | FAIL_MAX_STEPS | NONE

type ToolCall struct {
	ToolUseID string
	ToolName  string
	ToolArgs  json.RawMessage
}

type RunState struct {
	RunID, TenantID  string
	Status           RunStatus
	CurrentStep      int          // step of the latest LLMResponded; 0 before any
	MaxSteps         int
	NextSequence     int64        // sequence_number the next event must use
	LastEventID      string
	LastEventAt      time.Time
	PendingToolCalls []ToolCall   // current step's tool_use blocks with no ToolInvoked yet, in content order
	InFlightTool     *ToolCall    // ToolInvoked with no ToolResulted; nil otherwise
	NextAction       NextAction
	FinalOutput      string       // set by RunCompleted
	Failure          *events.RunFailedPayload
	// unexported: prompt, per-step content + collected tool results, seen (seq→event_id)
}

func Apply(s RunState, e events.Envelope) (RunState, error)
func Fold(evs []events.Envelope) (RunState, error)
func (s RunState) Messages() ([]anthropic.MessageParam, error) // user prompt, then per step: assistant(content), user(tool_results) once all results for that step exist
func (s RunState) Digest() (string, error)

type EventSource interface {
	// Events returns the run's events in log order. Returns ErrRunNotFound if there are none.
	Events(ctx context.Context, runID string) ([]events.Envelope, error)
}

func Replay(ctx context.Context, src EventSource, runID string) (RunState, error)

var (
	ErrRunNotFound        = errors.New("run not found")
	ErrFirstEventNotStart = errors.New("first event is not RunStarted at sequence 0")
	ErrSequenceGap        = errors.New("sequence gap")
	ErrConflictingEvent   = errors.New("conflicting event at existing sequence number")
	ErrEventAfterTerminal = errors.New("event after terminal event")
	ErrRunIDMismatch      = errors.New("event run_id does not match run")
	ErrUnknownToolUseID   = errors.New("tool event references unknown tool_use_id")
	ErrToolOrder          = errors.New("tool events out of order")
)
```

Errors are wrapped with the offending `sequence_number`, e.g. `fmt.Errorf("applying seq %d: %w", seq, ErrSequenceGap)`.

**`NextAction` truth table** (for a non-terminal run):

| Last applied event                                  | Condition                                   | `NextAction`             |
| --------------------------------------------------- | ------------------------------------------- | ------------------------ |
| `RunStarted`                                        | —                                           | `CALL_LLM`               |
| `LLMResponded` (`is_terminal=false`)                | —                                           | `EXECUTE_TOOL`           |
| `LLMResponded` (`is_terminal=true`)                 | worker should emit terminal event next      | `NONE`\*                 |
| `ToolInvoked`                                       | —                                           | `RESOLVE_IN_FLIGHT_TOOL` |
| `ToolResulted`                                      | pending tools remain in step                | `EXECUTE_TOOL`           |
| `ToolResulted`                                      | none remain, `CurrentStep < MaxSteps`       | `CALL_LLM`               |
| `ToolResulted`                                      | none remain, `CurrentStep == MaxSteps`      | `FAIL_MAX_STEPS`         |

\*_`Status` is still `RUNNING` in this case. A crash here leaves a run with its final LLM answer journaled but no terminal event. Phase 2 reports it. Phase 4 decides who finishes it._

### Kafka event source

```go
func NewKafkaEventSource(brokers []string) *KafkaEventSource // implements replay.EventSource
```

Algorithm: `p := PartitionFor(runID, RunEventsPartitions)` → dial the partition leader → `first, hwm := conn.ReadOffsets()` → if `hwm == first`, return `ErrRunNotFound` → read with a `kafka.Reader{Partition: p}` (no `GroupID`) from `first` until `msg.Offset == hwm-1` → keep messages whose `Key == runID`, decoding each (a decode error is returned, not skipped) → `ErrRunNotFound` if none match. The whole scan is bounded by the caller's `ctx` (default 30s in the CLI).

### Postgres (`internal/store`)

Schema: `internal/store/migrations/001_init.sql`, embedded with `//go:embed`. It's the system-spec DDL verbatim, with `IF NOT EXISTS`.

```go
type Projection struct{ pool *pgxpool.Pool } // write side: imported ONLY by cmd/projector

func NewProjection(pool *pgxpool.Pool) *Projection
func (p *Projection) Migrate(ctx context.Context) error
// ApplyEvent runs the single transaction from FR-23. inserted=false means the event already existed (redelivery).
func (p *Projection) ApplyEvent(ctx context.Context, e events.Envelope) (inserted bool, err error)

type EventReader struct{ pool *pgxpool.Pool } // read side: implements replay.EventSource
func NewEventReader(pool *pgxpool.Pool) *EventReader
```

`ApplyEvent` conflict handling: if `ON CONFLICT DO NOTHING` inserts 0 rows, `SELECT event_id FROM events WHERE run_id=$1 AND sequence_number=$2`. If it equals `e.EventID`, return `(false, nil)`. Otherwise return `ErrConflictingEvent`.

### Projector (`cmd/projector`)

```
Env:  DAE_KAFKA_BROKERS   (default "localhost:9092")
      DAE_POSTGRES_DSN    (default "postgres://dae:dae@localhost:5432/dae?sslmode=disable")
Loop: EnsureTopics → Migrate → for { FetchMessage → Decode → ApplyEvent (retry on transient DB error) → CommitMessages }
Log:  "projected seq=%d run_id=%s event=%s inserted=%t partition=%d offset=%d"
Exit: non-zero on decode error or ErrConflictingEvent (poison message; see EC-7). Clean exit on SIGINT/SIGTERM.
```

Reader config: `GroupID: "projector"`, `StartOffset: kafka.FirstOffset`, `CommitInterval: 0` (synchronous commits).

### Worker (`cmd/worker`)

```
Env:  ANTHROPIC_API_KEY (required), DAE_KAFKA_BROKERS, DAE_MOCK_TOOL_DELAY, DAE_CRASH_AFTER_SEQ
Run:  generates run_id → prints "run_id=<uuid>" → EnsureTopics → runs the Phase 1 fake task through the journaled loop
Exit: 0 RunCompleted; 1 RunFailed; 3 publish failed after retries (no terminal event written); 137 injected crash
```

`agent.NewLoop` gains an `events.EventPublisher` dependency. Tests inject an in-memory fake publisher that records envelopes and can be told to fail on the N-th call or on a given `EventType`.

`tools.Tool` gains `HasSideEffect() bool`. Mocks: `clone_repo` → `false`, `run_tests` → `false`, `apply_fix` → `true`, `open_pr` → `true`.

### Replay CLI (`scripts/replay`)

**Input:** `--run-id 7f1c… --source kafka`

**Output (example: killed during the `run_tests` tool delay):**

```
run_id=7f1c4b8e-2d3a-4f6b-9c1e-0a2b3c4d5e6f source=kafka events=6
status=RUNNING current_step=2 max_steps=10 next_sequence=6
next_action=RESOLVE_IN_FLIGHT_TOOL in_flight=run_tests(toolu_02B)
messages: user[text] assistant[text,tool_use] user[tool_result] assistant[tool_use]
state_digest=4be1…9a0c
```

## Constraints

- **Technical:** Go. `segmentio/kafka-go` (v0.4.51, already a dependency). `jackc/pgx/v5` with `pgxpool` is a new dependency, _reason: the Postgres driver for the projector, as chosen in CLAUDE.md_. `google/uuid` is a new dependency, _reason: RFC 4122 UUIDv4 generation for `event_id`/`run_id` without hand-rolled crypto/rand formatting_. No migration library: one embedded SQL file doesn't justify one. No Kafka transactions or idempotent producer: kafka-go doesn't support them, and duplicates are handled by `event_id` + `sequence_number` dedup on the read side instead.
- **Architecture:** Workers never import `internal/store`'s write side, and `cmd/worker` never links pgx (FR-26). Every I/O function takes `context.Context` first. No package-level mutable state; clients are passed in through constructors. CLAUDE.md's Architecture section must be updated to add `cmd/projector`, `internal/replay`, and `scripts/replay`.
- **Ordering:** Per-run ordering comes **only** from the partition key (`run_id`) plus the worker waiting for each ack before sending the next event. Timestamps are never used to order events.
- **Partition count is permanent:** changing `RunEventsPartitions` after events exist sends a `run_id` to a different partition and breaks the Kafka replay scan. Changing it requires a spec change plus a topic rebuild.
- **Performance & scale:** Laptop scale: single broker, tens of runs, runs under 50 events. No latency SLA. The one concrete target: a synchronous publish should take under 50ms p50 against local Kafka. If it's around 1s, the `BatchSize` setting is wrong. Replay cost grows linearly with partition size. That's accepted for Phase 2 (D-2).
- **Durability honesty:** with one broker, `acks=all` is effectively `acks=1`: an acknowledged event is on one broker's disk, not replicated. That's the right model for a laptop and is **not** a claim of production durability. Say so when explaining the system.
- **Testing:** Unit tests (fold, codec, loop with fake publisher and stub LLM) must run under plain `go test ./...` with no infrastructure. Tests that need Docker live behind `//go:build integration` and run with `go test -tags=integration ./...`.
- **Out of scope for Phase 2:** resuming execution after a crash (D-4); Redis/idempotency keys (Phase 3); `run-commands`, worker consumer groups, leases (Phase 4); approval flow producers (Phase 4); gRPC (Phase 5); dead-letter topic; event schema versioning; snapshots.

## Edge Cases & Error Handling

EC-1: **Publish of `ToolInvoked` fails or times out** (broker down, or the network drops after the broker wrote but before the ack reached the worker) → the tool is **not** executed. The worker retries the same envelope as specified above. If it's still failing, the worker exits 3 without a terminal event. The event may or may not be in the log. Both outcomes are safe: if it's absent, replay shows `EXECUTE_TOOL`; if it's present, replay shows `RESOLVE_IN_FLIGHT_TOOL` for a tool that never ran. Phase 3's reconciliation has to treat "in flight" as "possibly ran", not "definitely ran".

EC-2: **Ambiguous write followed by a successful retry** → the log contains two messages with the same `event_id` and `sequence_number`. The fold skips the second (FR-16). The projector's `ON CONFLICT` makes the second a no-op (`inserted=false`). No gap, no double-apply.

EC-3: **Crash after the LLM API call returns but before `LLMResponded` is acknowledged** → the response is lost. Replay shows `CALL_LLM` for that step, and a future resume calls the LLM again. It may get a different answer, which is correct: nothing downstream ever saw the first one. The only cost is tokens. This is why an LLM response is journaled before anything acts on it: **replay never re-calls the LLM, it reads the recorded answer**, which keeps replay deterministic even though the LLM isn't.

EC-4: **Crash during tool execution** (`ToolInvoked` acknowledged, `ToolResulted` not) → replay shows `InFlightTool` set and `NextAction = RESOLVE_IN_FLIGHT_TOOL`. Phase 2 does nothing else (D-4).

EC-5: **Crash mid-batch** (an `LLMResponded` with 2 `tool_use` blocks; the first tool resulted, the second not started) → `PendingToolCalls` holds the second call, and `NextAction = EXECUTE_TOOL`. `Messages()` ends with the assistant turn and does **not** include a partial `tool_result` user turn, because the API requires all of a turn's results in one message.

EC-6: **Projector crashes after the Postgres commit but before the Kafka offset commit** → on restart, the same message is redelivered. `ApplyEvent` returns `inserted=false`, the `runs` row is not touched a second time (the upsert only runs when a row was inserted, in the same transaction), and the offset is committed. Exactly one `events` row and a correct `runs` row.

EC-7: **Poison message on `run-events`** (invalid JSON, unknown `event_type`, or an `ErrConflictingEvent`) → the projector logs partition, offset, and raw value, then exits non-zero **without** committing the offset. It keeps failing on the same message until a human steps in. _Assumption: skipping would quietly corrupt the projection, and a dead-letter topic would be a new topic contract. Stopping loudly is the right call for Phase 2._

EC-8: **Postgres unavailable** → the worker isn't affected and finishes the run (it doesn't touch Postgres). The projector retries `ApplyEvent` with capped exponential backoff (200ms → 5s), forever, without fetching the next message or committing. Once Postgres is back, it catches up in order.

EC-9: **SIGINT/SIGTERM to the worker mid-run** → the context is cancelled, the in-flight LLM or tool call returns a context error, and the worker exits **without** publishing `RunFailed`. Recording a shutdown as a failure would make a resumable run permanently terminal.

EC-10: **Kafka unavailable when the worker starts** → `EnsureTopics` fails, and the worker exits non-zero before calling the LLM or publishing `RunStarted`. No run exists.

EC-11: **Existing `run-events` topic has a different partition count or finite retention** (e.g. created by hand) → `EnsureTopics` fails with a message naming the expected and actual values. The worker and projector refuse to start.

EC-12: **Precision and JSON normalization between Kafka and Postgres** → Postgres `TIMESTAMPTZ` stores microseconds, and `JSONB` reorders keys and strips whitespace. `occurred_at` is truncated to µs at creation (FR-4), and `Digest()` is computed over canonical JSON, not raw bytes (FR-17), so a replay from Kafka and a replay from Postgres give identical digests.

EC-13: **Replay from Postgres while the projector is behind** → returns a valid but *older* state (a prefix of the log). No error: a prefix is a legitimate state. This is exactly why Postgres isn't the authoritative source (D-2). The CLI prints `events=<n>` so the lag is visible.

EC-14: **Two projector instances accidentally running** (same group) → Kafka assigns them different partitions, so ordering per run still holds. If they're in different groups, both insert, and the second insert of each event is a no-op. The projection stays correct either way. The only cost is wasted work.

EC-15: **Event bigger than the broker's max message size** (1 MiB default, e.g. a huge tool result in Phase 9) → `Publish` returns a non-retryable `MessageTooLarge` error. The worker does not retry. It publishes `RunFailed{error_class: "event_too_large", retryable: false}` (small enough to fit) and exits 1. _Can't happen with the Phase 2 mocks. It's here so Phase 9 doesn't discover it by accident._

EC-16: **Unknown tool name / malformed tool args from the LLM** → handled as in Phase 1 (an error `tool_result` sent back to the model). It's journaled as a `ToolInvoked` (`has_side_effect: false`) + `ToolResulted` (`status: "error"`) pair, so the fold's "every tool_use has a result" invariant holds (FR-9).

EC-17: **`RunState` aliasing** → a naive `Apply` that does `s.steps = append(s.steps, …)` can write into a backing array shared with the caller's state. That silently corrupts a state the caller still holds (e.g. the pre-publish state kept for a retry). `Apply` must copy the slices it changes. A unit test applies an event to state S, then checks that S's `Messages()` and `Digest()` are unchanged.

## Acceptance Criteria

### Unit (no infrastructure, `go test ./...`)

- [ ] AC-1: Table-driven codec test: each of the 9 event types survives `Encode` → `Decode` → `DecodePayload` unchanged. `Decode` returns an error for an unknown `event_type`, a missing `event_id`, a missing `run_id`, and invalid JSON. _(FR-1–FR-4)_
- [ ] AC-2: Table-driven fold test with one case per crash point. Each case checks `Status`, `CurrentStep`, `NextSequence`, `NextAction`, `InFlightTool`, `PendingToolCalls`, and the role/block-type shape of `Messages()`. The crash points are: after `RunStarted`; after `LLMResponded` with 1 tool; after `ToolInvoked`; mid-batch of 2 tools; after the last `ToolResulted` with `CurrentStep < MaxSteps`; after the last `ToolResulted` with `CurrentStep == MaxSteps`; after a terminal `LLMResponded`; after `RunCompleted`; after `RunFailed`. _(FR-12, FR-14, FR-15, EC-3, EC-4, EC-5)_
- [ ] AC-3: Table-driven invariant test: each FR-16 violation returns the matching sentinel error (checked with `errors.Is`). A redelivered duplicate (same seq, same `event_id`) gives the same state and `Digest()` as without it. _(FR-16, EC-2)_
- [ ] AC-4: **Live/replay equivalence:** the loop runs with the Phase 1 six-response stub LLM and a fake publisher. For every call `k` the stub receives, `params.Messages` equals `Fold(publishedEventsSoFar).Messages()`, compared as canonical JSON. _(FR-13, D-1)_
- [ ] AC-5: The multi-step loop test produces exactly the canonical 18-event sequence from API Contracts (event types, steps, gapless `sequence_number` 0–17). The existing Phase 1 `TestLoopRun` cases still pass, and each ends with the terminal event from FR-10. _(FR-5–FR-7, FR-10)_
- [ ] AC-6: **Write-ahead:** with a fake publisher that fails every `ToolInvoked`, the tool's `Execute` call count is 0 and no later event is published. With a publisher that fails on `LLMResponded`, no tool runs. With a publisher that fails once and then succeeds, the retry sends the identical envelope (same `event_id` and `sequence_number`). _(FR-8, EC-1)_
- [ ] AC-7: The aliasing test in EC-17 passes. `Digest()` is the same for a state folded from original envelopes and one folded from envelopes whose payloads were re-marshaled through `map[string]any` (key order changed). _(FR-17, EC-12, EC-17)_
- [ ] AC-8: `go list -deps ./cmd/worker | grep jackc/pgx` prints nothing. _(FR-26)_

### Against real infrastructure (run by you, with evidence)

- [ ] AC-9: After `docker compose up`, `kafka-topics.sh --describe --topic run-events` shows 6 partitions, and `kafka-configs.sh --describe` shows `retention.ms=-1`. _(FR-27)_
- [ ] AC-10: With the projector running, `go run ./cmd/worker` completes the fake task. `SELECT sequence_number, event_type FROM events WHERE run_id=… ORDER BY sequence_number` returns gapless rows from 0, and they match the worker's printed `seq=` lines one for one. The `runs` row shows `status=COMPLETED`. _(FR-5–FR-7, FR-22–FR-25)_
- [ ] AC-11: **The Phase 2 headline test (system spec AC-3), crash during a tool.** With `DAE_MOCK_TOOL_DELAY=15s`, `kill -9` the worker during the pause right after it prints a `ToolInvoked` line. `scripts/replay --source kafka` shows `status=RUNNING`, `next_action=RESOLVE_IN_FLIGHT_TOOL`, and the correct `in_flight` tool, and its `state_digest` equals the digest on the worker's last printed line. The worker's printed output and the replay output are both pasted into `docs/decisions/phase2.md`. _(FR-18, FR-19, FR-21, EC-4)_
- [ ] AC-12: **Crash during an LLM call.** Same as AC-11, but killed while an LLM call is in progress (after a `ToolResulted` line). Replay shows `next_action=CALL_LLM`, and the digest matches the last printed line. _(EC-3)_
- [ ] AC-13: **Every crash point, repeatably.** For each `N` in `{0, 1, 2, 3, 16}`, a run with `DAE_CRASH_AFTER_SEQ=N` exits 137. `scripts/replay --source kafka` then shows `next_sequence=N+1` and the `next_action` from the truth table. Because the crash happens before the log line for seq N, check the digest with `--up-to-seq N-1` against the worker's last printed line (for `N=0`, which has no earlier line, skip the digest check). _(FR-15, FR-30)_
- [ ] AC-14: **Sources agree.** For the killed runs from AC-11 through AC-13 and one completed run, once the projector has caught up, `--source postgres` and `--source kafka` print the same `state_digest`. _(FR-20, EC-12)_
- [ ] AC-15: **Projector redelivery.** With `DAE_PROJECTOR_CRASH_BEFORE_OFFSET_COMMIT=5`, the projector exits after message 5's Postgres commit. On restart it logs `inserted=false` for that message. `SELECT count(*), count(DISTINCT sequence_number) FROM events WHERE run_id=…` shows equal counts that match Kafka's event count for the run. _(FR-23, EC-6)_
- [ ] AC-16: **Postgres is rebuildable.** Save `SELECT event_id, run_id, sequence_number, event_type, payload, occurred_at FROM events ORDER BY run_id, sequence_number` (and all of `runs`) to a file. Stop the projector. `TRUNCATE events, runs`. Reset group `projector` to earliest with `kafka-consumer-groups.sh --reset-offsets --to-earliest --execute`. Restart the projector. Re-export. `diff` is empty. _(System-spec critical rule: Postgres is a derived projection)_
- [ ] AC-17: **Worker doesn't depend on Postgres.** `docker compose stop postgres`, then run the worker: it completes with exit 0. The projector logs retries and doesn't advance. `docker compose start postgres`: the projector catches up, and the `runs` row shows `COMPLETED`. _(EC-8)_
- [ ] AC-18: **Kafka down mid-run.** During a tool delay, `docker compose stop kafka`. After the tool returns, the worker logs its publish retries, **never** logs another tool invocation or LLM call, and exits 3. _(FR-8, EC-1)_
- [ ] AC-19: **SIGINT isn't failure.** Ctrl+C the worker mid-run. Replay shows `status=RUNNING` with no `RunFailed` event. _(FR-11, EC-9)_
- [ ] AC-20: **Durable across infra restart.** `docker compose down` (without `-v`) → `docker compose up` → `scripts/replay --source kafka` for a run from before the restart still prints the same digest. _(FR-28)_
- [ ] AC-21: A projector started against a manually created `run-events` topic with 3 partitions refuses to start and names the mismatch. _(FR-27, EC-11)_
