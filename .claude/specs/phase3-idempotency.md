# Phase 3: Idempotent Tool Execution (Redis) — Spec

_Scope: Phase 3 only. Background contracts live in `.claude/specs/system-spec.md` (FR-5, EC-1, EC-2, EC-9, EC-12, AC-4, AC-14, the Redis Key Contracts table and its flagged `tool_use_id` collision note). Phase 2's contracts (`phase2-event-journal.md`) are assumed and are not repeated here. Where this phase needs a contract the system spec doesn't have yet, it's listed under **API Contracts → System-spec amendments**. Per CLAUDE.md, those amendments get applied to `system-spec.md` **before** implementation starts._

## Problem Statement

Phase 2 journals `ToolInvoked` before a tool runs, so after a crash, replay can say "this tool was in flight" (`RESOLVE_IN_FLIGHT_TOOL`). It can't say whether the tool's side effect actually happened. `ToolInvoked` without `ToolResulted` covers "never started", "PR opened, crashed before recording it", and everything in between. Phase 2 deliberately refuses to resume for this reason (Phase 2 D-4): blindly re-running `open_pr` could open a second PR, and never re-running it could leave the run stuck forever. This project exists to guarantee that a crashed run resumes **without duplicating a real-world side effect**. Without a durable record of "claimed / done" that is written *before* the effect fires, that guarantee can't hold, and every later phase (worker pool, chaos test, demo agent) would be building on an unproven claim. Phase 3 adds that record in Redis, defines exactly what a recovering worker does in each state, and proves it by crashing a worker at every point around a side-effecting tool call.

## Functional Requirements

### Idempotency key and record

FR-1: Every `ToolInvoked` for a tool with `has_side_effect: true` must carry an idempotency key `idem:{run_id}:{step}:{tool_use_id}` in both `payload.idempotency_key` and the envelope's `idempotency_key`, and the two must be equal. Tools with `has_side_effect: false` keep both `null`. _(D-1)_

FR-2: The value stored under a key must be a JSON `IdemRecord` (see API Contracts) with `state` of `claimed` or `resolved`. It must include `args_hash = hex(sha256(tool_name + "\x00" + canonical_json(tool_args)))`. `canonical_json` parses with `json.Decoder.UseNumber()`, so integers never pass through `float64`, and re-encodes with sorted keys and no insignificant whitespace.

FR-3: The key is computed **once**, when `ToolInvoked` is built. A recovering worker must use the key and `has_side_effect` flag **recorded in the journaled `ToolInvoked`**, and must never recompute them from the current code or registry. _(D-3)_

### Fencing protocol

FR-4: For a side-effecting tool, the worker must run this order exactly: (1) publish `ToolInvoked` and wait for the ack → (2) **claim** the key in Redis → (3) `Execute` → (4) **resolve** the key in Redis → (5) publish `ToolResulted`. `Execute` must never be called unless step (2) returned `Acquired` in this process. _(D-2, CLAUDE.md critical rules)_

FR-5: The claim must be one atomic `SET key <claimed record> NX GET EX <ttl>`. A nil reply means this process now holds the claim. A non-nil reply is the existing record, and the key is left unchanged.

FR-6: Resolve and takeover must be atomic compare-and-set Lua scripts that only write when the stored record is `state: claimed` **and** its `claim_token` equals the caller's token. If it doesn't match, they return `ErrClaimLost` and write nothing.

FR-7: After step (1), both the live `EXECUTE_TOOL` path and the recovery `RESOLVE_IN_FLIGHT_TOOL` path must call the **same** function, `idempotency.Guard.Run`, which implements the decision table in API Contracts. There is no separate recovery code path. _(D-3)_

FR-8: **Fail closed.** If Redis errors or times out at any point before `Execute`, the tool must not run. The Guard retries the failed Redis operation (5 attempts total, 500ms timeout per attempt, backoff 200ms → 2s). If every attempt fails, it returns `ErrFenceUnavailable`. The worker then exits with code 4 and publishes **no** terminal event, so the run stays resumable. _(system-spec EC-2)_

FR-9: A claim held by another claimant must be respected until it is **stale**: `now - claimed_at >= tool_timeout + claim_stale_grace`. Until then, the Guard polls `GET` every 1s and waits. If the key becomes `resolved`, that's a cache hit. If it becomes stale, the Guard moves on to reconciliation. `Execute` must be bounded by a context deadline of `tool_timeout`. _(D-5)_

FR-10: A claim whose `claim_token` equals the token this process just tried to write counts as `Acquired`. That case happens when a claim `SET` took effect but its reply was lost and the retry sees the process's own record (EC-3).

FR-11: A failed resolve (Redis error after retries, or `ErrClaimLost`) after a completed `Execute` must be logged loudly but must **not** stop the worker from publishing `ToolResulted`. The journal is authoritative. _(D-6)_

FR-12: If `Execute` returns `tools.ErrOutcomeUnknown`, or returns an error after `ctx` is done (cancellation or `tool_timeout`), the worker must **not** resolve and must **not** publish `ToolResulted`. The key stays `claimed`, and the worker exits 4. The next resume sees a stale claim and reconciles. A clean `(result, nil)` return is a definite outcome even if the deadline fired at the same instant, so it is resolved normally. Any other `Execute` error means "the effect did not happen". It is resolved and journaled as `status: "error"`, exactly as in Phase 2.

FR-13: An absent key is proof that the tool never ran only if the in-flight `ToolInvoked` is younger than the key's TTL: `now - invoked_at < idempotency_ttl - 1m`. If it's older, the key could have existed and expired, so the Guard claims it and then goes through the reconciliation path **before** executing. It must not execute straight away. _(EC-6)_

### Reconciliation

FR-14: A tool may implement `tools.Reconciler`. For a stale claim, the Guard must: call `Reconcile`. If `Applied`, it takes over the claim and resolves it with the reconciled result, **without** executing (`resolution: reconciled`). If not `Applied`, it takes over the claim and then executes (`resolution: reexecuted`). _(D-4)_

FR-15: A stale claim on a tool that does **not** implement `Reconciler` must never be executed. The worker publishes `RunFailed{error_class: "unresolved_side_effect", retryable: false}`, with the key, tool name, and `tool_use_id` in `error_message`. _(D-4, system-spec EC-1 "flag for manual review")_

FR-16: A `Reconcile` error is handled like a Redis error: fail closed, retry with the FR-8 policy, then `ErrFenceUnavailable` (exit 4).

FR-17: An existing record whose `run_id`, `tool_use_id`, `tool_name`, or `args_hash` doesn't match the invocation, or that isn't valid `IdemRecord` JSON, must never lead to `Execute`. The worker publishes `RunFailed{error_class: "idempotency_record_mismatch", retryable: false}`.

### Journal changes

FR-18: `ToolResulted` gains `resolution: "executed" | "cached" | "reconciled" | "reexecuted"`. `was_replayed_from_cache` is kept and must equal `resolution == "cached"`. Non-side-effect tools always record `executed`.

FR-19: For the in-flight tool, `replay.RunState.InFlightTool` must carry `HasSideEffect`, `IdempotencyKey`, and `InvokedAt` (the `ToolInvoked` envelope's `occurred_at`), all taken from the journaled `ToolInvoked`.

FR-20: `NextAction` gains `FINISH_RUN`. It replaces Phase 2's `NONE`-while-`RUNNING` case, where the last `LLMResponded` is terminal or has no `tool_use` blocks. `RunState.TerminalPayload()` returns the FR-10 (Phase 2) terminal payload for that step (`RunCompleted`, or `RunFailed` with `llm_output_truncated` / `malformed_llm_response`). The live loop must emit its terminal events through this same method, so a live finish and a resumed finish can't drift apart.

### Resume

FR-21: `cmd/worker` with `DAE_RESUME_RUN_ID=<uuid>` must replay that run from Kafka (`replay.KafkaEventSource`, never Postgres) and continue it from `NextAction`, publishing from `NextSequence`. If the run is already terminal, the worker prints its state, publishes nothing, and exits 0 for `COMPLETED` and 1 for `FAILED`/`CANCELLED`. If the run isn't found, it exits 2.

FR-22: A resumed `RESOLVE_IN_FLIGHT_TOOL` with journaled `has_side_effect: false` must re-execute the tool without touching Redis, then publish `ToolResulted`. Reads are safe to repeat.

FR-23: `Loop.Run` (fresh run) must become "publish `RunStarted`, then `drive(state)`", and `Loop.Resume` must be `drive(replayedState)`. `drive` is the one loop that handles every `NextAction`.

### Mock external world and evidence

FR-24: The mock side-effecting tools must record each real execution as a line in an append-only JSONL **side-effect ledger** at `DAE_MOCK_LEDGER_PATH` (default `.dae/mock-ledger.jsonl`, which gets added to `.gitignore`). Each entry is written with a single `write()` on an `O_APPEND` file followed by `fsync`, **after** the mock delay. The ledger is the mock's stand-in for "GitHub", and it is independent of Redis on purpose. _(D-7)_

FR-25: `open_pr` must implement `Reconciler` by looking up its `idempotency_key` in the ledger. If there's a match it returns `Applied` with the recorded result. Its result must be `opened PR #<n>: <title>`, where `n` = 1 + the number of `open_pr` entries already in the ledger for that `run_id`, so a duplicate shows up as `#2`. `apply_fix` must **not** implement `Reconciler`, so the no-reconciler path is exercised too. _(D-4)_

FR-26: `run_tests` must report failure until the ledger contains an `apply_fix` entry for the invocation's `run_id`, and success from then on. This replaces the per-instance call counter, which a resumed process would reset.

FR-27: The system must provide `go run ./scripts/effects [--ledger <path>] [--run-id <uuid>]`. It prints one row per `idempotency_key` (`key tool count`) and a summary `entries=<n> distinct_keys=<m> duplicates=<k>`, and exits 1 if `k > 0`. **Definition used from Phase 3 onward (Phase 8 depends on it):** a *duplicated side effect* is two or more ledger entries with the same `idempotency_key`.

FR-28: Before its seq line, every Guard decision must print one line: `fence key=<key> tool=<name> decision=<decision>`, where `<decision>` is one of `acquired | cache_hit | waiting | reconciled_found | reconciled_not_found | unresolvable | record_mismatch | redis_retry | fence_unavailable | resolve_failed | outcome_unknown`. `waiting` prints at most once every 5s with `claim_age=<dur> stale_in=<dur>`.

### Infrastructure and wiring

FR-29: The `redis` service in `docker-compose.yml` must run with `--appendonly yes --appendfsync always --maxmemory-policy noeviction` and use a named volume `redis-data` mounted at `/data`. _(D-8)_

FR-30: At startup, `cmd/worker` must `PING` Redis with a 5s timeout. If the ping fails, it exits 1 **before** publishing anything (no `RunStarted` for a fresh run, nothing at all for a resume).

FR-31: The worker must accept `DAE_CRASH_AT=<tool_name>:<point>`, where `<point>` is `after_claim` (claimed, before `Execute`), `after_execute` (`Execute` returned, before resolve), or `after_resolve` (resolved, before `ToolResulted` is published). It calls `os.Exit(137)` the first time that tool reaches that point in this process. `DAE_CRASH_AFTER_SEQ` stays as it is.

FR-32: `scripts/replay` must print `side_effect=<bool> idem_key=<key|null> invoked_at=<rfc3339>` on the `in_flight=` line, and `resolution=` wherever it summarizes a `ToolResulted`.

## API Contracts

### Design decisions made by this spec (confirm during review)

- **D-1: Key = `idem:{run_id}:{step}:{tool_use_id}`. The args hash goes in the value and is verified, not put in the key.** This resolves the collision flagged in Phase 2. _Why `tool_use_id`:_ it is fixed forever once `LLMResponded` is journaled, and it's already the identity the fold uses. Two identical `tool_use` blocks in one response get two ids, so they get two keys. A true retry of one invocation re-reads the same id from the journal, so it gets the same key. _Why not keep the hash in the key:_ then the key depends on the canonicalization code never changing. A future change to `canonical_json` would give an in-flight invocation a **different** key after a redeploy, recovery would find it absent and re-execute, and that duplicate would be silent. With the hash in the value, the same change produces a detected mismatch (FR-17) and the worker fails closed. _Rejected:_ `{sha256(tool_name+args)}` alone (collides); adding a content-block index (deterministic too, but a second identity for something `tool_use_id` already names). `step` is redundant with `tool_use_id`, but it's kept for readability in `redis-cli`.
- **D-2: Order is journal `ToolInvoked` → claim → `Execute` → resolve → journal `ToolResulted`.** Each adjacency closes one window. _Claim before `Execute`_ (CLAUDE.md): a crash mid-effect leaves a visible `claimed` record. _`ToolInvoked` before claim:_ during recovery, a missing key then means "never claimed, so never executed" (FR-13). If you claimed first, a crash between the claim and `ToolInvoked` would leave a claimed key for a tool the journal still calls *pending*, and the live path would have to reconcile as well. _Resolve before `ToolResulted`:_ a crash between the two leaves a `resolved` record, so recovery gets a cache hit (the system spec's AC-4 case).
- **D-3: One code path, driven by the journal.** Live and recovery both reach `Guard.Run` after `ToolInvoked` is acked (FR-7). Recovery trusts the journaled key and `has_side_effect` (FR-3), because the registry or hashing code may have changed since the crash. This is Phase 2's D-1 idea applied to fencing.
- **D-4: For a stale claim, reconciliation is per tool, and no reconciler means the run fails.** Only the tool knows how to ask the outside world "did this happen?" (Phase 9: "does a PR from this branch exist?"). A tool that can't answer is marked `unresolved_side_effect` and a human looks at it. The alternative, re-executing, is a guessed duplicate. `open_pr` (reconciles) and `apply_fix` (doesn't) are split on purpose, so both branches get exercised. _Rejected:_ parking the run without a terminal event. Nothing in Phase 3 could ever un-park it, so it would look resumable when it isn't.
- **D-5: A claim is respected until it's stale.** Without a heartbeat, a recovering worker can't tell a crashed claimant from a live, slow one. `Execute` is bounded by `tool_timeout`, so a claim older than `tool_timeout + claim_stale_grace` (30s) almost certainly belongs to a dead process or an abandoned attempt. _Residual risk, stated honestly:_ a claimant paused (GC, VM freeze) past that threshold whose effect lands afterwards can still duplicate. Fully closing that needs the external system to reject stale fencing tokens, and GitHub doesn't offer that. Phase 4's lease heartbeat shortens the wait. It doesn't remove the risk. _Cost in Phase 3:_ resuming soon after a crash mid-claim waits up to `tool_timeout + 30s`. That wait is visible in the logs (`decision=waiting`).
- **D-6: A resolve failure after `Execute` doesn't block `ToolResulted`.** Once `ToolResulted` is in Kafka, recovery never consults Redis for that invocation, so a leftover `claimed` record is harmless. Stopping instead would turn a Redis blip into a forced reconciliation, or an `unresolved_side_effect` failure for `apply_fix`, for an effect whose result is sitting in memory.
- **D-7: The mock external world is a file ledger, not Redis.** The evidence that something didn't happen twice must not come from the mechanism that's supposed to prevent it happening twice. The ledger survives `kill -9` and a Redis restart, gives `open_pr` something to reconcile against, and lets `scripts/effects` count duplicates. _Known limit:_ a local file won't work across Kubernetes pods (Phase 6/8 will need a shared equivalent).
- **D-8: Redis must be durable, because "absent key" is used as evidence.** FR-13 treats a missing key as "never executed". If Redis loses writes (no AOF, eviction, restart without a volume), a claimed key can vanish and recovery will re-execute. So: AOF with `appendfsync always` (every write is fsynced before the reply, and at laptop scale the cost doesn't matter), `noeviction`, and a named volume. This reverses the Phase 0/2 comment "disposable fencing state".
- **D-9: Phase 3 adds single-worker resume (`DAE_RESUME_RUN_ID`).** Without it, none of the recovery behavior can be demonstrated. **Assumption A-1:** at most one worker process works on a given run at a time, and the operator enforces that by only resuming after the original process is dead. The fence itself is built to be safe for tool execution even if A-1 is violated (FR-5, FR-9). The Kafka journal is not: two live writers would publish conflicting `sequence_number`s (EC-16). Phase 4's lease enforces A-1.

### System-spec amendments (apply to `system-spec.md` before implementing)

| Location | System spec today | Amended |
| --- | --- | --- |
| Redis Key Contracts, `idem:` row | `idem:{run_id}:{step}:{sha256(tool_name+args)}`; `SET key value NX EX <ttl>`; value = JSON `ToolResulted` payload | `idem:{run_id}:{step}:{tool_use_id}`; claim = `SET key <IdemRecord> NX GET EX <ttl>`; resolve/takeover = compare-and-set Lua on `claim_token`; value = `IdemRecord` JSON (below) |
| Redis Key Contracts, TTL note | 24h default, configurable | keep it, add: `DAE_IDEMPOTENCY_TTL`; must be `> tool_timeout + 30s` (checked at startup); TTL is refreshed on resolve; an expired key is treated per FR-13 |
| Redis Key Contracts, flagged Phase 2 note | "Phase 3 must decide…" | replace with D-1's decision and the reason |
| New note under Redis | — | Redis must run with AOF `appendfsync always`, `noeviction`, and a persistent volume. Fencing correctness depends on it (D-8) |
| `ToolInvoked` payload | `idempotency_key` present | populated (non-null) iff `has_side_effect`; equals envelope `idempotency_key` |
| `ToolResulted` payload | `{ step, tool_use_id, tool_name, result, status, was_replayed_from_cache }` | adds `resolution: "executed" \| "cached" \| "reconciled" \| "reexecuted"`; `was_replayed_from_cache == (resolution == "cached")`. Additive: Phase 2 events decode with `resolution: ""` |
| EC-1 | "Mitigation to design for: …" | concrete: claim → execute → resolve, three-state record, per-tool `Reconciler`, `unresolved_side_effect` otherwise (this spec) |
| EC-9 | "…key includes `step` and `tool_args`…" | "…key includes `tool_use_id`: different invocations, even with identical args, get different ids. A retry of one invocation re-reads the same id from the journal" |
| `RunFailed.error_class` values | — | adds `unresolved_side_effect`, `idempotency_record_mismatch` |

### Key and record (`internal/idempotency`)

```go
func Key(runID string, step int, toolUseID string) string          // "idem:7f1c…:5:toolu_05E"
func ArgsHash(toolName string, args json.RawMessage) (string, error) // FR-2

type RecordState string
const (
	StateClaimed  RecordState = "claimed"
	StateResolved RecordState = "resolved"
)

type IdemRecord struct {
	State      RecordState `json:"state"`
	RunID      string      `json:"run_id"`
	Step       int         `json:"step"`
	ToolUseID  string      `json:"tool_use_id"`
	ToolName   string      `json:"tool_name"`
	ArgsHash   string      `json:"args_hash"`
	ClaimToken string      `json:"claim_token"` // UUIDv4, fresh per claim/takeover attempt
	ClaimedBy  string      `json:"claimed_by"`  // "<hostname>-<pid>"
	ClaimedAt  time.Time   `json:"claimed_at"`  // UTC, µs
	ResolvedAt *time.Time  `json:"resolved_at"`
	Result     *string     `json:"result"`      // set when resolved
	Status     *string     `json:"status"`      // "success" | "error", set when resolved
	Resolution *string     `json:"resolution"`  // "executed" | "reconciled" | "reexecuted", set when resolved
}
```

**Example resolved value** (`GET idem:7f1c4b8e-…:5:toolu_05E`):

```json
{
  "state": "resolved",
  "run_id": "7f1c4b8e-2d3a-4f6b-9c1e-0a2b3c4d5e6f",
  "step": 5,
  "tool_use_id": "toolu_05E",
  "tool_name": "open_pr",
  "args_hash": "3b1f…c9",
  "claim_token": "0d8e2c61-7a55-4c1b-9a0e-5f3e2b1d4c77",
  "claimed_by": "sahil-mbp-48213",
  "claimed_at": "2026-09-20T14:02:11.104233Z",
  "resolved_at": "2026-09-20T14:02:11.131907Z",
  "result": "opened PR #1: Fix widget parsing",
  "status": "success",
  "resolution": "executed"
}
```

### Store (`internal/idempotency`)

```go
type ClaimResult struct {
	Acquired bool
	Existing *IdemRecord // non-nil iff !Acquired
}

type Store interface {
	// Claim: SET key rec NX GET EX ttl. Acquired=true on a nil reply.
	Claim(ctx context.Context, key string, rec IdemRecord) (ClaimResult, error)
	Get(ctx context.Context, key string) (*IdemRecord, error) // nil, nil if absent
	// Resolve and TakeOver are CAS on (state==claimed && claim_token==expectToken).
	Resolve(ctx context.Context, key, expectToken string, resolved IdemRecord) error // ErrClaimLost
	TakeOver(ctx context.Context, key, expectToken string, claimed IdemRecord) error // ErrClaimLost
}

func NewRedisStore(client *redis.Client, ttl time.Duration) *RedisStore
```

Compare-and-set script, shared by `Resolve` and `TakeOver` (`redis.NewScript`, run with `EVALSHA` and falling back to `EVAL`):

```lua
-- KEYS[1]=key  ARGV[1]=expected claim_token  ARGV[2]=new value  ARGV[3]=ttl seconds
local cur = redis.call('GET', KEYS[1])
if not cur then return 0 end
local rec = cjson.decode(cur)
if rec.state ~= 'claimed' or rec.claim_token ~= ARGV[1] then return 0 end
redis.call('SET', KEYS[1], ARGV[2], 'EX', ARGV[3])
return 1
```

_`SET … IFEQ` would avoid Lua, but it needs Redis 8.4. The compose file runs `redis:7-alpine`, and `SET NX GET` needs ≥ 7.0._

Redis client options: `DialTimeout: 1s`, `ReadTimeout: 500ms`, `WriteTimeout: 500ms`, `MaxRetries: -1` (go-redis v9 maps `-1` to "no retries"). The Guard owns retries, so each retry shows up as a `redis_retry` line and is testable with a fake store.

### Guard (`internal/idempotency`)

```go
type Guard struct { /* store Store; now func() time.Time; toolTimeout, staleGrace, ttl time.Duration; out io.Writer; crash CrashHook */ }

func NewGuard(store Store, cfg GuardConfig) *Guard

type Resolution string // "executed" | "cached" | "reconciled" | "reexecuted"

type Outcome struct {
	Result     string
	Status     string // "success" | "error"
	Resolution Resolution
}

// Run is called only after ToolInvoked is acked, for tools whose journaled
// has_side_effect is true. It returns an Outcome to journal as ToolResulted,
// or one of the errors below.
func (g *Guard) Run(ctx context.Context, inv tools.Invocation, call replay.ToolCall, tool tools.Tool) (Outcome, error)

var (
	ErrFenceUnavailable = errors.New("idempotency fence unavailable")         // → exit 4, no terminal event
	ErrUnresolvable     = errors.New("claimed side effect cannot be resolved") // → RunFailed unresolved_side_effect
	ErrRecordMismatch   = errors.New("idempotency record does not match invocation") // → RunFailed idempotency_record_mismatch
	ErrOutcomeUnknown   = errors.New("side effect outcome unknown; key left claimed") // → exit 4
	ErrClaimLost        = errors.New("claim no longer held")                   // resolve: logged only (D-6); takeover: re-run decision
)
```

**Decision table** (the three-state machine: absent → claimed → resolved). The Guard evaluates this from the top, starting with `Claim`:

| # | `Claim` reply | Condition | Log `decision=` | `Execute`? | Result |
| - | --- | --- | --- | --- | --- |
| 1 | nil (acquired) | `now - invoked_at < ttl - 1m` | `acquired` | **yes** | resolve → `executed` |
| 2 | nil (acquired) | `invoked_at` too old, so the key may have expired (FR-13) | — | — | go to row 7/8/9 with our own token as `expectToken` |
| 3 | existing, `claim_token` == token this process just sent | ambiguous earlier `SET` (EC-3) | `acquired` | **yes** | as row 1 |
| 4 | existing, identity/`args_hash` mismatch, or bad JSON | — | `record_mismatch` | no | `ErrRecordMismatch` |
| 5 | existing `resolved` | matches | `cache_hit` | no | `cached` with the stored result/status |
| 6 | existing `claimed`, other token | not stale | `waiting` | no | poll `GET` every 1s → row 5 when resolved, rows 7–9 when stale; a key that disappears restarts at `Claim` |
| 7 | `claimed`, stale | tool is a `Reconciler`, `Applied` | `reconciled_found` | no | `TakeOver`, then `Resolve` with reconciled result → `reconciled` |
| 8 | `claimed`, stale | tool is a `Reconciler`, not `Applied` | `reconciled_not_found` | **yes** | `TakeOver`, then execute → `reexecuted` |
| 9 | `claimed`, stale | tool isn't a `Reconciler` | `unresolvable` | no | `ErrUnresolvable` |
| 10 | Redis error / timeout at any row before `Execute` | 5 attempts failed | `redis_retry` ×n, then `fence_unavailable` | no | `ErrFenceUnavailable` |
| 11 | `TakeOver` → `ErrClaimLost` | someone else took over or resolved | — | no | restart at `Claim`; after 3 lost races → `ErrFenceUnavailable` |

After `Execute`: an `ErrOutcomeUnknown` return, or `ctx` done → `outcome_unknown`, don't resolve, return `ErrOutcomeUnknown` (FR-12). Otherwise `Resolve`. On failure it logs `resolve_failed` and still returns the Outcome (D-6).

**Crash-window walk-through** (the Phase 3 acceptance story; `open_pr` has a reconciler, `apply_fix` doesn't):

| Window | Journal after crash | Redis after crash | Ledger | Resume does | Effects after resume |
| --- | --- | --- | --- | --- | --- |
| W0: before `ToolInvoked` ack | pending (`EXECUTE_TOOL`) | absent | 0 | publish `ToolInvoked`, row 1 | 1 |
| W1: after `ToolInvoked`, before claim | in flight | absent | 0 | row 1 | 1 |
| W2: `after_claim` (or killed during mock delay) | in flight | `claimed` | 0 | row 6 → stale → `open_pr`: row 8; `apply_fix`: row 9 | `open_pr` 1; `apply_fix` 0 + `RunFailed` |
| W3: `after_execute` | in flight | `claimed` | 1 | row 6 → stale → `open_pr`: row 7; `apply_fix`: row 9 | 1 (+ `RunFailed` for `apply_fix`) |
| W4: `after_resolve` | in flight | `resolved` | 1 | row 5 | 1 |
| W5: after `ToolResulted` ack | done | `resolved` | 1 | fold skips it; Redis not consulted | 1 |

### Tools (`internal/tools`) — interface change

```go
type Invocation struct {
	RunID          string
	Step           int
	ToolUseID      string
	IdempotencyKey *string // nil when the tool has no side effect
}

type Tool interface {
	Name() string
	Description() string
	InputSchema() anthropic.ToolInputSchemaParam
	// Execute returning a non-nil error other than ErrOutcomeUnknown asserts
	// the side effect did NOT happen. A real tool that can't be sure must
	// return ErrOutcomeUnknown (FR-12).
	Execute(ctx context.Context, inv Invocation, rawArgs json.RawMessage) (string, error)
	HasSideEffect() bool
}

type Reconciler interface {
	Reconcile(ctx context.Context, inv Invocation, rawArgs json.RawMessage) (ReconcileResult, error)
}
type ReconcileResult struct {
	Applied bool
	Result  string // the original result, when Applied
}

var ErrOutcomeUnknown = errors.New("tool: side effect outcome unknown")
```

Passing the idempotency key into the tool is intentional. Phase 9's real `open_pr` derives its branch name from it, which is what makes "does a PR from this branch exist?" a reconciliation check that actually works.

### Ledger (`internal/tools`)

```go
type LedgerEntry struct {
	RunID          string    `json:"run_id"`
	ToolUseID      string    `json:"tool_use_id"`
	ToolName       string    `json:"tool_name"`
	IdempotencyKey string    `json:"idempotency_key"`
	Result         string    `json:"result"`
	RecordedAt     time.Time `json:"recorded_at"`
	WriterPID      int       `json:"writer_pid"`
}

func NewLedger(path string) *Ledger // passed to mock constructors; no package-level state
func (l *Ledger) Record(ctx context.Context, e LedgerEntry) error
func (l *Ledger) FindByKey(ctx context.Context, idempotencyKey string) (LedgerEntry, bool, error)
func (l *Ledger) Entries(ctx context.Context) ([]LedgerEntry, error)
```

Example line: `{"run_id":"7f1c…","tool_use_id":"toolu_05E","tool_name":"open_pr","idempotency_key":"idem:7f1c…:5:toolu_05E","result":"opened PR #1: Fix widget parsing","recorded_at":"2026-09-20T14:02:11.120004Z","writer_pid":48213}`

### Replay (`internal/replay`) — additions

```go
type ToolCall struct {
	ToolUseID      string
	ToolName       string
	ToolArgs       json.RawMessage
	HasSideEffect  bool       // InFlightTool only: from journaled ToolInvoked
	IdempotencyKey *string    // InFlightTool only
	InvokedAt      time.Time  // InFlightTool only: ToolInvoked envelope occurred_at
}

const ActionFinishRun NextAction = "FINISH_RUN"

func (s RunState) TerminalPayload() (events.Payload, error) // valid only when NextAction == FINISH_RUN
```

Updated truth-table rows (all other rows from Phase 2 unchanged):

| Last applied event | Condition | `NextAction` |
| --- | --- | --- |
| `LLMResponded` (`is_terminal=true`) | — | `FINISH_RUN` |
| `LLMResponded` (`stop_reason=tool_use`, zero `tool_use` blocks) | — | `FINISH_RUN` (payload: `malformed_llm_response`) |

### Worker (`cmd/worker`)

```
Env:  ANTHROPIC_API_KEY (required), DAE_KAFKA_BROKERS, DAE_MOCK_TOOL_DELAY, DAE_CRASH_AFTER_SEQ,
      DAE_REDIS_ADDR          (default "localhost:6379")
      DAE_IDEMPOTENCY_TTL     (Go duration, default "24h")
      DAE_TOOL_TIMEOUT        (Go duration, default "2m"; claim_stale_grace is a fixed 30s)
      DAE_MOCK_LEDGER_PATH    (default ".dae/mock-ledger.jsonl")
      DAE_RESUME_RUN_ID       (optional: resume instead of starting a new run)
      DAE_CRASH_AT            (optional, crash-testing only: "<tool>:after_claim|after_execute|after_resolve")
Start: EnsureTopics → PING Redis (5s) → validate TTL > tool_timeout + 30s (panic: config error)
       → fresh:  generate run_id, print "run_id=<uuid>", Loop.Run
       → resume: print "run_id=<uuid> resumed_from_seq=<n> next_action=<a>", Loop.Resume
Exit: 0 RunCompleted; 1 RunFailed / startup failure; 2 resume run not found;
      3 publish failed after retries; 4 fence unavailable or outcome unknown (no terminal event; resumable);
      137 injected crash
```

Example resume output for crash window W4:

```
run_id=7f1c4b8e-… resumed_from_seq=15 next_action=RESOLVE_IN_FLIGHT_TOOL
fence key=idem:7f1c4b8e-…:5:toolu_05E tool=open_pr decision=cache_hit
seq=15 event=ToolResulted step=5 next_action=CALL_LLM state_digest=…
seq=16 event=LLMResponded step=6 next_action=FINISH_RUN state_digest=…
seq=17 event=RunCompleted step=6 next_action=NONE state_digest=…
```

### Effects CLI (`scripts/effects`)

```
$ go run ./scripts/effects --run-id 7f1c4b8e-…
idem:7f1c4b8e-…:3:toolu_03C  apply_fix  1
idem:7f1c4b8e-…:5:toolu_05E  open_pr    1
entries=2 distinct_keys=2 duplicates=0
```

Exit 0 if there are no duplicates, 1 if there are, 2 if the ledger file is missing.

## Constraints

- **Technical:** Go. `redis/go-redis/v9` (v9.22.0, already in `go.mod` since Phase 0). **No new dependencies**: UUIDs via the existing `google/uuid`, and Lua via `redis.NewScript`. No `miniredis`: unit tests use a fake `Store`, and anything needing real Redis goes behind `//go:build integration`. Requires Redis ≥ 7.0 for `SET NX GET`.
- **Architecture:** `internal/idempotency` depends on `internal/tools` and `internal/replay` (types only). It must not import `internal/agent` or `internal/kafka`. The Redis client is passed in through constructors, with no package-level state (CLAUDE.md). `cmd/worker` still must not link pgx (Phase 2 FR-26). CLAUDE.md's Architecture section (`internal/idempotency` goes from proposed to real, and `scripts/effects` is new), Commands section (new env vars, resume, effects CLI), and Current Status table must be updated.
- **Assumptions:** A-1 (one worker per run, operator-enforced, D-9). A-2: claimant and recoverer clocks agree to within a few seconds (a single laptop). The stale check and FR-13 compare `claimed_at`/`invoked_at` from one process with `now` from another. A-3: Redis is durable per D-8. Single node, no replication, no Redlock. That is a laptop model, **not** a claim of production-grade fencing, and it should be described that way.
- **Performance & scale:** Laptop scale. Claim + resolve adds 2 Redis round trips per side-effecting tool. Target is under 5ms p50 combined against local Redis with `appendfsync always`. Worst-case fail-closed latency before exit 4 is about 6s. Worst-case resume wait on a claim that isn't stale yet is `tool_timeout + 30s` (use `DAE_TOOL_TIMEOUT=20s` for manual tests).
- **Testing:** Unit tests (key/hash, Guard decision table with a fake store and fake tools, loop ordering, resume, fold additions, mocks + ledger) run under plain `go test ./...`. Tests needing Redis or Kafka go behind the `integration` tag. All existing Phase 2 unit tests keep passing, updated only for the `Execute` signature change and `run_tests`'s new ledger-driven behavior.
- **Out of scope for Phase 3:** leases and heartbeats (Phase 4); `run-commands`, the worker pool, and automatic crash detection (Phase 4); the approval flow and `AWAITING_APPROVAL` TTL interactions (Phase 4); `CancelRun` (Phase 5); the Prometheus counter for `tool_call_duplicates_total` (Phase 7 — Phase 3 logs `cache_hit` / `reconciled_found` and that's all); external-system fencing tokens; real tools and GitHub (Phase 9); an operator CLI to hand-resolve an `unresolved_side_effect`; the `budget:` key.

## Edge Cases & Error Handling

EC-1: **Crash at each point around a side-effecting tool** → exactly as in the crash-window table: W0/W1 execute once, W2 executes once (`open_pr`) or fails the run without executing (`apply_fix`), W3 adopts the recorded result (`open_pr`) or fails the run (`apply_fix`), and W4 is a cache hit. No window produces a second ledger entry for the same key.

EC-2: **Redis unreachable or slow at claim time** → fail closed (FR-8): the tool doesn't run, `redis_retry` ×5 then `fence_unavailable`, exit 4, no terminal event. The journal ends at `ToolInvoked`, so a later resume (Redis back up) enters at row 1 or row 3 and executes exactly once. Non-side-effect tools are unaffected mid-run.

EC-3: **Ambiguous claim** (the `SET NX GET` applied but the reply was lost) → the retry's `SET NX GET` returns the record this process wrote. Its `claim_token` matches, so it's `Acquired` (row 3). Across a process restart the token is gone, so recovery sees a claimed record from someone else and reconciles, which is still correct.

EC-4: **Redis unreachable at resolve time** → `resolve_failed` is logged and `ToolResulted` is published anyway (D-6). If that publish also fails (exit 3), recovery sees in-flight + `claimed` → stale → reconcile, the W3 path.

EC-5: **Redis data loss** (AOF disabled, `FLUSHALL`, the volume wiped without also wiping Kafka) → an in-flight invocation's key disappears and recovery treats it as never executed (row 1), which is a possible duplicate. Prevented by D-8 config (AC-18 checks it). Not otherwise defended in Phase 3. `docker compose down -v` wipes Kafka, Redis, and Postgres together, which is consistent.

EC-6: **Key expired by TTL** (resumed more than `ttl` after `ToolInvoked`) → FR-13: absent no longer counts as proof, so the Guard claims and then reconciles before executing (row 2). `open_pr` checks the ledger. `apply_fix` fails the run as `unresolved_side_effect`.

EC-7: **Two claimants race for the same key** (two goroutines or processes) → `SET NX` is atomic, so exactly one gets `Acquired`. The other gets the winner's record and waits (row 6) until it's resolved (cache hit) or stale.

EC-8: **Claimant pauses past `tool_timeout + 30s`, then wakes up** → another claimant may have taken over and re-executed (row 8), which is a possible duplicate. The original's `Resolve` returns `ErrClaimLost` and logs `resolve_failed`. This is the residual risk named in D-5, handed to Phase 4 (lease) and Phase 9 (reconciliation by branch name). Under A-1 it doesn't arise.

EC-9: **Tool returns a normal error** (e.g. bad args) → it's asserted that no effect happened. Resolved with `status: error`, journaled, and replayed from the cache on recovery. If the LLM then retries, that's a new `tool_use_id` and a new key, which is correct: it's a new invocation. The mocks only fail before writing to the ledger, so this contract holds for them.

EC-10: **`Execute` times out, is cancelled, or returns `ErrOutcomeUnknown`** → the key stays `claimed`, there's no `ToolResulted`, and the worker exits 4 (FR-12). Resume waits for staleness, then reconciles. SIGINT/SIGTERM counts as a crash here, as it did in Phase 2.

EC-11: **Existing record doesn't match** (different `args_hash`, `tool_name`, `run_id`, or `tool_use_id`, or corrupt JSON) → the tool is never executed, and the run gets `RunFailed{idempotency_record_mismatch}`. This is how a canonicalization change across a redeploy gets detected instead of silently duplicating (D-1).

EC-12: **Resuming a Phase 2-era run with an in-flight side-effecting tool** (`has_side_effect: true`, `idempotency_key: null`, never claimed) → it's impossible to know whether it ran, and there's no key to reconcile by. `RunFailed{unresolved_side_effect}` without executing.

EC-13: **Journaled `has_side_effect: true`, but the tool is no longer in the registry** on resume → `RunFailed{unresolved_side_effect}`. Journaled `has_side_effect: false` → re-execute without Redis (FR-22), even if the current registry now says the tool has side effects. The journal wins (D-3).

EC-14: **Two identical `tool_use` blocks in one `LLMResponded`** → two `tool_use_id`s, two keys, two executions, two ledger entries with **different** keys, so it's not counted as a duplicate. The LLM asked twice. The old hash-only key would have quietly swallowed the second one.

EC-15: **The LLM independently decides the same effect again after a resume** (same tool and args, new `tool_use_id`) → a new key, so the fence doesn't block it. This is a semantic duplicate, and it's out of scope for a key-based fence. Phase 9's `open_pr` reconciliation by branch name is the defense. _A crash before `LLMResponded` is acked can't cause it, because no tool from the lost response ever ran (Phase 2 EC-3)._

EC-16: **A-1 violated** (resume started while the original worker is still alive) → the fence still prevents a second execution for any claim that isn't stale (EC-7). But both processes publish at the same `sequence_number`. The fold returns `ErrConflictingEvent` and the projector halts on it as a poison message (Phase 2 EC-7), which is a loud failure, not a silent one. Phase 4's lease prevents this.

EC-17: **Resuming a terminal run** → print the state, publish nothing, exit 0 or 1. **Resuming an unknown `run_id`** → exit 2.

EC-18: **Ledger write fails** inside a mock (disk full, bad path) → `Execute` returns an error before the effect: `status: error`, no ledger entry. **Ledger unreadable in `Reconcile`** → fail closed (FR-16), exit 4.

EC-19: **`DAE_CRASH_AT` left set when resuming** → it fires again if the resumed run reaches the same tool and point. For example, `after_claim` on `open_pr` crashes again right after the row 8 takeover. That's documented and expected. Unset it before resuming.

EC-20: **Redis unreachable at worker startup** → exit 1 before publishing anything (FR-30). A fresh run never gets a `RunStarted`.

## Acceptance Criteria

### Unit (no infrastructure, `go test ./...`)

- [ ] AC-1: Table test for `Key` and `ArgsHash`. Args differing only in key order or whitespace hash the same. `{"n": 9007199254740993}` and `{"n": 9007199254740992}` hash differently (no `float64` rounding). The tool name is part of the hash. Different `tool_use_id`s give different keys. _(FR-1, FR-2, D-1)_
- [ ] AC-2: **Guard decision-table test**, one case per row 1–11, using a fake `Store`, a counting fake tool, and an injected clock. Each case asserts the `decision=` line, the `Execute` count, the `Reconcile` count, the store's final record, and the returned error (`errors.Is`). Rows 4, 5, 6-then-5, 7, 9, 10 assert `Execute` count **0**. _(FR-5–FR-17)_
- [ ] AC-3: **Fail closed.** A fake store whose `Claim` always errors: `Execute` count 0, exactly 5 attempts, `ErrFenceUnavailable`. One that errors twice and then succeeds: `Execute` count 1. A fake store that applies the claim and then returns an error (ambiguous write): the retry is `Acquired` through the token match, `Execute` count 1. _(FR-8, FR-10, EC-2, EC-3)_
- [ ] AC-4: **Ordering.** A shared call recorder across the fake publisher, fake store, and fake tool shows exactly `Publish(ToolInvoked) → Claim → Execute → Resolve → Publish(ToolResulted)` for `apply_fix`/`open_pr`, and no `Claim`/`Resolve` for `clone_repo`/`run_tests`. A publisher failing `ToolInvoked` means `Claim` count 0. _(FR-4, D-2)_
- [ ] AC-5: The journaled `ToolInvoked` for a side-effecting tool has `payload.idempotency_key == envelope.idempotency_key == Key(run_id, step, tool_use_id)`. For read-only tools both are `null`. `ToolResulted.resolution` and `was_replayed_from_cache` are consistent (FR-18). _(FR-1, FR-18)_
- [ ] AC-6: **Resume table test.** Starting from folded fixtures for W0–W5 (plus a `FINISH_RUN` state and a terminal state) with the matching fake-store contents, `Loop.Resume` produces the expected next event types and final `Execute` counts from the crash-window table. A W1 fixture whose journaled key differs from what `Key()` would compute today uses the **journaled** key. _(FR-3, FR-7, FR-19–FR-23, EC-13, EC-17)_
- [ ] AC-7: Fold test: `InFlightTool` carries `HasSideEffect`, `IdempotencyKey`, `InvokedAt` from the journaled `ToolInvoked`. A terminal `LLMResponded` gives `FINISH_RUN`, and `TerminalPayload()` returns `RunCompleted` / `RunFailed{llm_output_truncated}` / `RunFailed{malformed_llm_response}` for the three FR-10 cases. The live loop's terminal events are unchanged from Phase 2 (AC-5 of Phase 2 still passes). _(FR-19, FR-20)_
- [ ] AC-8: Mock tests using a temp-dir ledger: `run_tests` fails until an `apply_fix` entry exists for the run and passes after. `open_pr.Reconcile` returns `Applied` with the original result for a recorded key and not `Applied` for an unknown key. `apply_fix` doesn't implement `Reconciler` (checked by a type assertion). _(FR-24–FR-26)_
- [ ] AC-9: Phase 2's `go list -deps ./cmd/worker | grep jackc/pgx` still prints nothing, and every Phase 2 unit test passes.

### Integration (`go test -tags=integration ./...`, needs `docker compose up`)

- [ ] AC-10: **Real atomicity.** 50 goroutines `Claim` the same key at once, released together by a barrier: exactly 1 `Acquired`, and the other 49 get back the winner's `claim_token`. `TTL key` > 0. _(FR-5, EC-7)_
- [ ] AC-11: The CAS script against real Redis: `Resolve` with the wrong token → `ErrClaimLost`, value unchanged. With the right token → `resolved`, TTL refreshed. `TakeOver` swaps the token. `Resolve` on an already-`resolved` key → `ErrClaimLost`. _(FR-6)_

### Against real infrastructure (run by you, with evidence)

Setup for every run below: `rm -f .dae/mock-ledger.jsonl`, projector running, `DAE_MOCK_TOOL_DELAY=15s DAE_TOOL_TIMEOUT=20s` where a pause is needed.

- [ ] AC-12: **Happy path.** A fresh run completes (exit 0). `redis-cli --scan --pattern 'idem:<run_id>:*'` shows exactly 2 keys (`apply_fix`, `open_pr`), both `"state":"resolved"`. `SELECT event_type, idempotency_key FROM events WHERE run_id=… AND event_type='ToolInvoked'` shows non-null keys on exactly those two. `scripts/effects --run-id …` prints `duplicates=0`. _(FR-1, FR-4, FR-27)_
- [ ] AC-13: **Headline: already-completed step is not re-invoked (system spec AC-4).** `DAE_CRASH_AT=open_pr:after_resolve` → exit 137. Resume → `decision=cache_hit`, `ToolResulted{resolution: cached, was_replayed_from_cache: true}`, run completes. The `open_pr` ledger count for that key is **1**, and the result still says `PR #1`. The worker output and `scripts/effects` output are pasted into `docs/decisions/phase3.md`. _(FR-7, W4)_
- [ ] AC-14: **Crash after the effect, reconciled (system spec EC-1).** `DAE_CRASH_AT=open_pr:after_execute` → resume prints `decision=waiting` until stale, then `decision=reconciled_found`. `ToolResulted.resolution=reconciled`, and the result text equals the ledger entry's. Ledger count 1. _(FR-9, FR-14, W3)_
- [ ] AC-15: **Crash after the claim, before the effect.** `DAE_CRASH_AT=open_pr:after_claim` → resume → `reconciled_not_found`, `resolution=reexecuted`, ledger count 1. _(FR-14, W2)_
- [ ] AC-16: **No reconciler, so no guess.** `DAE_CRASH_AT=apply_fix:after_execute` → resume → `decision=unresolvable`, `RunFailed{error_class: unresolved_side_effect}`, exit 1. `scripts/replay` shows `status=FAILED` with `in_flight=apply_fix(…) side_effect=true`. `apply_fix` ledger count 1. _(FR-15, D-4)_
- [ ] AC-17: **Real `kill -9`, not injection.** Kill the worker during the 15s `open_pr` delay (right after its `ToolInvoked` line). Resume → waits, then `reconciled_not_found` → completes. Ledger count 1. _(EC-1, EC-10)_
- [ ] AC-18: **Redis down means fail closed (system spec AC-14).** During the `run_tests` delay at step 2, run `docker compose stop redis`. The worker finishes `run_tests`, calls the LLM, publishes `apply_fix`'s `ToolInvoked`, logs `redis_retry` ×5 and `fence_unavailable`, **never** writes an `apply_fix` ledger entry, and exits 4. `docker compose start redis`, resume → completes, `duplicates=0`. _(FR-8, EC-2)_
- [ ] AC-19: **Redis is durable.** After AC-15's crash (key `claimed`), run `docker compose restart redis`, then `docker compose down && docker compose up -d` (no `-v`). `redis-cli GET <key>` still returns the claimed record. `redis-cli CONFIG GET appendonly`, `appendfsync`, and `maxmemory-policy` return `yes`, `always`, `noeviction`. _(FR-29, D-8, EC-5)_
- [ ] AC-20: **Every journal crash point resumes cleanly.** For every `N` from 0 to the seq of the final `LLMResponded` in a fresh run, run with `DAE_CRASH_AFTER_SEQ=N` (exit 137), then resume. Each run ends `COMPLETED`. `scripts/effects` over the whole ledger prints `duplicates=0`. A small shell loop is fine. Save its output. _(FR-20–FR-23, W0/W1/W5)_
- [ ] AC-21: `docker compose stop redis`, then a fresh worker → exit 1 without printing a `run_id=` line. The `run-events` high-watermarks (`kafka-get-offsets.sh --topic run-events`) are the same before and after. With Redis still stopped, resuming an existing non-terminal run also exits 1, and that run's event count in `scripts/replay` doesn't change. _(FR-30, EC-20)_
