Phase 3 Technical Design Plan — Idempotent Tool Execution (Redis)

Source of truth: .claude/specs/phase3-idempotency.md (FR/EC/AC/D numbers below refer to it). This plan is how to build it on top of the actual Phase 2 code. No code is written until this is approved.

Context

Phase 2 journals ToolInvoked before a tool runs, but after a crash ToolInvoked-without-ToolResulted can't say whether the side effect happened, so the loop refuses to resume it (Loop.Run returns "unexpected next_action" on RESOLVE_IN_FLIGHT_TOOL). Phase 3 adds a Redis claim/resolve record written before the effect, one shared Guard.Run code path for live and recovery, a resume mode for the worker, and an independent file ledger + crash injection to prove no window duplicates an effect.

Facts about the existing code that shape the design

- Loop.Run (internal/agent/loop.go:124) always starts with RunStarted from an empty state; cmd/worker always mints a new run_id. No resume path exists.
- publishApplyPrint (loop.go:327) is the single publish chokepoint: NewEnvelope → publishWithRetry → replay.Apply → crash check → print seq= line.
- events.NewEnvelope hardcodes IdempotencyKey: nil. ToolInvokedPayload.IdempotencyKey, ToolResultedPayload.WasReplayedFromCache, and the Postgres idempotency_key column (projected from the envelope field) already exist but are never populated.
- replay.RunState.InFlightTool is a \*ToolCall{ToolUseID, ToolName, ToolArgs}; the fold discards has_side_effect and the key from ToolInvoked.
- The fold rejects a second ToolInvoked or second ToolResulted for the same tool_use_id (ErrToolOrder). So a resumed in-flight tool must publish only ToolResulted, never re-publish ToolInvoked. A tool still in PendingToolCalls (W0) does publish ToolInvoked.
- computeNextAction returns NONE (status RUNNING) when the last LLMResponded is terminal/has no tool_use — the terminal event is written by callLLMAndJournal inline. That's the seam for FINISH_RUN.
- run_tests has an in-process call counter (resets on resume); mock tools call mockToolDelay first, and have no ledger.
- go-redis v9.22.0 and google/uuid are already in go.mod. No new dependencies. Existing fakes to reuse: fakePublisher, stubLLMClient, countingTool, newTestJournal, replay fx\* fixtures.

Prerequisite (before any code)

Apply the "System-spec amendments" table from the Phase 3 spec to .claude/specs/system-spec.md (idem key row, TTL note, flagged Phase 2 note, Redis durability note, ToolInvoked/ToolResulted payload changes, EC-1, EC-9, new error_class values). CLAUDE.md rule: spec first, then code.

Package layout and dependency direction

internal/tools ← Invocation, Tool (new Execute sig), Reconciler, ErrOutcomeUnknown, Ledger, mocks
internal/replay ← ToolCall gains HasSideEffect/IdempotencyKey/InvokedAt; ActionFinishRun; TerminalPayload()
internal/idempotency ← NEW. Key, ArgsHash, IdemRecord, Store (+RedisStore), Guard. imports tools + replay (types only)
internal/agent ← Loop gains Guard dependency; drive(); Resume(); terminal events via TerminalPayload
cmd/worker ← Redis client, PING, resume mode, DAE_CRASH_AT, exit codes
scripts/effects ← NEW scripts/replay ← extra output fields
internal/idempotency must not import internal/agent or internal/kafka. cmd/worker must still not link pgx (AC-9 check).

Build order (each slice compiles and passes go test ./... before the next)

Slice 1 — Tools interface, ledger, mocks (FR-24–26, D-7)

- internal/tools/tool.go: add Invocation{RunID, Step, ToolUseID, IdempotencyKey \*string}; change Tool.Execute(ctx, inv, rawArgs); add Reconciler, ReconcileResult, ErrOutcomeUnknown.
- internal/tools/ledger.go (new): Ledger with Record (single write() on O_APPEND, then fsync), FindByKey, Entries. Missing file = empty for reads. Constructed via NewLedger(path) and passed to mocks, no package state.
- Mocks: apply_fix and open_pr write to the ledger after the delay. open_pr result = opened PR #<1+count of open_pr entries for run_id>: <title> and implements Reconciler via FindByKey. apply_fix deliberately does not. run_tests fails until an apply_fix entry exists for the run (removes the counter). clone_repo only gets the new signature.
- Update registry_test.go and countingTool for the signature change; add ledger/mock tests (AC-8) using t.TempDir().

Slice 2 — Replay additions (FR-19, FR-20)

- replay/state.go: extend ToolCall (HasSideEffect, IdempotencyKey, InvokedAt); add ActionFinishRun.
- replay/fold.go: applyToolInvoked/deriveToolState carry the journaled fields (InvokedAt = envelope occurred_at). computeNextAction: last-step-has-no-tool_use (RUNNING) → FINISH_RUN instead of NONE.
- RunState.TerminalPayload(): pure; returns RunCompleted / RunFailed{llm_output_truncated} / RunFailed{malformed_llm_response} — logic lifted out of callLLMAndJournal. (Verify LLMResponded payload carries stop_reason; needed to tell truncated from malformed.)
- Update Phase 2 fold tests that asserted NONE for that case; add AC-7 tests. Digest is unaffected (new fields not in digestView), so Phase 2 digests stay stable.

Slice 3 — internal/idempotency core (FR-1–3, FR-5–6, D-1, D-8)

- key.go: Key, ArgsHash (json.Decoder.UseNumber, sorted keys, no whitespace; tool name + \x00 prefix). Table test AC-1, including the 2^53+1 case.
- record.go: IdemRecord, RecordState.
- store.go: Store interface + RedisStore. Claim = SET NX GET EX; Get; Resolve/TakeOver via one redis.NewScript CAS Lua (state==claimed && token match; refresh TTL). Client options per spec (MaxRetries: -1; the Guard owns retries).
- Integration tests AC-10 (50-goroutine barrier claim) and AC-11 (CAS) behind //go:build integration.

Slice 4 — Guard.Run (FR-4, 7–17, 28; the heart of the phase)

Structure as small single-purpose functions, one per decision-table row group, so the table maps to code and to tests:

- Guard fields: store, now func() time.Time, sleep func(ctx, d) (injected so tests don't wait), cfg (toolTimeout, staleGrace=30s, ttl), out io.Writer, crash CrashHook.
- Run: token = fresh UUID → claim (with retry) → dispatch on the reply:
  - acquired & invoked_at fresh → execute+resolve (row 1); acquired & too old → reconcile with own token (row 2, FR-13).
  - existing with our token → treat as acquired (row 3, FR-10).
  - verify(rec, inv) mismatch/bad JSON → ErrRecordMismatch (row 4).
  - resolved → cached (row 5); claimed not stale → wait polling 1s, waiting line at most every 5s (row 6); stale → reconcile (rows 7–9).
  - TakeOver → ErrClaimLost → restart at claim, max 3 lost races (row 11).
- withRetry(op) helper: 5 attempts, 500ms per-attempt timeout, 200ms→2s backoff, prints redis_retry, ends in ErrFenceUnavailable (row 10, FR-8, FR-16). Covers both Redis and Reconcile errors.
- execute: bound ctx by tool_timeout; ErrOutcomeUnknown or ctx done → print outcome_unknown, do not resolve, return ErrOutcomeUnknown (FR-12). Other errors → resolve status: error. Resolve failure → print resolve_failed, still return the Outcome (D-6).
- CrashHook interface with AfterClaim/AfterExecute/AfterResolve(toolName) called at the three points (FR-31). Nil-safe no-op default.
- Every decision prints fence key=… tool=… decision=… (FR-28) via out.
- Tests AC-2 (one case per row, fake store + counting tool + injected clock), AC-3 (fail closed, ambiguous write), asserting decision line, Execute count, Reconcile count, final record, errors.Is.

Slice 5 — Agent loop: one drive, live and resume (FR-3, 4, 7, 18, 20–23)

- JournalConfig/Loop get a Guard (interface, so tests can substitute) and a registry as before.
- Envelope key: in publishApplyPrint, after NewEnvelope, set env.IdempotencyKey from the ToolInvokedPayload key. One place, so payload and envelope keys can't diverge (FR-1). NewEnvelope itself is unchanged, so the existing codec_test.go:110 assertion stays valid.
- Loop.Run = publish RunStarted, then drive(state). Loop.Resume(state) = drive(state). drive handles every NextAction:
  - CALL_LLM, FAIL_MAX_STEPS: as today. callLLMAndJournal now only journals LLMResponded (terminal events no longer inline).
  - FINISH_RUN: publish state.TerminalPayload() — same method for live and resumed finishes.
  - EXECUTE_TOOL (pending): compute the key once (Key(run, step, tool_use_id) if has_side_effect), publish ToolInvoked (ack), then go to the shared tool path.
  - RESOLVE_IN_FLIGHT_TOOL: take the key/has_side_effect from the journaled InFlightTool (never recompute, FR-3), no ToolInvoked publish, go to the shared tool path.
  - Shared tool path: journaled has_side_effect=false → Execute directly, no Redis (FR-22), resolution executed. true → Guard.Run. Unknown tool with journaled side effect, or side-effecting with null key (Phase 2-era run) → RunFailed{unresolved_side_effect} (EC-12/13).
  - Map Guard errors: ErrUnresolvable→RunFailed{unresolved_side_effect, retryable:false}; ErrRecordMismatch→RunFailed{idempotency_record_mismatch}; ErrFenceUnavailable/ErrOutcomeUnknown→ return a sentinel with no terminal event.
  - Publish ToolResulted with resolution and was_replayed_from_cache == (resolution=="cached"). Add Resolution to ToolResultedPayload (additive; old events decode as "").
- Tests AC-4 (shared call recorder proves Publish(ToolInvoked)→Claim→Execute→Resolve→Publish(ToolResulted), none for read-only tools), AC-5, AC-6 (resume table from W0–W5 fixtures, journaled-key-wins fixture, terminal/FINISH_RUN states). Existing TestLiveReplayEquivalence, TestWriteAhead must keep passing.

Slice 6 — Worker wiring, config, infra (FR-21, 29–31, D-8, D-9)

- docker-compose.yml: redis command --appendonly yes --appendfsync always --maxmemory-policy noeviction, named volume redis-data:/data (add to top-level volumes); update the "disposable" comment.
- cmd/worker/main.go: new env (DAE_REDIS_ADDR, DAE_IDEMPOTENCY_TTL, DAE_TOOL_TIMEOUT, DAE_MOCK_LEDGER_PATH, DAE_RESUME_RUN_ID, DAE_CRASH_AT). Startup order: EnsureTopics → PING Redis 5s (exit 1 before any publish) → validate ttl > tool_timeout+30s (panic: startup config).
  - fresh: generate run_id, print, Loop.Run.
  - resume: replay.Replay via KafkaEventSource (never Postgres), print run_id=… resumed_from_seq=… next_action=…; terminal run → print state, exit 0/1; not found → exit 2; else Loop.Resume. Continue publishing from NextSequence.
  - Exit codes: add 4 (fence unavailable / outcome unknown). Parse DAE_CRASH_AT into a CrashHook that os.Exit(137) the first time.
- .gitignore: add .dae/.

Slice 7 — Evidence tooling (FR-27, FR-32)

- scripts/effects (new): flags --ledger, --run-id; one row per key with count, summary entries= distinct_keys= duplicates=; exit 1 on duplicates, 2 on missing ledger. Uses Ledger.Entries. Defines "duplicate = ≥2 entries with same idempotency_key" (Phase 8 depends on this).
- scripts/replay: extend in_flight= with side_effect= idem_key= invoked_at=; print resolution= for ToolResulted summaries.

Slice 8 — Docs and process

- Update CLAUDE.md (Architecture: internal/idempotency, scripts/effects real; Commands: new env vars, resume, effects; Status table).
- Add anything corrected twice to go-conventions.
- After implementation: manual diff review of Guard/drive by you, adversarial-reviewer on the diff, then run AC-12–21 yourself (/verify-ac 3), and finally write docs/decisions/phase3.md yourself (I'll ask guiding questions).

Key design choices and why

- One code path (drive + Guard.Run) for live and recovery — no separate recovery logic to drift (D-3). The journal, not current code, is authoritative for key and has_side_effect.
- Injected clock/sleep/store/crash hook in Guard — every decision-table row is unit-testable with no Redis and no real waiting; matches the "no global mutable state" rule.
- Guard owns retries, go-redis retries disabled — makes redis_retry visible and testable.
- Terminal events via TerminalPayload() — live finish and resumed finish can't diverge.
- Ledger independent of Redis — evidence of "happened once" must not come from the mechanism under test.

Risks / things to watch

1. Changing NONE→FINISH_RUN and moving terminal events out of callLLMAndJournal touches Phase 2 behavior; guard with the existing live/replay equivalence and write-ahead tests, and keep event order byte-identical.
2. The fold's one-invoke/one-result rule: resume paths must never re-publish ToolInvoked for an in-flight tool.
3. DAE_CRASH_AT left set on resume re-fires (EC-19) — document in CLAUDE.md commands.
4. Waiting on a non-stale claim takes tool_timeout+30s; use DAE_TOOL_TIMEOUT=20s for manual runs.
5. Residual risks stated in the spec (paused claimant past staleness, A-1 single worker, file ledger not shared across pods) are accepted, not solved.

Verification

- go test ./... (AC-1–9) green; go list -deps ./cmd/worker | grep jackc/pgx prints nothing.
- docker compose up, then go test -tags=integration ./... (AC-10, 11 plus Phase 2 integration tests).
- Manual, with evidence saved for the decisions doc: AC-12 happy path, AC-13 open_pr:after_resolve → cache_hit, AC-14 after_execute → reconciled_found, AC-15 after_claim → reconciled_not_found, AC-16 apply_fix:after_execute → unresolved_side_effect, AC-17 real kill -9, AC-18 docker compose stop redis → exit 4, AC-19 Redis durability across down/up, AC-20 crash-after-every-seq loop, AC-21 Redis-down startup. Every run ends with go run ./scripts/effects showing duplicates=0.
