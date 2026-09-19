# Phase 3 in plain terms

## What changed

Phase 2 gave you a journal, so after a crash you can rebuild exactly where a run stopped. The gap is that ToolInvoked with no ToolResulted means "the tool may or may not have run." Replay can't tell "never started" from "PR opened, then crashed before writing it down." Phase 3 closes that gap with a small record in Redis, written before the tool fires.

## New concepts

1. Idempotency (vs. a simple retry)
   A retry means "try again." Idempotency means "trying again is safe because the effect happens at most once." Retrying open_pr blindly gives you PR #1 and PR #2. Retrying clone_repo is harmless. That's why tools carry has_side_effect.

2. Check-then-act is a race
   Naive version: "if no PR exists, open one." Two workers (or a crashed-and-resumed one) can both check, both see nothing, and both act. The fix is to make check and claim a single atomic step.

3. Redis SET NX (atomic claim)
   SET key value NX means "write only if the key doesn't exist," and Redis does it as one indivisible operation. If 50 workers try at once, exactly one wins. The rest learn they lost, and Phase 3 uses NX GET so they also see the winner's record.

4. The three-state record: absent → claimed → resolved
   Key: idem:{run_id}:{step}:{tool_use_id}.

- absent means nobody has started. Proof it never ran, but only if Redis is durable and the key hasn't expired.
- claimed means someone started, and the outcome is unknown.
- resolved means it finished and the result is stored. Anyone asking again gets a cache hit and doesn't re-run it.

5. Claim before executing

- If you only write the result after the tool succeeds, a crash mid-tool leaves no trace. Claiming first means a crash leaves a visible claimed record, which is what recovery needs to see.

6. Stale claims and reconciliation

- A claimed record with a dead owner is ambiguous. So the Guard waits until the claim is stale (older than tool_timeout + 30s), then asks the tool "did this actually happen?" That question is Reconcile.

- open_pr can answer it (it checks the ledger), so it adopts the result or safely re-runs.
- apply_fix can't answer, so the run fails with unresolved_side_effect. Failing beats guessing a duplicate.

7. Compare-and-set (CAS) with a claim token

- Resolving or taking over a claim must only work if you still hold it. Each claim gets a random claim_token, and a small Lua script in Redis checks the token and writes in one atomic step. If someone else took over, you get ErrClaimLost and write nothing.

8. Fail closed

- If Redis is unreachable, the worker does not run the tool. It retries 5 times, then exits with code 4 and publishes no terminal event, so the run stays resumable. Failing open ("Redis is down, run it anyway") would be exactly how duplicates happen.

9. Durable Redis

- "Absent key means never ran" only holds if Redis doesn't lose writes. So compose now runs Redis with AOF appendfsync always, noeviction, and a named volume. Phase 0 called Redis "disposable," and this reverses that.

10. An independent "outside world" ledger

- The mock tools append to a file (.dae/mock-ledger.jsonl) standing in for GitHub. The proof that something happened once must not come from the same Redis mechanism that's supposed to enforce it.

## What happens under the hood (one side-effecting tool)

1. Publish ToolInvoked (with the idempotency key) to Kafka and wait for the ack.
2. Claim the key in Redis (SET NX GET).
3. Execute the tool. The mock waits, then writes a ledger line.
4. Resolve the key in Redis (CAS: claimed → resolved, with the result).
5. Publish ToolResulted to Kafka.

After a crash, the recovery path runs the same Guard.Run, driven by the journaled key rather than recomputed values.

### Example: crash windows for open_pr

┌────────────────────────────────────┬────────────┬────────┬───────────────────────────────────────────────────────┬────────┐
│ Crash point │ Redis says │ Ledger │ Resume does │ Result │
├────────────────────────────────────┼────────────┼────────┼───────────────────────────────────────────────────────┼────────┤
│ Before claim │ absent │ 0 │ claims and runs │ 1 PR │
├────────────────────────────────────┼────────────┼────────┼───────────────────────────────────────────────────────┼────────┤
│ After claim, before effect │ claimed │ 0 │ waits until stale, reconciles: "not found," re-runs │ 1 PR │
├────────────────────────────────────┼────────────┼────────┼───────────────────────────────────────────────────────┼────────┤
│ After effect, before resolve │ claimed │ 1 │ waits until stale, reconciles: "found," adopts result │ 1 PR │
├────────────────────────────────────┼────────────┼────────┼───────────────────────────────────────────────────────┼────────┤
│ After resolve, before ToolResulted │ resolved │ 1 │ cache hit │ 1 PR │
└────────────────────────────────────┴────────────┴────────┴───────────────────────────────────────────────────────┴────────┘

Every window ends with exactly one PR. For apply_fix, the two "claimed" rows end in RunFailed{unresolved_side_effect} because it has no reconciler.

## New things you'll run

- Worker resume: DAE_RESUME_RUN_ID=<uuid> go run ./cmd/worker replays the run from Kafka and continues it. This is the first time a crashed run actually gets picked back up.
- Crash injection: DAE_CRASH_AT=open_pr:after_execute (or after_claim, after_resolve) kills the worker at an exact point. You'll also do a real kill -9.
- go run ./scripts/effects: counts ledger entries per key and prints duplicates=0. This is the pass/fail evidence, and Phase 8 reuses it.
- Redis checks: redis-cli GET idem:... to see claimed/resolved, and docker compose stop redis to watch fail-closed (redis_retry ×5, then exit 4).
- New log lines: fence key=... decision=acquired | cache_hit | waiting | reconciled_found | ...

## Reading a fence line

fence key=idem:7f1c…:5:toolu_05E tool=open_pr decision=cache_hit

- key: which invocation this is.
- tool: which tool.
- decision: what the Guard concluded. cache_hit means "already done, not re-running."

## Known limits, on purpose

- One worker per run, and you enforce it by only resuming after the old process is dead (Phase 4 adds leases).
- A worker that freezes past the stale threshold and then wakes up could still duplicate. Fully closing that needs the external system to reject stale tokens, and GitHub doesn't.
- The file ledger is local-only, so Kubernetes will need a shared equivalent later.
