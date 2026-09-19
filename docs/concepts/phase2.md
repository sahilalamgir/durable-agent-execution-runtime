# Phase 2 in plain terms

## What changed

Phase 1 kept everything in one process's memory, so a crash lost the run. Phase 2 writes every step to Kafka before acting on it, so after a crash the exact state can be rebuilt from Kafka alone. Postgres gets a copy for queries, but it is never the source of truth.

## New concepts

1. Event sourcing
   Don't store "where the run is now." Store every step that happened, in order, and compute "where it is now" by replaying them. Example: `RunStarted, LLMResponded, ToolInvoked, ToolResulted` tells you the run finished step 1.

2. The event journal (append-only log)
   Events are only ever added, never edited. That gives you a permanent, ordered history, and it's why a dead worker's state can be rebuilt by someone else.

3. Kafka, topic, partition
   Kafka is a durable, ordered log. A topic (`run-events`) is split into partitions (6 here). Messages are keyed by `run_id`, so one run always lands in the same partition and stays in order.

4. Write-ahead: journal first, act second
   The worker publishes the event, waits for Kafka's ack, and only then does the thing. A crash after the event but before the action leaves a visible "in flight" record. The reverse order could leave an action with no record.

5. The fold (replay)
   A pure function: start empty, apply each event in order, get `RunState`. It does no I/O, no LLM calls, no tool calls. Same events in, same state out, every time.

6. One code path for live and replay
   The live loop builds its LLM conversation from the same fold that replay uses. There's no separate in-memory copy, so the two can't drift apart.

7. `NextAction`
   The fold also says what to do next: `CALL_LLM`, `EXECUTE_TOOL`, `RESOLVE_IN_FLIGHT_TOOL`, or `NONE`. `RESOLVE_IN_FLIGHT_TOOL` means "a tool started and I don't know if it finished." Phase 2 only reports this. Phase 3 acts on it.

8. State digest
   A SHA-256 fingerprint of the whole state. If the live worker and a replay print the same digest, the rebuild is correct. It hashes normalized data, not raw JSON, because Postgres `JSONB` reorders keys.

9. Dual-write problem (why Kafka first, then Postgres)
   Writing to two systems can't be atomic. A crash between the two writes leaves them disagreeing. So the worker writes only to Kafka, and a separate projector copies into Postgres.

10. Projector and at-least-once delivery
    The projector reads Kafka and inserts into Postgres. It commits its Kafka offset only after the Postgres commit succeeds. If it crashes in between, it re-reads the event, and `UNIQUE(run_id, sequence_number)` with `ON CONFLICT DO NOTHING` absorbs the duplicate.

## What happens under the hood (one step of a run)

1. The worker builds an event and publishes it to Kafka (`acks=all`), retrying if that fails.
2. After the ack, it applies the event to `RunState` and prints a line.
3. `NextAction` says what to do: call the LLM, run a tool, or stop.
4. The worker does that, and the result becomes the next event.
5. Separately, the projector reads the same topic and inserts each event into Postgres.
6. `scripts/replay` reads Kafka directly, folds the events, and prints the state.

### Example: a 3-event slice

| seq | event | next_action after it |
|---|---|---|
| 1 | `LLMResponded` | `EXECUTE_TOOL` |
| 2 | `ToolInvoked` | `RESOLVE_IN_FLIGHT_TOOL` |
| 3 | `ToolResulted` | `CALL_LLM` |

Kill the worker after seq 2 and replay shows `RESOLVE_IN_FLIGHT_TOOL` with the same digest the worker last printed.

## New things you'll run

- Worker: `go run ./cmd/worker` prints its `run_id` and one line per event.
- Projector: `go run ./cmd/projector` copies Kafka into Postgres and logs each insert.
- Replay: `go run ./scripts/replay --run-id <uuid> --source kafka` rebuilds and prints the state (`--source postgres` to cross-check).
- Crash tests: `kill -9` the worker during `DAE_MOCK_TOOL_DELAY=15s`, or use `DAE_CRASH_AFTER_SEQ=N` to die at an exact event.

## Reading a worker line

`seq=2 event=ToolInvoked step=1 next_action=RESOLVE_IN_FLIGHT_TOOL state_digest=94c0…`

- `seq`: this event's position in the run, starting at 0.
- `event`: what was just saved to Kafka.
- `step`: which LLM round-trip it belongs to.
- `next_action`: what should happen next.
- `state_digest`: fingerprint of the whole state so far.

## Known limits, on purpose

- Phase 2 detects a stuck run but doesn't resume it. Resume plus tool safety is Phase 3.
- Only one worker runs at a time and nothing enforces it yet (leases come in Phase 4).
- Very large integers in tool args could collide in the digest, which is fine for the mock tools.
