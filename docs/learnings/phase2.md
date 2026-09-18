# Phase 2 learnings

## What changed
Phase 1 kept everything in memory, so a crash lost the run. Now every step is written to Kafka before it happens, and the exact state can be rebuilt from Kafka alone after a crash.

## How it works
1. `cmd/worker` starts a run, prints its `run_id`, and calls `agent.Loop`.
2. For each step the loop publishes an event to Kafka (`internal/kafka`), waits for the ack, then applies the event to `RunState` (`internal/replay`).
3. `RunState.NextAction` tells the loop what to do next: call the LLM, run a tool, or stop. The loop builds the LLM's message history from `RunState` too.
4. `cmd/projector` reads the same Kafka topic and inserts each event into Postgres. It is the only thing that writes to Postgres.
5. `scripts/replay` reads Kafka directly (not Postgres), folds the events into a `RunState`, and prints it.

Events per run: `RunStarted`, then per LLM step `LLMResponded`, `ToolInvoked`, `ToolResulted`, and finally `RunCompleted` (18 events for the 6-step demo task).

## Reading a worker line
`seq=2 event=ToolInvoked step=1 next_action=RESOLVE_IN_FLIGHT_TOOL state_digest=94c0...`
- `seq`: position in this run's event list, starting at 0.
- `event`: what was just saved to Kafka.
- `step`: which LLM round-trip this belongs to.
- `next_action`: what should happen next, computed from all events so far.
- `state_digest`: SHA-256 fingerprint of the whole state at this point.

## The crash test that proves it
Ran the worker with `DAE_MOCK_TOOL_DELAY=15s`, then `kill -9` during the tool pause. The last worker line was `seq=2 ToolInvoked ... state_digest=94c01cb5...`. Replay from Kafka printed `next_action=RESOLVE_IN_FLIGHT_TOOL in_flight=clone_repo(...)` and the same `state_digest=94c01cb5...`. Same fingerprint means the state was rebuilt correctly from Kafka with no help from the dead process. `RESOLVE_IN_FLIGHT_TOOL` means a tool may or may not have finished, and replay honestly doesn't know.

Also verified: crash after specific event numbers (exit code 137), Kafka and Postgres replay give the same digest, worker completes with Postgres down, worker exits 3 with Kafka down, and data survives `docker compose down` / `up`.

## Bugs that taught me something
- **`toolset_name`:** I saved the LLM response by re-encoding the parsed Go struct, which turned a missing field into an empty one, and the API rejected it on replay. Fix: save the exact original bytes.
- **Kafka volume:** the compose file looked durable but mounted the wrong path, so restarts silently wiped all events. Only a real `down`/`up` test caught it. Fix: mount the path Kafka actually writes to.

## Next steps
- Phase 3: Redis fencing so an in-flight tool is never run twice.
- Phase 4: worker pool that actually resumes crashed runs.
- Known gaps left on purpose: a second in-flight tool isn't rejected, step numbers aren't validated, the projector doesn't check for sequence gaps.
