- Worker writes to Kafka first, then a separate projector copies it into Postgres
  - The worker only ever writes events to Kafka. A separate process (the projector) reads Kafka and inserts into Postgres. Kafka is the source of truth. Postgres is a copy for SQL queries.
  - Rejected: the worker writing to both Kafka and Postgres itself.
    - Two separate writes can't be atomic. If the worker crashes between them, Kafka and Postgres disagree and nothing says which one is right (the dual-write problem).
    - If Postgres went first and the worker crashed before Kafka, the event would exist in Postgres but not in Kafka. Replay reads Kafka, so that event would be lost from the record that matters.
    - With Kafka first, a crash before the Postgres write is harmless. The projector resumes from its last committed Kafka offset and inserts the missed event. A duplicate insert is ignored by `UNIQUE(run_id, sequence_number)` with `ON CONFLICT DO NOTHING`, and the offset is only committed after the Postgres commit succeeds.
    - Postgres can't be what a worker replays from, because the projector lags behind. A worker reading Postgres could miss the last events and reuse sequence numbers Kafka already has.
    - A status row isn't enough to resume a run. The events hold the conversation history, the tool_use IDs, and whether a tool was in flight. The state is computed from them.

- Journal first, act second, and replay uses the same code as the live loop
  - for every step the worker publishes the event, waits for Kafka's ack, applies it to `RunState`, and only then acts (calls the tool or the LLM). The live loop gets its conversation history from `RunState`, the same fold that replay uses. There is no separate in-memory message list.- If the worker crashes after the event but before the tool runs, replay shows "tool in flight". That is recoverable. The reverse order (act first, log after) can leave a tool that ran with no record of it, which is unrecoverable.
  - One code path means live execution and replay can't drift apart.
  - the LLM call itself happens before its response is journaled. A crash in that window loses the response and replay simply asks again. That costs tokens, not correctness.

- The state fingerprint (digest) hashes canonical data, not raw JSON
  - `state_digest` is a SHA-256 of the state after normalizing it. The state is turned into JSON, parsed into a generic value, and re-encoded (which sorts keys), then hashed.
  - the digest is how I prove the live worker, a Kafka replay and a Postgres replay all agree. Postgres `JSONB` reorders keys and normalizes whitespace and numbers, so hashing raw bytes would make identical states look different.
  - numbers pass through `float64`, so two different very large integers in tool args could collide. Fine for the mock tools, but maybe should be reworked for real ones.
  - Bug this caught: the first version ignored `NextSequence` and `is_terminal`, so two different states could hash the same. Adversarial review found it and it was fixed.

Phase 2 does not resume a crashed run. It detects and reports what is stuck (`RESOLVE_IN_FLIGHT_TOOL`). Acting on that safely needs Redis fencing in Phase 3.
