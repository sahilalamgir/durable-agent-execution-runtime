# Phase 0 decisions

## Kafka runs in KRaft mode with three listeners
- One Kafka container that is both broker and controller (KRaft), so there is no separate Zookeeper container to run and explain.
- Three listeners: `PLAINTEXT` advertised as `kafka:19092` (for containers on the compose network), `PLAINTEXT_HOST` advertised as `localhost:9092` (for Go processes on my Mac), and `CONTROLLER` (Kafka's internal quorum traffic, not published).
  - A Kafka broker tells every client "reconnect to me at this address", and that address has to work from where the client is. From my Mac the name `kafka` doesn't resolve, because it only exists inside the compose network. From another container, `localhost` would mean the container itself.
  - Rejected: one listener advertised as `localhost:9092`. It works for the script today but breaks the moment a worker runs in a container (Phase 6).
  - I never hit this failure. The split was there from the start, so the script connecting on `localhost:9092` worked first try.

## Healthchecks in compose, not a sleep in the Go script
- Each service has a healthcheck (`kafka-broker-api-versions.sh`, `redis-cli ping`, `pg_isready`), so `docker compose ps` shows `healthy` when the service is ready.
  - Rejected: a sleep/retry loop in the Go script. It hides "is the infrastructure up" inside application code.
  - Limits I found: "healthy" only means the broker process is up, not that any topic exists (that is the topic bug below). `pg_isready` runs inside the Postgres container, so it doesn't prove port 5432 is reachable from a Go process on my Mac.

## Postgres is only checked by its healthcheck, no Go code touches it
- AC-1 only asks the Go script to touch Kafka and Redis, so Postgres just has to start and report healthy.
  - Rejected: adding a Postgres ping and the `pgx` dependency now. It would be the first Postgres code in the repo, written before Phase 2 decides how the projector connects, for a dependency nothing else uses yet.

## No volumes in Phase 0
- The containers are disposable. `docker compose down` removes everything, which is how I proved the script works against a broker with no topics. Durability only became a requirement in Phase 2.

## Create the topic explicitly instead of relying on auto-creation
- First failure: `[3] Unknown Topic Or Partition`. `kafka-go`'s `Writer` tells the broker not to auto-create a missing topic unless `AllowAutoTopicCreation: true` is set. The broker's own default was fine, so the bug was in the client. Nothing needed to change in `docker-compose.yml`.
- Setting the flag wasn't enough. Against a broker with no topics the script still failed on the first run, then passed on the second run because the topic now existed. Auto-creation is asynchronous, so the request that triggers it can be rejected before the topic is ready.
- Solution: call `conn.CreateTopics` before producing. `kafka-go` treats "already exists" as success, so it is safe on every run.
  - Rejected: keeping the flag and retrying the produce. See the next decision.

## Write once, synchronously, and don't retry
- My first fix for the race was a retry loop around `kafka.Writer`. The adversarial reviewer found two problems with it.
  - Retry can duplicate: if a write reaches the broker but the acknowledgement is lost, Go can't tell "it didn't happen" from "it happened and I didn't hear back". The retry sends a second copy. `kafka-go` has no idempotent producer, so nothing deduplicates it, and the consumer reads one copy and never sees the other.
  - `kafka.Writer` is async: `Close()` flushes any pending batch even after the context deadline. A produce reported as failed can still land on the topic during cleanup.
- Solution: one write through `kafka.Conn` with a 10 second write deadline. It either succeeds before the deadline or returns an error, with nothing left running in the background and nothing to retry. With the topic created up front there is no reason to retry anyway.
- This is a small version of what Phase 3 is for. A side effect can't be retried safely unless something (a claim key) can tell "already happened" apart from "didn't hear back". Here I removed the retry. In Phase 3 recovery has to re-attempt steps, so it needs the claim.

## Each run uses a fresh value and verifies it
- The value is `phase0 connectivity check <unix nanoseconds>`. The same value goes into Kafka and Redis, and each read is compared to it.
  - Why: with a constant string, a leftover message or Redis key from an earlier run could make "OK" print without this run's write working.
  - This is a different problem from the duplicate write above. The fresh value protects the read side from being fooled by old data. It does nothing about a write happening twice, because both copies would carry the same value.
  - Consequence: a stale record makes the check fail loudly instead of falsely passing. The failing run consumes it (`ReadMessage` commits the offset), so the next run passes.

## Every I/O step is bounded, and `Close` errors are logged
- Each Kafka and Redis step gets its own 10 second timeout derived from the context. The first Redis version had none, which is the opposite of the project rule that a Redis check must time out cleanly.
- `defer conn.Close()` silently drops its error, so `closeQuietly` logs it instead.

## Deliberately left for later phases
- The review flagged these, and none is a Phase 0 bug.
  - `ReadMessage` commits the offset before the message is processed. That's fine for a script that only prints it, but a worker consuming `run-commands` (Phase 4) needs `FetchMessage` and a commit only after the result is durable.
  - The topic has one partition, so this environment can't show a consumer-group rebalance.
  - `pg_isready` is a narrower proof than "a Go process can reach Postgres".

Phase 0 proves connectivity only. It does not test durability, ordering, or any idempotency.
