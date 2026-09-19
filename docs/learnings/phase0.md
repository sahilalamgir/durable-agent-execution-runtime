# Phase 0 learnings

## What changed
Before this phase nothing ran. Now `docker compose up` starts Kafka, Redis and Postgres, and `go run ./scripts/connectivity` proves a Go process on my Mac can talk to Kafka and Redis. There is no agent, no events and no durability yet. This is only the plumbing the later phases stand on.

## How it works
1. `docker-compose.yml` defines three services on compose's default network, with ports published to `localhost` (Kafka 9092, Redis 6379, Postgres 5432) and a healthcheck on each.
2. `scripts/connectivity` builds a value that is unique to this run (`phase0 connectivity check <unix nanoseconds>`).
3. `ensureTopic` creates the topic `phase0-connectivity-test` (1 partition, replication factor 1).
4. `produceMessage` connects to the partition leader and writes one message with a 10 second deadline.
5. `consumeMessage` reads from the topic as consumer group `phase0-connectivity-group` and checks the message equals the value from step 2.
6. `checkRedis` runs `SET` (1 minute TTL) then `GET` and checks the result equals the same value.
7. Success prints `phase 0 connectivity check: OK`. Any failure exits with status 1 via `log.Fatalf`.

Postgres is only checked by its healthcheck. No Go code touches it yet.

## Reading the output
```
kafka message received: phase0 connectivity check 1789359683717371000
redis value retrieved: phase0 connectivity check 1789359683717371000
phase 0 connectivity check: OK
```
- The number is a Unix timestamp in nanoseconds, made at the start of this run.
- The same number in both lines means both round trips carried this run's value, not a leftover from an earlier run.

## The test that proves it
Ran `docker compose down` (this removes the containers and the network, so Kafka comes back with no topics), then `docker compose up -d`, then `go run ./scripts/connectivity` straight away. `docker compose ps` still showed all three as `health: starting` at that point. The script passed on the first attempt and printed the same value from Kafka and Redis.

The empty-broker test matters more than the second run. Earlier versions of the script also passed once a topic already existed, which hid a real bug.

## Bugs that taught me something
- **`Unknown Topic Or Partition`:** `kafka-go`'s `Writer` doesn't ask the broker to auto-create a missing topic unless `AllowAutoTopicCreation` is set. The broker wasn't the problem, so the fix was in the Go code, not in `docker-compose.yml`.
- **The first fix wasn't enough:** with the flag set, the script still failed against an empty broker, then passed on the next run because the topic now existed. Auto-creation is asynchronous, so the first produce can lose the race. Only running against a broker with no topics showed this. Fix: create the topic explicitly with `conn.CreateTopics`.
- **The fix for that was itself unsafe:** I patched the race with a retry loop around `kafka.Writer`. The adversarial reviewer showed a retry can send a second copy when only the acknowledgement was lost, and that `Writer.Close()` can flush a message after the produce was already reported as failed. Fix: one synchronous write through `kafka.Conn`, no retry.
- **A check that could pass by accident:** a constant message let a leftover record from a previous run print "OK". Fix: a fresh value per run, compared on read.
- **No timeout on the Redis check:** the first version passed a plain background context, so it could hang. Fix: a 10 second timeout, like the Kafka steps.
- **Docker wasn't ready even though the app was open:** `docker info` printed the client section and an empty `Server:` section, which meant the engine hadn't finished starting. `docker compose up` only works once the engine is up.

## Next steps
- Phase 1: the agent loop with no infrastructure at all.
- Phase 2: the first real use of Kafka as a journal and of Postgres, so Postgres reachability from Go finally gets exercised.
- Phase 3: the retry problem from this phase comes back for real tools, where recovery must re-attempt a step and needs a Redis claim to tell "already happened" from "didn't hear back".
- Phase 4: a consumer that commits its offset only after the result is durable.
- Known gaps left on purpose: the topic has one partition, and `pg_isready` proves less than a Go connection would. In my runs the whole script took 12 to 20 seconds. I haven't found out why, and I haven't tested whether the consumer group join is the cause.
