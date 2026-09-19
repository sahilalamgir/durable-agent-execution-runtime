# Phase 0 in plain terms

## What changed

Before Phase 0 there was nothing to run. Now one command starts Kafka, Redis and Postgres on your Mac, and a small Go script proves it can talk to Kafka and Redis. There is no agent, no events and no durability yet. Every later phase stands on this plumbing, so you prove it works first.

## New concepts

1. Container and image
   An image is a prebuilt package of a program plus everything it needs (Kafka, Redis, Postgres each have one). A container is a running copy of an image, isolated from the rest of your Mac. You don't install Kafka on your machine. You start a container, and when you delete it, your Mac is untouched.

2. Docker Compose
   One file (`docker-compose.yml`) describes all three containers, and one command starts them together the same way every time. It replaces three long `docker run` commands you'd get subtly wrong.

3. The compose network and service-name DNS
   Compose creates a private network for your containers. Inside it, each service is reachable by its service name (a container can reach `kafka`, `redis`, `postgres`). Your Mac is not on that network, so the name `kafka` means nothing to a program running on your Mac.

4. Published ports
   `"9092:9092"` means "connect to `localhost:9092` on your Mac and Docker forwards it into the container's port 9092." This is how the Go script, which runs on your Mac, reaches a container.

5. Kafka basics (only what Phase 0 needs)
   A broker is the Kafka server. A topic is a named channel of messages. A producer writes messages to a topic. A consumer reads them. The script does one of each. Partitions and consumer groups get real in Phase 2 and 4.

6. Advertised listeners (Kafka's "reconnect to me here")
   When you connect, a Kafka broker tells you an address to use for everything after that. That address must work from where you are. So the broker has three listeners: one advertised as `kafka:19092` for containers, one advertised as `localhost:9092` for your Mac, and one internal one for Kafka's own coordination. KRaft mode means one Kafka container does the broker and controller jobs, so there's no separate Zookeeper container.

7. Redis basics
   Redis is a very fast key-value store that lives outside your program. The script writes one key (`SET`, with a 1 minute expiry) and reads it back (`GET`). The claim logic that makes Redis important starts in Phase 3.

8. Postgres in Phase 0
   It only has to start and report healthy. Nothing writes to it until Phase 2.

9. Healthchecks: running vs healthy
   "Up" means the process started. "Healthy" means a small test command inside the container succeeded (`redis-cli ping`, `pg_isready`, a Kafka API call). `docker compose ps` shows both. Healthy still doesn't mean "any topic exists."

10. Auto-created topics race
    A producer can ask Kafka to create a missing topic. But creation is asynchronous, so the produce that triggers it can be rejected before the topic is ready. Creating the topic explicitly before producing avoids the race.

11. You can't blindly retry a write
    If a write reaches the broker but the acknowledgement is lost, Go can't tell "it didn't happen" from "it happened and I didn't hear back." A retry then writes a second copy. Phase 0 avoids this by writing once with no retry. Phase 3 has to solve it properly for real tools.

12. Context and timeouts in Go
    Every Kafka and Redis call gets a `context.Context` with a 10 second timeout. If the service hangs, the call gives up instead of waiting forever. This becomes a hard rule later (a Redis check must time out cleanly).

## What happens under the hood

1. `docker compose up -d` creates the network, then starts the Kafka, Redis and Postgres containers.
2. Each container runs its healthcheck every few seconds until it reports `healthy`.
3. `go run ./scripts/connectivity` builds a value unique to this run (a timestamp).
4. It connects to Kafka on `localhost:9092` and creates the topic if it's missing.
5. It writes one message to the topic and waits for the result.
6. It reads a message back as a consumer and checks it equals the value from step 3.
7. It connects to Redis on `localhost:6379`, sets a key, reads it back and checks it equals the same value.
8. It prints `phase 0 connectivity check: OK`, or exits with an error on any failure.

### Example: which address to use

| Who is connecting | Kafka address | Why |
| --- | --- | --- |
| Go script on your Mac | `localhost:9092` | Published port, and the broker advertises `localhost:9092` on that listener |
| Another container on the compose network | `kafka:19092` | Service-name DNS, and the broker advertises `kafka:19092` on that listener |

The wrong combination fails. The script on your Mac using `kafka:9092` gets `no such host`, because that name only exists inside the compose network. No container needs the second row yet (workers move into containers in Phase 6).

### Example: what went wrong on the way

1. The first run failed with `Unknown Topic Or Partition`. The Go client wasn't asking Kafka to auto-create the topic.
2. Turning that on still failed on a broker with no topics, because creation was asynchronous. It passed the next run only because the topic now existed.
3. A retry loop hid the race, but a retry can write a duplicate when the acknowledgement is lost.
4. The fix was to create the topic first and write once, with no retry.

## New things you'll run

- `docker compose up -d`: start everything in the background.
- `docker compose ps`: check every service shows `healthy`.
- `go run ./scripts/connectivity`: the pass/fail check. It exits with status 1 on failure.
- `docker compose down`: stop and remove the containers and network. Since Phase 2 added named volumes, this keeps Kafka and Postgres data. Use `docker compose down -v` for a true fresh start.
- `docker exec dae-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list`: see which topics exist.
- `docker info`: if the `Server:` section is empty, Docker Desktop's engine isn't ready yet, even if the app is open.

## Reading the output

```
kafka message received: phase0 connectivity check 1789359683717371000
redis value retrieved: phase0 connectivity check 1789359683717371000
phase 0 connectivity check: OK
```

- The number is a Unix timestamp in nanoseconds, made at the start of this run.
- The same number in both lines means both round trips carried this run's value, not a leftover from an earlier run.

## Known limits, on purpose

- Postgres is only checked by its healthcheck, which runs inside the container. It doesn't prove a Go process on your Mac can reach it.
- The topic has one partition, so this setup can't show what a consumer-group rebalance does.
- The script hard-codes `localhost` addresses. That only works from your Mac, not from inside a container.
- Phase 0 treated all three services as disposable. Later phases changed that (Kafka and Postgres volumes in Phase 2, durable Redis in Phase 3).
