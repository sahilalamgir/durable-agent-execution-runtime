Phase 0 — Local Environment (docker-compose + connectivity script)

Context

The project's whole point is proving a durability/reliability layer (event sourcing, idempotent tool execution, Kafka worker pool, chaos testing) underneath a deliberately simple agent workload. None of that can be built or tested without first proving that Kafka, Redis, and Postgres can all run locally and be reached from Go — that's the entirety of Phase 0 per .claude/specs/system-spec.md (FR-1/AC-1) and docs/workflow.md. This phase is pure plumbing: no events, no idempotency, no schema. The repo currently has zero code — no go.mod, no compose file, no /cmd or /scripts — so this plan creates that first slice from scratch.

Repo confirmed state: no go.mod anywhere, go1.27.1 darwin/arm64 installed, no docker-compose.yml/.env, git remote is github.com/sahilalamgir/durable-agent-execution-runtime (confirms the Go module path). Docker Desktop's app processes are running but the daemon is currently unresponsive (docker info/docker version hang) — that needs to be resolved by the user before docker-compose up can be verified; this plan doesn't attempt to fix it.

What gets built

1.  docker-compose.yml (repo root) — three services, Compose's default network (service-name DNS is automatic, no custom network block needed):

- Kafka — apache/kafka:3.7.0, KRaft mode (no Zookeeper container needed since 3.7 ships native KRaft support). Three listeners, which is the concrete expression of the host-vs-container DNS distinction just covered in tutoring:
  - PLAINTEXT (advertised as kafka:19092) — for future containers on the Compose network (workers, once they're containerized in Phase 6).
  - PLAINTEXT_HOST (advertised as localhost:9092) — for the Go script running on the host, published via ports: ["9092:9092"].
  - CONTROLLER — KRaft's internal quorum traffic, not exposed to the host.
  - Healthcheck via kafka-broker-api-versions.sh.
  - Note for implementation: verify exact env var names against the image's current docs — they shift between Kafka minor versions — but keep this three-listener split, it's the load-bearing design decision.
- Redis — redis:7-alpine, port 6379:6379, healthcheck via redis-cli ping.
- Postgres — postgres:16-alpine, port 5432:5432, env POSTGRES_USER/PASSWORD/DB=dae, healthcheck via pg_isready. No volumes (nothing needs to persist until Phase 2) — this keeps docker-compose down a clean full reset.

All three get healthchecks instead of a sleep/retry loop in Go, so docker-compose ps shows readiness and the script doesn't race container startup.

2.  Go module

- go mod init github.com/sahilalamgir/durable-agent-execution-runtime at repo root.
- go get github.com/segmentio/kafka-go@latest
- go get github.com/redis/go-redis/v9@latest
- No jackc/pgx — nothing queries Postgres until Phase 2; adding it now is unused dependency surface. Postgres's half of AC-1 ("docker-compose brings up ... Postgres") is satisfied by its healthcheck passing, not by Go code touching it — inventing a ping now would mean writing the first Postgres code in the repo before Phase 2 decides connection/pooling conventions.

3.  scripts/connectivity/main.go — single file, package main, matches CLAUDE.md's /scripts bullet ("local dev helper scripts"):

- main() → run(context.Background()); fatal error → log.Fatalf("connectivity check failed: %v", err) (this script's body is startup, so CLAUDE.md's startup-panic exception applies — log.Fatalf chosen over panic() for clean output a human reads directly).
- run(ctx context.Context) error, every I/O step taking ctx per CLAUDE.md's rule, errors wrapped via fmt.Errorf("doing X: %w", err):
  a. Kafka produce: kafka.Writer → localhost:9092, topic phase0-connectivity-test (deliberately distinct from the real spec's run-commands/run-events/approval-requests topics), one message, 10s timeout.
  b. Kafka consume: kafka.Reader for the same topic/broker, read the message back, print its value.
  c. Redis: SET phase0:connectivity:check (short TTL, throwaway) then GET it back, print it.
  d. Print "phase 0 connectivity check: OK" on success.
- No global mutable state — clients are local vars, not package vars, seeding that convention from the first file written.

4.  CLAUDE.md updates (after implementation)

- Add go run ./scripts/connectivity to the Commands section.
- Architecture section needs no change — /scripts is already listed.
- Current Status table: Phase 0 → mark done, but only after the user has personally verified it (see below) — not automatically by the implementer.

Explicitly out of scope

No Kafka partitioning/consumer-group design (Phase 4), no Redis fencing/claim logic (Phase 3), no Postgres schema/migrations (Phase 2), no /cmd/worker or /cmd/controlplane (Phases 1/5). Nothing here should anticipate those.

Verification (done by the user personally, per workflow.md's self-verification rule)

1.  Fully start Docker Desktop (steady whale icon) — required before step 2 can work; currently unresponsive.
2.  docker-compose up -d
3.  docker-compose ps — all three containers show healthy.
4.  go run ./scripts/connectivity
5.  Confirm output shows the round-tripped Kafka message, the Redis get/set, ending in the success line.
6.  docker-compose down

Matches AC-1 exactly: compose brings up all three, the Go script proves Kafka produce/consume and Redis set/get.

No .claude/specs/phase-0-\*.md and no docs/decisions/phase-0.md needed — workflow.md explicitly calls this phase too mechanical to warrant either.
