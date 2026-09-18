// Command projector is the only process that writes to Postgres
// (CLAUDE.md's Critical Rules, FR-22). It consumes run-events as consumer
// group "projector" and, for each message, strictly one at a time, inserts
// it into the events table and upserts the matching runs row, then commits
// the Kafka offset — never the other way around (FR-23).
package main

import (
	"context"
	"errors"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	segmentiokafka "github.com/segmentio/kafka-go"

	"github.com/sahilalamgir/durable-agent-execution-runtime/internal/events"
	dkafka "github.com/sahilalamgir/durable-agent-execution-runtime/internal/kafka"
	"github.com/sahilalamgir/durable-agent-execution-runtime/internal/replay"
	"github.com/sahilalamgir/durable-agent-execution-runtime/internal/store"
)

const (
	defaultKafkaBrokers = "localhost:9092"
	defaultPostgresDSN  = "postgres://dae:dae@localhost:5432/dae?sslmode=disable"

	// dbRetryInitialBackoff/dbRetryMaxBackoff bound ApplyEvent's retry
	// loop for transient Postgres errors (EC-8): 200ms up to a 5s cap,
	// forever — the projector never gives up on a transient error, only
	// on a poison message (EC-7).
	dbRetryInitialBackoff = 200 * time.Millisecond
	dbRetryMaxBackoff     = 5 * time.Second
)

func main() {
	os.Exit(run())
}

func run() int {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	brokers := kafkaBrokers()
	dsn := postgresDSN()
	crashBeforeOffsetCommit := crashBeforeOffsetCommitFromEnv()

	if err := dkafka.EnsureTopics(ctx, brokers); err != nil {
		log.Printf("ensuring kafka topics: %v", err)
		return 1
	}

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		log.Printf("connecting to postgres: %v", err)
		return 1
	}
	defer pool.Close()

	proj := store.NewProjection(pool)
	if err := proj.Migrate(ctx); err != nil {
		log.Printf("applying schema migration: %v", err)
		return 1
	}

	reader := segmentiokafka.NewReader(segmentiokafka.ReaderConfig{
		Brokers:        brokers,
		Topic:          dkafka.TopicRunEvents,
		GroupID:        dkafka.ProjectorConsumerGroup,
		StartOffset:    segmentiokafka.FirstOffset,
		CommitInterval: 0, // synchronous commits: FetchMessage + CommitMessages only.
	})
	defer func() {
		if err := reader.Close(); err != nil {
			log.Printf("closing kafka reader: %v", err)
		}
	}()

	return consumeLoop(ctx, reader, proj, crashBeforeOffsetCommit)
}

// consumeLoop is the projector's whole job: FetchMessage, decode,
// ApplyEvent (retrying transient DB errors forever), log, check crash
// injection, CommitMessages — strictly one message at a time (FR-23).
func consumeLoop(ctx context.Context, reader *segmentiokafka.Reader, proj *store.Projection, crashBeforeOffsetCommit int) int {
	consumed := 0
	for {
		msg, err := reader.FetchMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return 0 // SIGINT/SIGTERM: clean shutdown.
			}
			log.Printf("fetching message: %v", err)
			return 1
		}

		env, err := events.Decode(msg.Value)
		if err != nil {
			// EC-7: a poison message. Log partition/offset/raw value and
			// exit without committing the offset, so it keeps failing on
			// the same message until a human steps in.
			log.Printf("poison message: decoding partition=%d offset=%d: %v\nraw value: %s", msg.Partition, msg.Offset, err, msg.Value)
			return 1
		}

		inserted, err := applyWithRetry(ctx, proj, env)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return 0
			}
			log.Printf("poison message: applying partition=%d offset=%d run_id=%s seq=%d: %v", msg.Partition, msg.Offset, env.RunID, env.SequenceNumber, err)
			return 1
		}

		log.Printf("projected seq=%d run_id=%s event=%s inserted=%t partition=%d offset=%d",
			env.SequenceNumber, env.RunID, env.EventType, inserted, msg.Partition, msg.Offset)

		consumed++
		if crashBeforeOffsetCommit > 0 && consumed == crashBeforeOffsetCommit {
			os.Exit(1)
		}

		if err := reader.CommitMessages(ctx, msg); err != nil {
			if errors.Is(err, context.Canceled) {
				return 0
			}
			log.Printf("committing offset partition=%d offset=%d: %v", msg.Partition, msg.Offset, err)
			return 1
		}
	}
}

// applyWithRetry retries proj.ApplyEvent with capped exponential backoff
// for any transient error, forever, per EC-8. Two kinds of error are NOT
// transient and are returned immediately instead: replay.ErrConflictingEvent
// (a genuine conflict will never resolve itself) and any Postgres error
// store.IsTransientError classifies as permanent (e.g. a NOT NULL or
// invalid-input-syntax violation) — retrying either forever would spin with
// zero progress and no signal anything is wrong, exactly the poison-message
// case EC-7 already handles for decode errors.
func applyWithRetry(ctx context.Context, proj *store.Projection, env events.Envelope) (bool, error) {
	backoff := dbRetryInitialBackoff
	for {
		inserted, err := proj.ApplyEvent(ctx, env)
		if err == nil {
			return inserted, nil
		}
		if errors.Is(err, replay.ErrConflictingEvent) {
			return false, err
		}
		if !store.IsTransientError(err) {
			return false, err
		}

		log.Printf("applying event %s (run_id=%s seq=%d): %v; retrying in %s", env.EventID, env.RunID, env.SequenceNumber, err, backoff)
		timer := time.NewTimer(backoff)
		select {
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			return false, ctx.Err()
		}

		backoff *= 2
		if backoff > dbRetryMaxBackoff {
			backoff = dbRetryMaxBackoff
		}
	}
}

func kafkaBrokers() []string {
	raw := os.Getenv("DAE_KAFKA_BROKERS")
	if raw == "" {
		raw = defaultKafkaBrokers
	}
	return strings.Split(raw, ",")
}

func postgresDSN() string {
	if dsn := os.Getenv("DAE_POSTGRES_DSN"); dsn != "" {
		return dsn
	}
	return defaultPostgresDSN
}

// crashBeforeOffsetCommitFromEnv reads DAE_PROJECTOR_CRASH_BEFORE_OFFSET_COMMIT
// (FR-30): the projector exits right after the N-th consumed message's
// Postgres transaction commits and before its Kafka offset commit. 0 (the
// default when unset) disables crash injection: message counts are always
// >= 1, so 0 can never collide with a real value.
func crashBeforeOffsetCommitFromEnv() int {
	raw := os.Getenv("DAE_PROJECTOR_CRASH_BEFORE_OFFSET_COMMIT")
	if raw == "" {
		return 0
	}
	n, err := strconv.Atoi(raw)
	if err != nil {
		log.Fatalf("DAE_PROJECTOR_CRASH_BEFORE_OFFSET_COMMIT=%q is not a valid integer: %v", raw, err)
	}
	return n
}
