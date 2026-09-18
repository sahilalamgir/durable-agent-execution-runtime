// Command replay is the FR-21 CLI: it reconstructs and prints a run's
// state from either Kafka (the authoritative source, D-2) or Postgres (for
// inspection/cross-checking only).
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/anthropics/anthropic-sdk-go"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/sahilalamgir/durable-agent-execution-runtime/internal/events"
	"github.com/sahilalamgir/durable-agent-execution-runtime/internal/replay"
	"github.com/sahilalamgir/durable-agent-execution-runtime/internal/store"
)

const (
	defaultKafkaBrokers = "localhost:9092"
	defaultPostgresDSN  = "postgres://dae:dae@localhost:5432/dae?sslmode=disable"
	defaultTimeout      = 30 * time.Second
)

func main() {
	os.Exit(run(os.Stdout, os.Stderr))
}

// run implements the CLI end to end, taking stdout/stderr as parameters so
// it's testable without capturing the real os.Stdout/os.Stderr.
func run(stdout, stderr io.Writer) int {
	runID := flag.String("run-id", "", "run_id to replay (required)")
	source := flag.String("source", "kafka", `event source: "kafka" (authoritative) or "postgres" (cross-check only)`)
	upToSeq := flag.Int64("up-to-seq", -1, "truncate replay to sequence_number <= this value (-1: no truncation)")
	flag.Parse()

	if *runID == "" {
		fmt.Fprintln(stderr, "--run-id is required")
		return 1
	}

	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()

	src, cleanup, err := buildEventSource(ctx, *source)
	if err != nil {
		fmt.Fprintf(stderr, "building event source: %v\n", err)
		return 1
	}
	defer cleanup()

	evs, err := src.Events(ctx, *runID)
	if err != nil {
		if errors.Is(err, replay.ErrRunNotFound) {
			fmt.Fprintf(stderr, "run %s not found\n", *runID)
			return 2
		}
		fmt.Fprintf(stderr, "reading events: %v\n", err)
		return 1
	}

	var state replay.RunState
	eventCount := len(evs)
	if *upToSeq >= 0 {
		eventCount = countUpToSeq(evs, *upToSeq)
		state, err = replay.FoldUpTo(evs, *upToSeq)
	} else {
		state, err = replay.Fold(evs)
	}
	if err != nil {
		if errors.Is(err, replay.ErrRunNotFound) {
			fmt.Fprintf(stderr, "run %s not found\n", *runID)
			return 2
		}
		fmt.Fprintf(stderr, "folding events: %v\n", err)
		return 1
	}

	if err := printState(stdout, *runID, *source, eventCount, state); err != nil {
		fmt.Fprintf(stderr, "printing state: %v\n", err)
		return 1
	}
	return 0
}

// countUpToSeq mirrors replay.FoldUpTo's truncation boundary, purely for
// the CLI's "events=N" display line (FR-21's "events=<n>" makes lag/
// truncation visible, EC-13).
func countUpToSeq(evs []events.Envelope, upToSeq int64) int {
	n := 0
	for _, e := range evs {
		if e.SequenceNumber > upToSeq {
			break
		}
		n++
	}
	return n
}

// buildEventSource constructs the EventSource named by source, and a
// cleanup func to release whatever resources it opened.
func buildEventSource(ctx context.Context, source string) (replay.EventSource, func(), error) {
	switch source {
	case "kafka":
		brokers := strings.Split(envOr("DAE_KAFKA_BROKERS", defaultKafkaBrokers), ",")
		return replay.NewKafkaEventSource(brokers), func() {}, nil

	case "postgres":
		// The one place outside cmd/projector that imports pgx — FR-26
		// only restricts cmd/worker.
		dsn := envOr("DAE_POSTGRES_DSN", defaultPostgresDSN)
		pool, err := pgxpool.New(ctx, dsn)
		if err != nil {
			return nil, nil, fmt.Errorf("connecting to postgres: %w", err)
		}
		return store.NewEventReader(pool), pool.Close, nil

	default:
		return nil, nil, fmt.Errorf("unknown --source %q (want %q or %q)", source, "kafka", "postgres")
	}
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// printState prints the FR-21 format:
//
//	run_id=<uuid> source=kafka events=6
//	status=RUNNING current_step=2 max_steps=10 next_sequence=6
//	next_action=RESOLVE_IN_FLIGHT_TOOL in_flight=run_tests(toolu_02B)
//	messages: user[text] assistant[text,tool_use] user[tool_result] assistant[tool_use]
//	state_digest=4be1…9a0c
//
// It never prints full message bodies — only role + block-type shape.
func printState(w io.Writer, runID, source string, eventCount int, s replay.RunState) error {
	fmt.Fprintf(w, "run_id=%s source=%s events=%d\n", runID, source, eventCount)
	fmt.Fprintf(w, "status=%s current_step=%d max_steps=%d next_sequence=%d\n", s.Status, s.CurrentStep, s.MaxSteps, s.NextSequence)
	fmt.Fprintln(w, nextActionLine(s))

	msgs, err := s.Messages()
	if err != nil {
		return fmt.Errorf("computing messages: %w", err)
	}
	fmt.Fprintf(w, "messages: %s\n", summarizeMessages(msgs))

	digest, err := s.Digest()
	if err != nil {
		return fmt.Errorf("computing digest: %w", err)
	}
	fmt.Fprintf(w, "state_digest=%s\n", digest)
	return nil
}

func nextActionLine(s replay.RunState) string {
	line := fmt.Sprintf("next_action=%s", s.NextAction)
	switch {
	case s.InFlightTool != nil:
		line += fmt.Sprintf(" in_flight=%s(%s)", s.InFlightTool.ToolName, s.InFlightTool.ToolUseID)
	case len(s.PendingToolCalls) > 0:
		names := make([]string, len(s.PendingToolCalls))
		for i, tc := range s.PendingToolCalls {
			names[i] = fmt.Sprintf("%s(%s)", tc.ToolName, tc.ToolUseID)
		}
		line += fmt.Sprintf(" pending=%s", strings.Join(names, ","))
	}
	return line
}

func summarizeMessages(msgs []anthropic.MessageParam) string {
	parts := make([]string, len(msgs))
	for i, m := range msgs {
		types := make([]string, 0, len(m.Content))
		for _, b := range m.Content {
			types = append(types, blockParamType(b))
		}
		parts[i] = fmt.Sprintf("%s[%s]", m.Role, strings.Join(types, ","))
	}
	return strings.Join(parts, " ")
}

func blockParamType(b anthropic.ContentBlockParamUnion) string {
	switch {
	case b.OfText != nil:
		return "text"
	case b.OfToolUse != nil:
		return "tool_use"
	case b.OfToolResult != nil:
		return "tool_result"
	default:
		return "other"
	}
}
