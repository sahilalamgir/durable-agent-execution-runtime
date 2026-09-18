// Command worker is the Phase 2 entrypoint: it runs one hardcoded fake
// repo-maintenance task through the agent loop, journaling every step to
// the run-events Kafka topic before acting on it (FR-8). It never imports
// internal/store's Postgres write side (FR-26): cmd/projector is the only
// process that writes to Postgres.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"

	"github.com/anthropics/anthropic-sdk-go"
	"github.com/anthropics/anthropic-sdk-go/option"
	"github.com/google/uuid"

	"github.com/sahilalamgir/durable-agent-execution-runtime/internal/agent"
	"github.com/sahilalamgir/durable-agent-execution-runtime/internal/kafka"
	"github.com/sahilalamgir/durable-agent-execution-runtime/internal/tools"
)

const (
	model     = anthropic.ModelClaudeSonnet5
	maxTokens = int64(1024)
	// maxSteps caps the number of LLM round-trips a single run may take.
	// This is workload config, not a secret, and maps onto RunStarted's
	// max_steps payload field.
	maxSteps = 10

	// workloadType and tenantID are constants for Phase 2: there is
	// exactly one workload (this fake repo-maintenance task) and no
	// multi-tenancy yet (system-spec.md's envelope contract: tenant_id is
	// "local-dev" for now).
	workloadType = "repo-maintenance-agent"
	tenantID     = "local-dev"

	defaultKafkaBrokers = "localhost:9092"
)

const fakeTaskPrompt = "The repo github.com/example/widgets has a failing test. Clone it, run the tests, fix whatever is failing, re-run the tests to confirm, then open a pull request with the fix."

// Exit codes (system-spec.md's cmd/worker contract):
//
//	0   RunCompleted
//	1   RunFailed, or any other non-retryable failure before a run existed
//	3   publish failed after every retry attempt (no terminal event written)
//	137 DAE_CRASH_AFTER_SEQ fired (os.Exit is called directly inside
//	    internal/agent's Loop; this constant documents it, it is never
//	    returned by run itself)
const (
	exitOK             = 0
	exitFailed         = 1
	exitPublishFailed  = 3
	exitCrashInjection = 137
)

func main() {
	apiKey := os.Getenv("ANTHROPIC_API_KEY")
	if apiKey == "" {
		log.Fatal("ANTHROPIC_API_KEY environment variable is required")
	}
	os.Exit(run(context.Background(), apiKey))
}

// run wires up the loop and runs the hardcoded fake task to completion. It
// returns the process exit code directly rather than an error, since the
// exit code depends on exactly which failure mode occurred (FR-8's write-
// ahead guarantee means "publish failed" and "the run itself failed" are
// different outcomes that must map to different codes).
func run(ctx context.Context, apiKey string) int {
	runID := uuid.New().String()
	// FR-18: printed before any other output.
	fmt.Printf("run_id=%s\n", runID)

	brokers := kafkaBrokers()
	if err := kafka.EnsureTopics(ctx, brokers); err != nil {
		fmt.Fprintf(os.Stderr, "ensuring kafka topics: %v\n", err)
		return exitFailed
	}

	publisher := kafka.NewEventPublisher(brokers)
	defer func() {
		if err := publisher.Close(); err != nil {
			fmt.Fprintf(os.Stderr, "closing kafka publisher: %v\n", err)
		}
	}()

	client := anthropic.NewClient(option.WithAPIKey(apiKey))
	llm := agent.NewAnthropicClient(client)

	registry := tools.NewRegistry(
		tools.NewCloneRepoTool(),
		tools.NewRunTestsTool(),
		tools.NewApplyFixTool(),
		tools.NewOpenPRTool(),
	)

	journal := agent.NewJournalConfig(publisher, runID, tenantID, workloadType)
	if crashAfterSeq, ok := crashAfterSeqFromEnv(); ok {
		journal.CrashAfterSeq = crashAfterSeq
	}

	loop := agent.NewLoop(llm, registry, model, maxTokens, maxSteps, os.Stdout, journal)

	outcome, err := loop.Run(ctx, agent.Task{Prompt: fakeTaskPrompt})
	if err != nil {
		fmt.Fprintf(os.Stderr, "worker failed: %v\n", err)
		if errors.Is(err, agent.ErrPublishFailed) {
			return exitPublishFailed
		}
		return exitFailed
	}

	if outcome.Failed {
		return exitFailed
	}
	return exitOK
}

// kafkaBrokers reads DAE_KAFKA_BROKERS as a comma-separated list, defaulting
// to a single local broker.
func kafkaBrokers() []string {
	raw := os.Getenv("DAE_KAFKA_BROKERS")
	if raw == "" {
		raw = defaultKafkaBrokers
	}
	return strings.Split(raw, ",")
}

// crashAfterSeqFromEnv reads DAE_CRASH_AFTER_SEQ (FR-30), an optional
// sequence_number after which the worker exits(137) immediately once that
// event is acknowledged and applied. ok is false when the env var is unset,
// meaning crash injection stays disabled.
func crashAfterSeqFromEnv() (n int64, ok bool) {
	raw := os.Getenv("DAE_CRASH_AFTER_SEQ")
	if raw == "" {
		return 0, false
	}
	n, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		log.Fatalf("DAE_CRASH_AFTER_SEQ=%q is not a valid integer: %v", raw, err)
	}
	return n, true
}
