// Command worker runs one hardcoded fake repo-maintenance task through the
// agent loop, journaling every step to the run-events Kafka topic before
// acting on it (FR-8), and fencing every side-effecting tool through Redis
// (Phase 3). With DAE_RESUME_RUN_ID it instead replays a crashed run from
// Kafka and continues it. It never imports internal/store's Postgres write
// side (FR-26): cmd/projector is the only process that writes to Postgres.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/anthropics/anthropic-sdk-go"
	"github.com/anthropics/anthropic-sdk-go/option"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"

	"github.com/sahilalamgir/durable-agent-execution-runtime/internal/agent"
	"github.com/sahilalamgir/durable-agent-execution-runtime/internal/idempotency"
	"github.com/sahilalamgir/durable-agent-execution-runtime/internal/kafka"
	"github.com/sahilalamgir/durable-agent-execution-runtime/internal/replay"
	"github.com/sahilalamgir/durable-agent-execution-runtime/internal/tools"
)

const (
	model     = anthropic.ModelClaudeSonnet5
	maxTokens = int64(1024)
	// maxSteps caps the number of LLM round-trips a fresh run may take. A
	// resumed run keeps the max_steps its RunStarted journaled.
	maxSteps = 10

	// workloadType and tenantID are constants: there is exactly one workload
	// (this fake repo-maintenance task) and no multi-tenancy yet.
	workloadType = "repo-maintenance-agent"
	tenantID     = "local-dev"

	redisPingTimeout = 5 * time.Second
)

const fakeTaskPrompt = "The repo github.com/example/widgets has a failing test. Clone it, run the tests, fix whatever is failing, re-run the tests to confirm, then open a pull request with the fix."

// Exit codes:
//
//	0   RunCompleted (or a resumed run that was already COMPLETED)
//	1   RunFailed / CANCELLED, or a startup failure (including Redis unreachable)
//	2   DAE_RESUME_RUN_ID names a run with no events
//	3   publish failed after every retry attempt (no terminal event written)
//	4   the idempotency fence was unavailable, or a tool's outcome is unknown:
//	    no terminal event is written, so the run stays resumable
//	137 an injected crash (DAE_CRASH_AFTER_SEQ / DAE_CRASH_AT)
const (
	exitOK               = 0
	exitFailed           = 1
	exitRunNotFound      = 2
	exitPublishFailed    = 3
	exitFenceUnavailable = 4
)

func main() {
	apiKey := os.Getenv("ANTHROPIC_API_KEY")
	if apiKey == "" {
		log.Fatal("ANTHROPIC_API_KEY environment variable is required")
	}
	cfg, err := loadConfig(os.Getenv)
	if err != nil {
		log.Fatalf("invalid configuration: %v", err)
	}
	os.Exit(run(context.Background(), apiKey, cfg))
}

// run wires up the loop and runs (or resumes) the task. It returns the
// process exit code directly: the code depends on exactly which failure mode
// occurred, and "publish failed", "the run failed" and "the fence could not
// vouch for a tool" are different outcomes that must map to different codes.
func run(ctx context.Context, apiKey string, cfg config) int {
	brokers := cfg.brokers
	if err := kafka.EnsureTopics(ctx, brokers); err != nil {
		fmt.Fprintf(os.Stderr, "ensuring kafka topics: %v\n", err)
		return exitFailed
	}

	// FR-30: prove Redis is reachable before anything is published, so a
	// worker that can't fence never starts a run or touches a resumed one.
	redisClient := idempotency.NewRedisClient(cfg.redisAddr)
	defer redisClient.Close()
	if err := pingRedis(ctx, redisClient); err != nil {
		fmt.Fprintf(os.Stderr, "redis at %s unreachable: %v\n", cfg.redisAddr, err)
		return exitFailed
	}

	runID, state, code, done := startingState(ctx, cfg)
	if done {
		return code
	}

	publisher := kafka.NewEventPublisher(brokers)
	defer func() {
		if err := publisher.Close(); err != nil {
			fmt.Fprintf(os.Stderr, "closing kafka publisher: %v\n", err)
		}
	}()

	loop, err := buildLoop(apiKey, cfg, redisClient, publisher, runID)
	if err != nil {
		fmt.Fprintf(os.Stderr, "building worker: %v\n", err)
		return exitFailed
	}

	var outcome agent.Outcome
	if cfg.resumeRunID != "" {
		outcome, err = loop.Resume(ctx, state)
	} else {
		outcome, err = loop.Run(ctx, agent.Task{Prompt: fakeTaskPrompt})
	}
	return exitCodeFor(outcome, err)
}

// startingState decides what this process does with its run. For a fresh
// run it prints run_id and returns an empty state. For a resume it replays
// the run from Kafka (never Postgres, D-2) and prints where it resumes from.
// done is true when there is nothing left to run: the run is already
// terminal, or could not be replayed; code is then the exit code.
func startingState(ctx context.Context, cfg config) (runID string, state replay.RunState, code int, done bool) {
	if cfg.resumeRunID == "" {
		// FR-18: printed before any other output, but only once Redis is
		// known to be reachable (FR-30).
		runID = uuid.New().String()
		fmt.Printf("run_id=%s\n", runID)
		return runID, replay.RunState{}, 0, false
	}

	src := replay.NewKafkaEventSource(cfg.brokers)
	state, err := replay.Replay(ctx, src, cfg.resumeRunID)
	if errors.Is(err, replay.ErrRunNotFound) {
		fmt.Fprintf(os.Stderr, "run %s not found in kafka\n", cfg.resumeRunID)
		return "", replay.RunState{}, exitRunNotFound, true
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "replaying run %s: %v\n", cfg.resumeRunID, err)
		return "", replay.RunState{}, exitFailed, true
	}

	if terminal, code := terminalExitCode(state.Status); terminal {
		fmt.Printf("run_id=%s status=%s next_sequence=%d\n", state.RunID, state.Status, state.NextSequence)
		return state.RunID, state, code, true
	}
	fmt.Printf("run_id=%s resumed_from_seq=%d next_action=%s\n", state.RunID, state.NextSequence-1, state.NextAction)
	return state.RunID, state, 0, false
}

// terminalExitCode reports whether status is terminal and, if so, the exit
// code a worker resuming that run returns: 0 for COMPLETED, 1 otherwise.
func terminalExitCode(status replay.RunStatus) (terminal bool, code int) {
	switch status {
	case replay.StatusCompleted:
		return true, exitOK
	case replay.StatusFailed, replay.StatusCancelled:
		return true, exitFailed
	default:
		return false, 0
	}
}

func pingRedis(ctx context.Context, client *redis.Client) error {
	ctx, cancel := context.WithTimeout(ctx, redisPingTimeout)
	defer cancel()
	if err := client.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("pinging redis: %w", err)
	}
	return nil
}

// buildLoop wires the loop's dependencies explicitly: the ledger the mock
// tools write to, the Guard over Redis, and the crash hooks.
func buildLoop(apiKey string, cfg config, redisClient *redis.Client, publisher kafka.EventPublisher, runID string) (*agent.Loop, error) {
	crash, err := idempotency.ParseCrashSpec(cfg.crashSpec, os.Exit)
	if err != nil {
		return nil, fmt.Errorf("parsing DAE_CRASH_AT: %w", err)
	}
	guard := idempotency.NewGuard(idempotency.NewRedisStore(redisClient, cfg.idemTTL), idempotency.GuardConfig{
		ToolTimeout: cfg.toolTimeout,
		TTL:         cfg.idemTTL,
		Out:         os.Stdout,
		Crash:       crash,
	})

	ledger := tools.NewLedger(cfg.ledgerPath)
	registry := tools.NewRegistry(
		tools.NewCloneRepoTool(),
		tools.NewRunTestsTool(ledger),
		tools.NewApplyFixTool(ledger),
		tools.NewOpenPRTool(ledger),
	)

	journal := agent.NewJournalConfig(publisher, runID, tenantID, workloadType)
	if cfg.crashAfterSeq != nil {
		journal.CrashAfterSeq = *cfg.crashAfterSeq
	}

	llm := agent.NewAnthropicClient(anthropic.NewClient(option.WithAPIKey(apiKey)))
	return agent.NewLoop(llm, registry, model, maxTokens, maxSteps, os.Stdout, journal, guard), nil
}

// exitCodeFor maps how Loop returned to the process exit code, printing the
// error if there is one.
func exitCodeFor(outcome agent.Outcome, err error) int {
	if err != nil {
		fmt.Fprintf(os.Stderr, "worker failed: %v\n", err)
		switch {
		case errors.Is(err, agent.ErrPublishFailed):
			return exitPublishFailed
		case errors.Is(err, idempotency.ErrFenceUnavailable), errors.Is(err, idempotency.ErrOutcomeUnknown):
			return exitFenceUnavailable
		default:
			return exitFailed
		}
	}
	if outcome.Failed {
		return exitFailed
	}
	return exitOK
}
