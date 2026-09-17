// Command worker is the Phase 1 entrypoint: it runs one hardcoded fake
// repo-maintenance task through the agent loop to completion in memory,
// with no Kafka consumption and no durability layer. Later phases add
// Kafka-command consumption on top of this same loop without relocating
// this file.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"

	"github.com/anthropics/anthropic-sdk-go"
	"github.com/anthropics/anthropic-sdk-go/option"

	"github.com/sahilalamgir/durable-agent-execution-runtime/internal/agent"
	"github.com/sahilalamgir/durable-agent-execution-runtime/internal/tools"
)

const (
	model     = anthropic.ModelClaudeSonnet5
	maxTokens = int64(1024)
	// maxSteps caps the number of LLM round-trips a single run may take.
	// This is workload config, not a secret, and maps onto the future
	// RunStarted.max_steps event field.
	maxSteps = 10
)

const fakeTaskPrompt = "The repo github.com/example/widgets has a failing test. Clone it, run the tests, fix whatever is failing, re-run the tests to confirm, then open a pull request with the fix."

// errMaxStepsExceeded is a sentinel returned by run when the loop hit its
// step cap without reaching a terminal state. It is not a genuine failure,
// so main() checks for it specifically and exits quietly rather than
// routing it through log.Fatalf.
var errMaxStepsExceeded = errors.New("run did not complete: exceeded max_steps")

func main() {
	apiKey := os.Getenv("ANTHROPIC_API_KEY")
	if apiKey == "" {
		log.Fatal("ANTHROPIC_API_KEY environment variable is required")
	}
	err := run(context.Background(), apiKey)
	if errors.Is(err, errMaxStepsExceeded) {
		os.Exit(1)
	}
	if err != nil {
		log.Fatalf("worker failed: %v", err)
	}
}

// run wires up the loop and runs the hardcoded fake task to completion. It
// returns a non-nil error when the LLM call itself fails, or
// errMaxStepsExceeded (not a genuine failure) when the loop hit its step
// cap without reaching a terminal state. It never calls os.Exit itself,
// leaving that to main() so any future deferred cleanup here still runs.
func run(ctx context.Context, apiKey string) error {
	client := anthropic.NewClient(option.WithAPIKey(apiKey))
	llm := agent.NewAnthropicClient(client)

	registry := tools.NewRegistry(
		tools.NewCloneRepoTool(),
		tools.NewRunTestsTool(),
		tools.NewApplyFixTool(),
		tools.NewOpenPRTool(),
	)

	loop := agent.NewLoop(llm, registry, model, maxTokens, maxSteps, os.Stdout)

	outcome, err := loop.Run(ctx, agent.Task{Prompt: fakeTaskPrompt})
	if err != nil {
		return fmt.Errorf("running agent loop: %w", err)
	}

	if outcome.Truncated {
		fmt.Fprintln(os.Stderr, "warning: run was truncated (hit max_tokens)")
	}

	if outcome.MaxStepsExceeded {
		return errMaxStepsExceeded
	}
	return nil
}
