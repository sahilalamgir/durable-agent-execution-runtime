package tools

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/anthropics/anthropic-sdk-go"
)

// RunTestsTool mocks running the repository's test suite. The first call
// reports two failing tests; every call after that reports the suite
// passing, so a Loop driving clone_repo -> run_tests -> apply_fix ->
// run_tests takes a genuine multi-step arc.
type RunTestsTool struct {
	// calls counts invocations. It is a plain unsynchronized int: correct
	// only because Phase 1's Loop is single-goroutine and executes tools
	// sequentially. A concurrent caller would need a mutex around this.
	calls int
}

// NewRunTestsTool constructs a RunTestsTool.
func NewRunTestsTool() *RunTestsTool {
	return &RunTestsTool{}
}

func (t *RunTestsTool) Name() string { return "run_tests" }

// HasSideEffect reports false: running the test suite has no real-world
// side effect worth fencing against duplication.
func (t *RunTestsTool) HasSideEffect() bool { return false }

func (t *RunTestsTool) Description() string {
	return "Runs the repository's test suite and reports pass/fail results."
}

func (t *RunTestsTool) InputSchema() anthropic.ToolInputSchemaParam {
	return anthropic.ToolInputSchemaParam{
		Properties: map[string]any{},
	}
}

func (t *RunTestsTool) Execute(ctx context.Context, rawArgs json.RawMessage) (string, error) {
	if err := mockToolDelay(ctx); err != nil {
		return "", fmt.Errorf("waiting out mock delay: %w", err)
	}
	t.calls++
	if t.calls == 1 {
		return "2 tests failed: TestParseWidget, TestWidgetTotal", nil
	}
	return "all tests passed (2/2)", nil
}
