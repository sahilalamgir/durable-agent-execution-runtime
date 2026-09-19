package tools

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/anthropics/anthropic-sdk-go"
)

// RunTestsTool mocks running the repository's test suite. It reports failure
// until the ledger contains an apply_fix entry for the invocation's run, and
// success from then on. Driving this from the ledger instead of an in-process
// call counter means a resumed process (whose counter would reset) sees the
// same answer the crashed one would have.
type RunTestsTool struct {
	ledger *Ledger
}

// NewRunTestsTool constructs a RunTestsTool that reads from ledger.
func NewRunTestsTool(ledger *Ledger) *RunTestsTool {
	return &RunTestsTool{ledger: ledger}
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

func (t *RunTestsTool) Execute(ctx context.Context, inv Invocation, _ json.RawMessage) (string, error) {
	if err := mockToolDelay(ctx); err != nil {
		return "", fmt.Errorf("waiting out mock delay: %w", err)
	}
	entries, err := t.ledger.Entries(ctx)
	if err != nil {
		return "", fmt.Errorf("checking whether the fix was applied: %w", err)
	}
	if countEntries(entries, inv.RunID, "apply_fix") == 0 {
		return "2 tests failed: TestParseWidget, TestWidgetTotal", nil
	}
	return "all tests passed (2/2)", nil
}
