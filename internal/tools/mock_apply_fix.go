package tools

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/anthropics/anthropic-sdk-go"
)

// ApplyFixTool mocks applying a code fix to the repository. Each real
// execution is recorded in the Ledger. It deliberately does NOT implement
// Reconciler, so the "stale claim, no way to check" path stays exercised.
type ApplyFixTool struct {
	ledger *Ledger
}

// NewApplyFixTool constructs an ApplyFixTool that records into ledger.
func NewApplyFixTool(ledger *Ledger) *ApplyFixTool {
	return &ApplyFixTool{ledger: ledger}
}

func (t *ApplyFixTool) Name() string { return "apply_fix" }

// HasSideEffect reports true: applying a fix mutates the repository's
// working tree and must be fenced against duplication (Phase 3).
func (t *ApplyFixTool) HasSideEffect() bool { return true }

func (t *ApplyFixTool) Description() string {
	return "Applies a fix to the repository's code."
}

func (t *ApplyFixTool) InputSchema() anthropic.ToolInputSchemaParam {
	return anthropic.ToolInputSchemaParam{
		Properties: map[string]any{
			"description": map[string]any{
				"type":        "string",
				"description": "A description of the fix being applied.",
			},
		},
		Required: []string{"description"},
	}
}

type applyFixArgs struct {
	Description string `json:"description"`
}

func (t *ApplyFixTool) Execute(ctx context.Context, inv Invocation, rawArgs json.RawMessage) (string, error) {
	if err := mockToolDelay(ctx); err != nil {
		return "", fmt.Errorf("waiting out mock delay: %w", err)
	}
	var args applyFixArgs
	if err := json.Unmarshal(rawArgs, &args); err != nil {
		return "", fmt.Errorf("unmarshaling apply_fix args: %w", err)
	}
	result := fmt.Sprintf("applied fix: %s", args.Description)
	// The ledger write is the mock's "real effect": it happens after the
	// delay, and a failure here means the effect did not happen.
	entry := LedgerEntry{
		RunID: inv.RunID, ToolUseID: inv.ToolUseID, ToolName: t.Name(),
		IdempotencyKey: entryKey(inv), Result: result,
		RecordedAt: time.Now().UTC().Truncate(time.Microsecond), WriterPID: os.Getpid(),
	}
	if err := t.ledger.Record(ctx, entry); err != nil {
		return "", fmt.Errorf("recording apply_fix effect: %w", err)
	}
	return result, nil
}
