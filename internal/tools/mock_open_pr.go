package tools

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/anthropics/anthropic-sdk-go"
)

// OpenPRTool mocks opening a pull request. Each real execution is recorded
// in the Ledger, and the PR number is 1 + the open_pr entries already
// recorded for the run, so a duplicated PR shows up as #2. It implements
// Reconciler by looking its idempotency key up in the Ledger.
type OpenPRTool struct {
	ledger *Ledger
}

// NewOpenPRTool constructs an OpenPRTool that records into ledger.
func NewOpenPRTool(ledger *Ledger) *OpenPRTool {
	return &OpenPRTool{ledger: ledger}
}

func (t *OpenPRTool) Name() string { return "open_pr" }

// HasSideEffect reports true: opening a pull request is exactly the
// real-world side effect this project exists to guard against duplicating
// (Phase 3).
func (t *OpenPRTool) HasSideEffect() bool { return true }

func (t *OpenPRTool) Description() string {
	return "Opens a pull request with the given title and body."
}

func (t *OpenPRTool) InputSchema() anthropic.ToolInputSchemaParam {
	return anthropic.ToolInputSchemaParam{
		Properties: map[string]any{
			"title": map[string]any{
				"type":        "string",
				"description": "The pull request title.",
			},
			"body": map[string]any{
				"type":        "string",
				"description": "The pull request body.",
			},
		},
		Required: []string{"title", "body"},
	}
}

type openPRArgs struct {
	Title string `json:"title"`
	Body  string `json:"body"`
}

func (t *OpenPRTool) Execute(ctx context.Context, inv Invocation, rawArgs json.RawMessage) (string, error) {
	if err := mockToolDelay(ctx); err != nil {
		return "", fmt.Errorf("waiting out mock delay: %w", err)
	}
	var args openPRArgs
	if err := json.Unmarshal(rawArgs, &args); err != nil {
		return "", fmt.Errorf("unmarshaling open_pr args: %w", err)
	}
	existing, err := t.ledger.Entries(ctx)
	if err != nil {
		return "", fmt.Errorf("counting existing PRs: %w", err)
	}
	result := fmt.Sprintf("opened PR #%d: %s", countEntries(existing, inv.RunID, t.Name())+1, args.Title)
	entry := LedgerEntry{
		RunID: inv.RunID, ToolUseID: inv.ToolUseID, ToolName: t.Name(),
		IdempotencyKey: entryKey(inv), Result: result,
		RecordedAt: time.Now().UTC().Truncate(time.Microsecond), WriterPID: os.Getpid(),
	}
	if err := t.ledger.Record(ctx, entry); err != nil {
		return "", fmt.Errorf("recording open_pr effect: %w", err)
	}
	return result, nil
}

// Reconcile reports whether a PR was already opened under inv's idempotency
// key, by looking the key up in the ledger.
func (t *OpenPRTool) Reconcile(ctx context.Context, inv Invocation, _ json.RawMessage) (ReconcileResult, error) {
	if inv.IdempotencyKey == nil {
		return ReconcileResult{}, errors.New("reconciling open_pr: invocation has no idempotency key")
	}
	entry, found, err := t.ledger.FindByKey(ctx, *inv.IdempotencyKey)
	if err != nil {
		return ReconcileResult{}, fmt.Errorf("reconciling open_pr: %w", err)
	}
	if !found {
		return ReconcileResult{}, nil
	}
	return ReconcileResult{Applied: true, Result: entry.Result}, nil
}

func countEntries(entries []LedgerEntry, runID, toolName string) int {
	n := 0
	for _, e := range entries {
		if e.RunID == runID && e.ToolName == toolName {
			n++
		}
	}
	return n
}
