package main

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/sahilalamgir/durable-agent-execution-runtime/internal/tools"
)

func TestRun(t *testing.T) {
	entry := func(run, tool, key string) tools.LedgerEntry {
		return tools.LedgerEntry{RunID: run, ToolName: tool, IdempotencyKey: key}
	}
	tests := []struct {
		name        string
		entries     []tools.LedgerEntry
		runID       string
		wantCode    int
		wantSummary string
		wantRows    []string
	}{
		{
			name:        "no duplicates",
			entries:     []tools.LedgerEntry{entry("r1", "apply_fix", "idem:r1:3:a"), entry("r1", "open_pr", "idem:r1:5:b")},
			wantCode:    0,
			wantSummary: "entries=2 distinct_keys=2 duplicates=0",
			wantRows:    []string{"idem:r1:3:a  apply_fix  1", "idem:r1:5:b  open_pr  1"},
		},
		{
			name:        "same key twice is a duplicated side effect",
			entries:     []tools.LedgerEntry{entry("r1", "open_pr", "idem:r1:5:b"), entry("r1", "open_pr", "idem:r1:5:b")},
			wantCode:    1,
			wantSummary: "entries=2 distinct_keys=1 duplicates=1",
			wantRows:    []string{"idem:r1:5:b  open_pr  2"},
		},
		{
			name: "same tool and args but different keys is not a duplicate",
			entries: []tools.LedgerEntry{
				entry("r1", "open_pr", "idem:r1:5:toolu_a"), entry("r1", "open_pr", "idem:r1:5:toolu_b"),
			},
			wantCode:    0,
			wantSummary: "entries=2 distinct_keys=2 duplicates=0",
		},
		{
			name:        "three entries under one key count as two duplicates",
			entries:     []tools.LedgerEntry{entry("r1", "x", "k"), entry("r1", "x", "k"), entry("r1", "x", "k")},
			wantCode:    1,
			wantSummary: "entries=3 distinct_keys=1 duplicates=2",
		},
		{
			name:        "run-id filter ignores other runs",
			entries:     []tools.LedgerEntry{entry("r1", "open_pr", "k1"), entry("r2", "open_pr", "k1")},
			runID:       "r1",
			wantCode:    0,
			wantSummary: "entries=1 distinct_keys=1 duplicates=0",
		},
		{
			name:        "run-id filter matching nothing",
			entries:     []tools.LedgerEntry{entry("other", "open_pr", "z")},
			runID:       "no-such-run",
			wantCode:    0,
			wantSummary: "entries=0 distinct_keys=0 duplicates=0",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "ledger.jsonl")
			l := tools.NewLedger(path)
			for _, e := range tt.entries {
				if err := l.Record(context.Background(), e); err != nil {
					t.Fatal(err)
				}
			}

			var out, errOut bytes.Buffer
			code := run(context.Background(), &out, &errOut, path, tt.runID)

			if code != tt.wantCode {
				t.Errorf("exit code = %d, want %d (stderr %q)", code, tt.wantCode, errOut.String())
			}
			if !strings.Contains(out.String(), tt.wantSummary) {
				t.Errorf("output missing %q:\n%s", tt.wantSummary, out.String())
			}
			for _, row := range tt.wantRows {
				if !strings.Contains(out.String(), row) {
					t.Errorf("output missing row %q:\n%s", row, out.String())
				}
			}
		})
	}
}

// An unreadable ledger is not evidence of a duplicate: it must not share the
// duplicates exit code.
func TestRunUnreadableLedger(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ledger.jsonl")
	if err := os.WriteFile(path, []byte("not json\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	var out, errOut bytes.Buffer
	if code := run(context.Background(), &out, &errOut, path, ""); code != 3 {
		t.Fatalf("exit code = %d, want 3 for an unreadable ledger", code)
	}
}

func TestRunMissingLedger(t *testing.T) {
	var out, errOut bytes.Buffer
	code := run(context.Background(), &out, &errOut, filepath.Join(t.TempDir(), "nope.jsonl"), "")
	if code != 2 {
		t.Fatalf("exit code = %d, want 2 for a missing ledger", code)
	}
}
