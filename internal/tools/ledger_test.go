package tools

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func newTestLedger(t *testing.T) *Ledger {
	t.Helper()
	return NewLedger(filepath.Join(t.TempDir(), "sub", "ledger.jsonl"))
}

func keyPtr(k string) *string { return &k }

func TestLedgerRecordAndRead(t *testing.T) {
	ctx := context.Background()
	l := newTestLedger(t)

	if got, err := l.Entries(ctx); err != nil || len(got) != 0 {
		t.Fatalf("Entries() on missing file = %v, %v; want empty, nil", got, err)
	}
	if l.Exists() {
		t.Fatal("Exists() = true before any Record")
	}

	for _, e := range []LedgerEntry{
		{RunID: "r1", ToolName: "apply_fix", IdempotencyKey: "k1", Result: "a"},
		{RunID: "r1", ToolName: "open_pr", IdempotencyKey: "k2", Result: "b"},
	} {
		if err := l.Record(ctx, e); err != nil {
			t.Fatalf("Record() error = %v", err)
		}
	}
	got, err := l.Entries(ctx)
	if err != nil || len(got) != 2 || got[0].IdempotencyKey != "k1" || got[1].IdempotencyKey != "k2" {
		t.Fatalf("Entries() = %+v, %v", got, err)
	}

	tests := []struct {
		name      string
		key       string
		wantFound bool
	}{
		{"known key", "k2", true},
		{"unknown key", "nope", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, found, err := l.FindByKey(ctx, tt.key)
			if err != nil || found != tt.wantFound {
				t.Fatalf("FindByKey(%q) found=%v err=%v, want found=%v", tt.key, found, err, tt.wantFound)
			}
		})
	}
}

func TestLedgerRejectsCorruptLine(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ledger.jsonl")
	if err := os.WriteFile(path, []byte("not json\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := NewLedger(path).Entries(context.Background()); err == nil {
		t.Fatal("Entries() on corrupt ledger returned nil error")
	}
}

func TestRunTestsFollowsLedger(t *testing.T) {
	ctx := context.Background()
	l := newTestLedger(t)
	tool := NewRunTestsTool(l)

	tests := []struct {
		name    string
		prepare func()
		runID   string
		want    string
	}{
		{"fails before any fix", func() {}, "r1", "2 tests failed: TestParseWidget, TestWidgetTotal"},
		{"fix for another run does not count", func() {
			_ = l.Record(ctx, LedgerEntry{RunID: "r2", ToolName: "apply_fix"})
		}, "r1", "2 tests failed: TestParseWidget, TestWidgetTotal"},
		{"passes once this run has a fix", func() {
			_ = l.Record(ctx, LedgerEntry{RunID: "r1", ToolName: "apply_fix"})
		}, "r1", "all tests passed (2/2)"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.prepare()
			got, err := tool.Execute(ctx, Invocation{RunID: tt.runID}, []byte(`{}`))
			if err != nil || got != tt.want {
				t.Fatalf("Execute() = %q, %v; want %q", got, err, tt.want)
			}
		})
	}
}

func TestOpenPRReconcileAndNumbering(t *testing.T) {
	ctx := context.Background()
	l := newTestLedger(t)
	tool := NewOpenPRTool(l)
	args := []byte(`{"title":"T","body":"B"}`)
	inv1 := Invocation{RunID: "r1", ToolUseID: "u1", IdempotencyKey: keyPtr("idem:r1:5:u1")}

	res, err := tool.Reconcile(ctx, inv1, args)
	if err != nil || res.Applied {
		t.Fatalf("Reconcile() before execute = %+v, %v; want not applied", res, err)
	}

	first, err := tool.Execute(ctx, inv1, args)
	if err != nil || first != "opened PR #1: T" {
		t.Fatalf("Execute() = %q, %v", first, err)
	}
	res, err = tool.Reconcile(ctx, inv1, args)
	if err != nil || !res.Applied || res.Result != first {
		t.Fatalf("Reconcile() after execute = %+v, %v; want applied with %q", res, err, first)
	}

	other := Invocation{RunID: "r1", ToolUseID: "u2", IdempotencyKey: keyPtr("idem:r1:7:u2")}
	if res, err := tool.Reconcile(ctx, other, args); err != nil || res.Applied {
		t.Fatalf("Reconcile() for unknown key = %+v, %v; want not applied", res, err)
	}
	// A duplicate execution is visible as #2.
	if second, _ := tool.Execute(ctx, other, args); second != "opened PR #2: T" {
		t.Fatalf("second Execute() = %q, want PR #2", second)
	}

	if _, err := tool.Reconcile(ctx, Invocation{RunID: "r1"}, args); err == nil {
		t.Fatal("Reconcile() without key returned nil error")
	}
}

func TestReconcilerImplementations(t *testing.T) {
	l := newTestLedger(t)
	tests := []struct {
		name string
		tool Tool
		want bool
	}{
		{"open_pr reconciles", NewOpenPRTool(l), true},
		{"apply_fix does not", NewApplyFixTool(l), false},
		{"run_tests does not", NewRunTestsTool(l), false},
		{"clone_repo does not", NewCloneRepoTool(), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, ok := tt.tool.(Reconciler)
			if ok != tt.want {
				t.Fatalf("%s implements Reconciler = %v, want %v", tt.tool.Name(), ok, tt.want)
			}
		})
	}
}

func TestSideEffectToolsWriteLedgerOnlyOnSuccess(t *testing.T) {
	ctx := context.Background()
	l := newTestLedger(t)
	tool := NewApplyFixTool(l)
	_, err := tool.Execute(ctx, Invocation{RunID: "r1"}, []byte(`{"description":123}`))
	if err == nil {
		t.Fatal("Execute() with bad args returned nil error")
	}
	if entries, _ := l.Entries(ctx); len(entries) != 0 {
		t.Fatalf("ledger has %d entries after a failed Execute, want 0", len(entries))
	}
	if errors.Is(err, ErrOutcomeUnknown) {
		t.Fatal("bad args must not be reported as an unknown outcome")
	}
}

// Reviewer F2: a failure before anything reached the file means "did not
// happen" (a plain error), while failures after a successful write are
// reported as an unknown outcome (see Ledger.Record).
func TestLedgerRecordFailureBeforeWriteIsNotOutcomeUnknown(t *testing.T) {
	// The path is a directory, so opening it for append fails before any write.
	l := NewLedger(t.TempDir())
	err := l.Record(context.Background(), LedgerEntry{RunID: "r"})
	if err == nil {
		t.Fatal("Record() into a directory returned nil error")
	}
	if errors.Is(err, ErrOutcomeUnknown) {
		t.Fatalf("Record() error = %v; a failure before the write must not claim an unknown outcome", err)
	}
}
