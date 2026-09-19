package tools

import (
	"context"
	"path/filepath"
	"testing"
)

func TestRegistryLookup(t *testing.T) {
	ledger := NewLedger(filepath.Join(t.TempDir(), "ledger.jsonl"))
	reg := NewRegistry(NewCloneRepoTool(), NewRunTestsTool(ledger))

	tests := []struct {
		name     string
		toolName string
		wantOK   bool
	}{
		{"existing tool", "clone_repo", true},
		{"another existing tool", "run_tests", true},
		{"unknown tool", "does_not_exist", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := reg.Lookup(tt.toolName)
			if ok != tt.wantOK {
				t.Fatalf("Lookup(%q) ok = %v, want %v", tt.toolName, ok, tt.wantOK)
			}
			if ok && got.Name() != tt.toolName {
				t.Fatalf("Lookup(%q) returned tool named %q", tt.toolName, got.Name())
			}
		})
	}
}

func TestRegistryDefinitions(t *testing.T) {
	ledger := NewLedger(filepath.Join(t.TempDir(), "ledger.jsonl"))
	reg := NewRegistry(
		NewOpenPRTool(ledger),
		NewCloneRepoTool(),
		NewApplyFixTool(ledger),
		NewRunTestsTool(ledger),
	)

	defs := reg.Definitions()
	if len(defs) != 4 {
		t.Fatalf("len(Definitions()) = %d, want 4", len(defs))
	}

	wantOrder := []string{"apply_fix", "clone_repo", "open_pr", "run_tests"}
	for i, want := range wantOrder {
		if defs[i].OfTool == nil {
			t.Fatalf("Definitions()[%d].OfTool is nil", i)
		}
		if got := defs[i].OfTool.Name; got != want {
			t.Fatalf("Definitions()[%d].Name = %q, want %q", i, got, want)
		}
	}
}

func TestCloneRepoToolExecute(t *testing.T) {
	tests := []struct {
		name    string
		args    string
		want    string
		wantErr bool
	}{
		{"succeeds", `{"repo_url":"https://example.com/repo.git"}`, "cloned https://example.com/repo.git into /workspace/widgets at commit a1b2c3d", false},
		{"malformed args", `{"repo_url":123}`, "", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tool := NewCloneRepoTool()
			got, err := tool.Execute(context.Background(), Invocation{RunID: "run-1", ToolUseID: "toolu_1"}, []byte(tt.args))
			if (err != nil) != tt.wantErr {
				t.Fatalf("Execute() error = %v, wantErr %v", err, tt.wantErr)
			}
			if !tt.wantErr && got != tt.want {
				t.Fatalf("Execute() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestApplyFixToolExecute(t *testing.T) {
	tests := []struct {
		name    string
		args    string
		want    string
		wantErr bool
	}{
		{"succeeds", `{"description":"fix widget parsing"}`, "applied fix: fix widget parsing", false},
		{"malformed args", `{"description":123}`, "", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tool := NewApplyFixTool(NewLedger(filepath.Join(t.TempDir(), "ledger.jsonl")))
			got, err := tool.Execute(context.Background(), Invocation{RunID: "run-1", ToolUseID: "toolu_1"}, []byte(tt.args))
			if (err != nil) != tt.wantErr {
				t.Fatalf("Execute() error = %v, wantErr %v", err, tt.wantErr)
			}
			if !tt.wantErr && got != tt.want {
				t.Fatalf("Execute() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestOpenPRToolExecute(t *testing.T) {
	tests := []struct {
		name    string
		args    string
		want    string
		wantErr bool
	}{
		{"succeeds", `{"title":"Fix widget parsing","body":"Fixes failing tests."}`, "opened PR #1: Fix widget parsing", false},
		{"malformed args", `{"title":123,"body":"Fixes failing tests."}`, "", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tool := NewOpenPRTool(NewLedger(filepath.Join(t.TempDir(), "ledger.jsonl")))
			got, err := tool.Execute(context.Background(), Invocation{RunID: "run-1", ToolUseID: "toolu_1"}, []byte(tt.args))
			if (err != nil) != tt.wantErr {
				t.Fatalf("Execute() error = %v, wantErr %v", err, tt.wantErr)
			}
			if !tt.wantErr && got != tt.want {
				t.Fatalf("Execute() = %q, want %q", got, tt.want)
			}
		})
	}
}
