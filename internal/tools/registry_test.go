package tools

import (
	"context"
	"testing"
)

func TestRegistryLookup(t *testing.T) {
	reg := NewRegistry(NewCloneRepoTool(), NewRunTestsTool())

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
	reg := NewRegistry(
		NewOpenPRTool(),
		NewCloneRepoTool(),
		NewApplyFixTool(),
		NewRunTestsTool(),
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
			got, err := tool.Execute(context.Background(), []byte(tt.args))
			if (err != nil) != tt.wantErr {
				t.Fatalf("Execute() error = %v, wantErr %v", err, tt.wantErr)
			}
			if !tt.wantErr && got != tt.want {
				t.Fatalf("Execute() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestRunTestsToolExecute(t *testing.T) {
	tests := []struct {
		name string
		call int
		want string
	}{
		{"first call reports failures", 1, "2 tests failed: TestParseWidget, TestWidgetTotal"},
		{"second call reports success", 2, "all tests passed (2/2)"},
		{"third call still reports success", 3, "all tests passed (2/2)"},
	}

	tool := NewRunTestsTool()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tool.Execute(context.Background(), []byte(`{}`))
			if err != nil {
				t.Fatalf("Execute() error = %v", err)
			}
			if got != tt.want {
				t.Fatalf("Execute() call %d = %q, want %q", tt.call, got, tt.want)
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
			tool := NewApplyFixTool()
			got, err := tool.Execute(context.Background(), []byte(tt.args))
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
		{"succeeds", `{"title":"Fix widget parsing","body":"Fixes failing tests."}`, "opened PR #42: Fix widget parsing", false},
		{"malformed args", `{"title":123,"body":"Fixes failing tests."}`, "", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tool := NewOpenPRTool()
			got, err := tool.Execute(context.Background(), []byte(tt.args))
			if (err != nil) != tt.wantErr {
				t.Fatalf("Execute() error = %v, wantErr %v", err, tt.wantErr)
			}
			if !tt.wantErr && got != tt.want {
				t.Fatalf("Execute() = %q, want %q", got, tt.want)
			}
		})
	}
}
