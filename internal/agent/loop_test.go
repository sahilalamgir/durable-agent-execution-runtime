package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"testing"

	"github.com/anthropics/anthropic-sdk-go"

	"github.com/sahilalamgir/durable-agent-execution-runtime/internal/tools"
)

// stubLLMClient replays a scripted list of responses, one per
// CreateMessage call. It records how many times it was called so tests can
// assert the loop never calls the LLM more than expected.
type stubLLMClient struct {
	responses []*anthropic.Message
	calls     int
}

func (s *stubLLMClient) CreateMessage(ctx context.Context, params anthropic.MessageNewParams) (*anthropic.Message, error) {
	if s.calls >= len(s.responses) {
		return nil, fmt.Errorf("stubLLMClient: no more scripted responses (called %d times)", s.calls+1)
	}
	resp := s.responses[s.calls]
	s.calls++
	return resp, nil
}

// mustMessage builds an *anthropic.Message fixture from raw JSON shaped
// like a real Messages API response. anthropic.Message's AsAny()/union
// helpers read from an unexported field populated only by UnmarshalJSON, so
// hand-built struct literals will not work with the type switches in
// loop.go — every fixture must go through json.Unmarshal.
func mustMessage(t *testing.T, rawJSON string) *anthropic.Message {
	t.Helper()
	var msg anthropic.Message
	if err := json.Unmarshal([]byte(rawJSON), &msg); err != nil {
		t.Fatalf("unmarshaling fixture message: %v", err)
	}
	return &msg
}

func toolUseFixture(msgID, toolUseID, toolName, inputJSON string) string {
	return fmt.Sprintf(`{"id":%q,"type":"message","role":"assistant","model":"claude-sonnet-5",
"content":[{"type":"tool_use","id":%q,"name":%q,"input":%s}],
"stop_reason":"tool_use","stop_sequence":null,"usage":{"input_tokens":10,"output_tokens":5}}`,
		msgID, toolUseID, toolName, inputJSON)
}

func terminalFixture(msgID, text string) string {
	return fmt.Sprintf(`{"id":%q,"type":"message","role":"assistant","model":"claude-sonnet-5",
"content":[{"type":"text","text":%q}],
"stop_reason":"end_turn","stop_sequence":null,"usage":{"input_tokens":8,"output_tokens":4}}`,
		msgID, text)
}

func maxTokensFixture(msgID, text string) string {
	return fmt.Sprintf(`{"id":%q,"type":"message","role":"assistant","model":"claude-sonnet-5",
"content":[{"type":"text","text":%q}],
"stop_reason":"max_tokens","stop_sequence":null,"usage":{"input_tokens":8,"output_tokens":4}}`,
		msgID, text)
}

func newTestRegistry() *tools.Registry {
	return tools.NewRegistry(
		tools.NewCloneRepoTool(),
		tools.NewRunTestsTool(),
		tools.NewApplyFixTool(),
		tools.NewOpenPRTool(),
	)
}

func TestLoopRun(t *testing.T) {
	tests := []struct {
		name              string
		maxSteps          int
		fixtures          []string
		wantTerminal      bool
		wantMaxStepsExc   bool
		wantSteps         int
		wantStubCallCount int
	}{
		{
			name:     "terminal on first response",
			maxSteps: 10,
			fixtures: []string{
				terminalFixture("msg_1", "Nothing to fix; tests already pass."),
			},
			wantTerminal:      true,
			wantSteps:         1,
			wantStubCallCount: 1,
		},
		{
			name:     "multi-step ends terminal",
			maxSteps: 10,
			fixtures: []string{
				toolUseFixture("msg_1", "toolu_1", "clone_repo", `{"repo_url":"https://github.com/example/widgets.git"}`),
				toolUseFixture("msg_2", "toolu_2", "run_tests", `{}`),
				toolUseFixture("msg_3", "toolu_3", "apply_fix", `{"description":"fix widget parsing"}`),
				toolUseFixture("msg_4", "toolu_4", "run_tests", `{}`),
				toolUseFixture("msg_5", "toolu_5", "open_pr", `{"title":"Fix widget parsing","body":"Fixes failing tests."}`),
				terminalFixture("msg_6", "Done, PR #42 opened."),
			},
			wantTerminal:      true,
			wantSteps:         6,
			wantStubCallCount: 6,
		},
		{
			name:     "max_steps exceeded",
			maxSteps: 3,
			fixtures: []string{
				toolUseFixture("msg_1", "toolu_1", "clone_repo", `{"repo_url":"https://github.com/example/widgets.git"}`),
				toolUseFixture("msg_2", "toolu_2", "clone_repo", `{"repo_url":"https://github.com/example/widgets.git"}`),
				toolUseFixture("msg_3", "toolu_3", "clone_repo", `{"repo_url":"https://github.com/example/widgets.git"}`),
			},
			wantMaxStepsExc:   true,
			wantSteps:         3,
			wantStubCallCount: 3,
		},
		{
			name:     "unknown tool name",
			maxSteps: 10,
			fixtures: []string{
				toolUseFixture("msg_1", "toolu_1", "delete_repo", `{}`),
				terminalFixture("msg_2", "Sorry, I made a mistake, done now."),
			},
			wantTerminal:      true,
			wantSteps:         2,
			wantStubCallCount: 2,
		},
		{
			name:     "malformed args",
			maxSteps: 10,
			fixtures: []string{
				toolUseFixture("msg_1", "toolu_1", "apply_fix", `{"description":123}`),
				terminalFixture("msg_2", "Retried, done."),
			},
			wantTerminal:      true,
			wantSteps:         2,
			wantStubCallCount: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var responses []*anthropic.Message
			for _, raw := range tt.fixtures {
				responses = append(responses, mustMessage(t, raw))
			}
			stub := &stubLLMClient{responses: responses}

			loop := NewLoop(stub, newTestRegistry(), anthropic.ModelClaudeSonnet5, 1024, tt.maxSteps, io.Discard)

			outcome, err := loop.Run(context.Background(), Task{Prompt: "do the thing"})
			if err != nil {
				t.Fatalf("Run() error = %v", err)
			}
			if outcome.Terminal != tt.wantTerminal {
				t.Errorf("Terminal = %v, want %v", outcome.Terminal, tt.wantTerminal)
			}
			if outcome.MaxStepsExceeded != tt.wantMaxStepsExc {
				t.Errorf("MaxStepsExceeded = %v, want %v", outcome.MaxStepsExceeded, tt.wantMaxStepsExc)
			}
			if outcome.Steps != tt.wantSteps {
				t.Errorf("Steps = %d, want %d", outcome.Steps, tt.wantSteps)
			}
			if stub.calls != tt.wantStubCallCount {
				t.Errorf("stub called %d times, want %d", stub.calls, tt.wantStubCallCount)
			}
		})
	}
}

func TestLoopRunTruncated(t *testing.T) {
	stub := &stubLLMClient{responses: []*anthropic.Message{
		mustMessage(t, maxTokensFixture("msg_1", "cut off mid-sente")),
	}}
	loop := NewLoop(stub, newTestRegistry(), anthropic.ModelClaudeSonnet5, 1024, 10, io.Discard)

	outcome, err := loop.Run(context.Background(), Task{Prompt: "do the thing"})
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if !outcome.Terminal {
		t.Errorf("Terminal = %v, want true", outcome.Terminal)
	}
	if !outcome.Truncated {
		t.Errorf("Truncated = %v, want true", outcome.Truncated)
	}
}

func TestNewLoopPanicsOnInvalidMaxSteps(t *testing.T) {
	for _, maxSteps := range []int{0, -1} {
		t.Run(fmt.Sprintf("maxSteps=%d", maxSteps), func(t *testing.T) {
			defer func() {
				if r := recover(); r == nil {
					t.Fatalf("NewLoop(maxSteps=%d) did not panic, want panic", maxSteps)
				}
			}()
			NewLoop(&stubLLMClient{}, newTestRegistry(), anthropic.ModelClaudeSonnet5, 1024, maxSteps, io.Discard)
		})
	}
}

func TestLoopRunLLMError(t *testing.T) {
	stub := &stubLLMClient{} // no scripted responses: first call fails.
	loop := NewLoop(stub, newTestRegistry(), anthropic.ModelClaudeSonnet5, 1024, 10, io.Discard)

	_, err := loop.Run(context.Background(), Task{Prompt: "do the thing"})
	if err == nil {
		t.Fatal("Run() error = nil, want non-nil")
	}
}
