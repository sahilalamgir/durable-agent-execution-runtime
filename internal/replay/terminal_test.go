package replay

import (
	"testing"
	"time"

	"github.com/sahilalamgir/durable-agent-execution-runtime/internal/events"
)

func TestTerminalPayload(t *testing.T) {
	tests := []struct {
		name       string
		stopReason string
		content    string
		isTerminal bool
		want       events.Payload
	}{
		{
			name: "end_turn completes with the joined text", stopReason: "end_turn",
			content: fxTextContent("all done"), isTerminal: true,
			want: events.RunCompletedPayload{FinalOutput: "all done", TotalSteps: 1},
		},
		{
			name: "max_tokens is truncated", stopReason: "max_tokens",
			content: fxTextContent("cut off"), isTerminal: true,
			want: events.RunFailedPayload{
				Step: 1, ErrorClass: "llm_output_truncated",
				ErrorMessage: "llm response was truncated (stop_reason=max_tokens)",
			},
		},
		{
			name: "tool_use without a tool_use block is malformed", stopReason: "tool_use",
			content: fxTextContent("hm"), isTerminal: false,
			want: events.RunFailedPayload{
				Step: 1, ErrorClass: "malformed_llm_response",
				ErrorMessage: "llm reported stop_reason=tool_use but the response had no tool_use content block",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, err := Fold([]events.Envelope{
				fxRunStarted(t, testRunID, 0, 10, "go"),
				fxLLMResponded(t, testRunID, 1, 1, tt.stopReason, tt.content, tt.isTerminal),
			})
			if err != nil {
				t.Fatalf("Fold() error = %v", err)
			}
			if s.NextAction != ActionFinishRun {
				t.Fatalf("NextAction = %v, want FINISH_RUN", s.NextAction)
			}
			got, err := s.TerminalPayload()
			if err != nil || got != tt.want {
				t.Fatalf("TerminalPayload() = %#v, %v; want %#v", got, err, tt.want)
			}
		})
	}
}

func TestTerminalPayloadRejectsOtherActions(t *testing.T) {
	s, err := Fold([]events.Envelope{fxRunStarted(t, testRunID, 0, 10, "go")})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.TerminalPayload(); err == nil {
		t.Fatal("TerminalPayload() with next_action=CALL_LLM returned nil error")
	}
}

func TestInFlightToolCarriesJournaledInvocation(t *testing.T) {
	key := "idem:" + testRunID + ":1:toolu_1"
	invoked := mustEnvelope(t, testRunID, 2, events.ToolInvokedPayload{
		Step: 1, ToolUseID: "toolu_1", ToolName: "open_pr",
		ToolArgs: []byte(`{"title":"T","body":"B"}`), IdempotencyKey: &key, HasSideEffect: true,
	})
	invoked.IdempotencyKey = &key

	s, err := Fold([]events.Envelope{
		fxRunStarted(t, testRunID, 0, 10, "go"),
		fxLLMResponded(t, testRunID, 1, 1, "tool_use",
			fxToolUseContent("", [3]string{"toolu_1", "open_pr", `{"title":"T","body":"B"}`}), false),
		invoked,
	})
	if err != nil {
		t.Fatalf("Fold() error = %v", err)
	}
	if s.NextAction != ActionResolveInFlightTool || s.InFlightTool == nil {
		t.Fatalf("NextAction = %v, InFlightTool = %v", s.NextAction, s.InFlightTool)
	}
	got := s.InFlightTool
	if !got.HasSideEffect || got.IdempotencyKey == nil || *got.IdempotencyKey != key {
		t.Fatalf("InFlightTool = %+v, want side effect with key %q", got, key)
	}
	if want := invoked.OccurredAt; !got.InvokedAt.Equal(want) || got.InvokedAt.Equal(time.Time{}) {
		t.Fatalf("InvokedAt = %v, want %v", got.InvokedAt, want)
	}

}

func TestToolResultsCarryResolution(t *testing.T) {
	resulted := mustEnvelope(t, testRunID, 3, events.ToolResultedPayload{
		Step: 1, ToolUseID: "toolu_1", ToolName: "open_pr", Result: "opened PR #1: T", Status: "success",
		WasReplayedFromCache: true, Resolution: "cached",
	})
	s, err := Fold([]events.Envelope{
		fxRunStarted(t, testRunID, 0, 10, "go"),
		fxLLMResponded(t, testRunID, 1, 1, "tool_use",
			fxToolUseContent("", [3]string{"toolu_1", "open_pr", `{}`}), false),
		fxToolInvoked(t, testRunID, 2, 1, "toolu_1", "open_pr", `{}`, true),
		resulted,
	})
	if err != nil {
		t.Fatalf("Fold() error = %v", err)
	}
	got := s.ToolResults()
	want := []ToolResult{{ToolUseID: "toolu_1", ToolName: "open_pr", Status: "success", Resolution: "cached", WasReplayedFromCache: true}}
	if len(got) != 1 || got[0] != want[0] {
		t.Fatalf("ToolResults() = %+v, want %+v", got, want)
	}
}

// A terminal response is never followed by tool execution, even when its
// content has a tool_use block (e.g. a max_tokens response cut off mid-call).
func TestTerminalStepWithToolUseBlockFinishesInsteadOfExecuting(t *testing.T) {
	s, err := Fold([]events.Envelope{
		fxRunStarted(t, testRunID, 0, 10, "go"),
		fxLLMResponded(t, testRunID, 1, 1, "max_tokens",
			fxToolUseContent("", [3]string{"toolu_1", "apply_fix", `{"description":"cut"}`}), true),
	})
	if err != nil {
		t.Fatalf("Fold() error = %v", err)
	}
	if s.NextAction != ActionFinishRun {
		t.Fatalf("NextAction = %v, want FINISH_RUN", s.NextAction)
	}
	got, err := s.TerminalPayload()
	if err != nil {
		t.Fatal(err)
	}
	if failed, ok := got.(events.RunFailedPayload); !ok || failed.ErrorClass != "llm_output_truncated" {
		t.Fatalf("TerminalPayload() = %#v, want llm_output_truncated", got)
	}
}
