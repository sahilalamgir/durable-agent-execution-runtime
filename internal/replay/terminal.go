package replay

import (
	"fmt"
	"strings"

	"github.com/anthropics/anthropic-sdk-go"

	"github.com/sahilalamgir/durable-agent-execution-runtime/internal/events"
)

// TerminalPayload returns the terminal event a worker must publish when
// NextAction is FINISH_RUN: RunCompleted, or RunFailed with
// llm_output_truncated / malformed_llm_response (Phase 2 FR-10). The live
// loop and a resumed run both emit their finish through this one method, so
// they cannot drift apart.
func (s RunState) TerminalPayload() (events.Payload, error) {
	if s.NextAction != ActionFinishRun {
		return nil, fmt.Errorf("terminal payload: next_action is %s, not %s", s.NextAction, ActionFinishRun)
	}
	last := s.steps[len(s.steps)-1]

	switch {
	case !last.isTerminal:
		return events.RunFailedPayload{
			Step:         s.CurrentStep,
			ErrorClass:   "malformed_llm_response",
			ErrorMessage: "llm reported stop_reason=tool_use but the response had no tool_use content block",
			Retryable:    false,
		}, nil
	case last.stopReason == string(anthropic.StopReasonMaxTokens):
		return events.RunFailedPayload{
			Step:         s.CurrentStep,
			ErrorClass:   "llm_output_truncated",
			ErrorMessage: "llm response was truncated (stop_reason=max_tokens)",
			Retryable:    false,
		}, nil
	default:
		return events.RunCompletedPayload{
			FinalOutput: s.LastStepText(),
			TotalSteps:  s.CurrentStep,
		}, nil
	}
}

// LastStepText joins the text blocks of the latest LLMResponded, in order:
// the model's accumulated answer once it stops calling tools.
func (s RunState) LastStepText() string {
	if len(s.steps) == 0 {
		return ""
	}
	var parts []string
	for _, block := range s.steps[len(s.steps)-1].content {
		if b, ok := block.AsAny().(anthropic.TextBlock); ok {
			parts = append(parts, b.Text)
		}
	}
	return strings.Join(parts, "")
}

// ToolResult summarizes one journaled ToolResulted, for display.
type ToolResult struct {
	ToolUseID            string
	ToolName             string
	Status               string
	Resolution           string
	WasReplayedFromCache bool
}

// ToolResults returns every ToolResulted applied so far, in tool_use order
// within each step. Resolution is "" for events journaled before Phase 3.
func (s RunState) ToolResults() []ToolResult {
	var out []ToolResult
	for _, step := range s.steps {
		for _, id := range step.order {
			r, ok := step.results[id]
			if !ok {
				continue
			}
			out = append(out, ToolResult{
				ToolUseID: id, ToolName: r.ToolName, Status: r.Status,
				Resolution: r.Resolution, WasReplayedFromCache: r.WasReplayedFromCache,
			})
		}
	}
	return out
}
