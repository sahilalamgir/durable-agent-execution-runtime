package replay

import (
	"fmt"
	"testing"
	"time"

	"github.com/anthropics/anthropic-sdk-go"

	"github.com/sahilalamgir/durable-agent-execution-runtime/internal/events"
)

const (
	testRunID    = "7f1c4b8e-2d3a-4f6b-9c1e-0a2b3c4d5e6f"
	testTenantID = "local-dev"
)

var fixtureNow = time.Date(2026, 9, 17, 18, 4, 5, 0, time.UTC)

// mustEnvelope builds an envelope via events.NewEnvelope, failing the test
// on error rather than returning it, so fixture-building call sites in
// table tests stay one line each.
func mustEnvelope(t *testing.T, runID string, seq int64, p events.Payload) events.Envelope {
	t.Helper()
	env, err := events.NewEnvelope(runID, testTenantID, seq, p, fixtureNow.Add(time.Duration(seq)*time.Second))
	if err != nil {
		t.Fatalf("NewEnvelope(seq=%d) error = %v", seq, err)
	}
	return env
}

func fxRunStarted(t *testing.T, runID string, seq int64, maxSteps int, prompt string) events.Envelope {
	t.Helper()
	return mustEnvelope(t, runID, seq, events.RunStartedPayload{
		WorkloadType: "repo-maintenance-agent",
		Input:        []byte(fmt.Sprintf(`{"prompt":%q}`, prompt)),
		MaxSteps:     maxSteps,
	})
}

// fxToolUseContent builds a content-block JSON array with one text block
// followed by one tool_use block per (toolUseID, toolName, argsJSON) triple.
func fxToolUseContent(text string, calls ...[3]string) string {
	blocks := ""
	if text != "" {
		blocks += fmt.Sprintf(`{"type":"text","text":%q}`, text)
	}
	for _, c := range calls {
		if blocks != "" {
			blocks += ","
		}
		blocks += fmt.Sprintf(`{"type":"tool_use","id":%q,"name":%q,"input":%s}`, c[0], c[1], c[2])
	}
	return "[" + blocks + "]"
}

func fxTextContent(text string) string {
	return fmt.Sprintf(`[{"type":"text","text":%q}]`, text)
}

func fxLLMResponded(t *testing.T, runID string, seq int64, step int, stopReason, contentJSON string, isTerminal bool) events.Envelope {
	t.Helper()
	return mustEnvelope(t, runID, seq, events.LLMRespondedPayload{
		Step:       step,
		StopReason: stopReason,
		Content:    []byte(contentJSON),
		IsTerminal: isTerminal,
	})
}

func fxToolInvoked(t *testing.T, runID string, seq int64, step int, toolUseID, toolName, argsJSON string, hasSideEffect bool) events.Envelope {
	t.Helper()
	return mustEnvelope(t, runID, seq, events.ToolInvokedPayload{
		Step:          step,
		ToolUseID:     toolUseID,
		ToolName:      toolName,
		ToolArgs:      []byte(argsJSON),
		HasSideEffect: hasSideEffect,
	})
}

func fxToolResulted(t *testing.T, runID string, seq int64, step int, toolUseID, toolName, result, status string) events.Envelope {
	t.Helper()
	return mustEnvelope(t, runID, seq, events.ToolResultedPayload{
		Step:      step,
		ToolUseID: toolUseID,
		ToolName:  toolName,
		Result:    result,
		Status:    status,
	})
}

func fxRunCompleted(t *testing.T, runID string, seq int64, finalOutput string, totalSteps int) events.Envelope {
	t.Helper()
	return mustEnvelope(t, runID, seq, events.RunCompletedPayload{
		FinalOutput: finalOutput,
		TotalSteps:  totalSteps,
	})
}

func fxRunFailed(t *testing.T, runID string, seq int64, step int, errorClass, errorMessage string, retryable bool) events.Envelope {
	t.Helper()
	return mustEnvelope(t, runID, seq, events.RunFailedPayload{
		Step:         step,
		ErrorClass:   errorClass,
		ErrorMessage: errorMessage,
		Retryable:    retryable,
	})
}

// messageShape summarizes msgs as ["role:blocktype,blocktype", ...], for
// asserting Messages()'s role/block-type shape without asserting exact
// text/args content.
func messageShape(msgs []anthropic.MessageParam) []string {
	shape := make([]string, len(msgs))
	for i, m := range msgs {
		types := ""
		for _, b := range m.Content {
			if types != "" {
				types += ","
			}
			types += blockParamType(b)
		}
		shape[i] = fmt.Sprintf("%s:%s", m.Role, types)
	}
	return shape
}

func blockParamType(b anthropic.ContentBlockParamUnion) string {
	switch {
	case b.OfText != nil:
		return "text"
	case b.OfToolUse != nil:
		return "tool_use"
	case b.OfToolResult != nil:
		return "tool_result"
	default:
		return "other"
	}
}
