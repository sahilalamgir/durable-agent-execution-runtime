package store

import (
	"encoding/json"
	"testing"

	"github.com/sahilalamgir/durable-agent-execution-runtime/internal/events"
)

func TestStatusForEvent(t *testing.T) {
	tests := []struct {
		eventType events.EventType
		want      string
	}{
		{events.EventRunStarted, "RUNNING"},
		{events.EventLLMResponded, "RUNNING"},
		{events.EventToolInvoked, "RUNNING"},
		{events.EventToolResulted, "RUNNING"},
		{events.EventRunApproved, "RUNNING"},
		{events.EventAwaitingApproval, "AWAITING_APPROVAL"},
		{events.EventRunCompleted, "COMPLETED"},
		{events.EventRunFailed, "FAILED"},
		{events.EventRunCancelled, "CANCELLED"},
	}
	for _, tt := range tests {
		t.Run(string(tt.eventType), func(t *testing.T) {
			if got := statusForEvent(tt.eventType); got != tt.want {
				t.Errorf("statusForEvent(%s) = %q, want %q", tt.eventType, got, tt.want)
			}
		})
	}
}

func TestStepFromPayload(t *testing.T) {
	tests := []struct {
		name     string
		payload  events.Payload
		wantStep int
		wantOK   bool
	}{
		{
			name:    "RunStarted has no step",
			payload: events.RunStartedPayload{WorkloadType: "repo-maintenance-agent", Input: json.RawMessage(`{}`), MaxSteps: 10},
			wantOK:  false,
		},
		{
			name:     "LLMResponded",
			payload:  events.LLMRespondedPayload{Step: 3, StopReason: "tool_use", Content: json.RawMessage(`[]`)},
			wantStep: 3,
			wantOK:   true,
		},
		{
			name:     "ToolInvoked",
			payload:  events.ToolInvokedPayload{Step: 2, ToolUseID: "toolu_1", ToolName: "clone_repo"},
			wantStep: 2,
			wantOK:   true,
		},
		{
			name:     "ToolResulted",
			payload:  events.ToolResultedPayload{Step: 2, ToolUseID: "toolu_1", ToolName: "clone_repo", Status: "success"},
			wantStep: 2,
			wantOK:   true,
		},
		{
			name:     "AwaitingApproval",
			payload:  events.AwaitingApprovalPayload{Step: 4, Reason: "needs approval", ApprovalPayload: json.RawMessage(`{}`)},
			wantStep: 4,
			wantOK:   true,
		},
		{
			name:     "RunApproved",
			payload:  events.RunApprovedPayload{Step: 4, ApprovedBy: "sahil", Decision: "APPROVE"},
			wantStep: 4,
			wantOK:   true,
		},
		{
			name:    "RunCompleted has no step",
			payload: events.RunCompletedPayload{FinalOutput: "done", TotalSteps: 6},
			wantOK:  false,
		},
		{
			name:     "RunFailed",
			payload:  events.RunFailedPayload{Step: 5, ErrorClass: "max_steps_exceeded"},
			wantStep: 5,
			wantOK:   true,
		},
		{
			name:     "RunCancelled",
			payload:  events.RunCancelledPayload{Step: 2, CancelledBy: "sahil", Reason: "no longer needed"},
			wantStep: 2,
			wantOK:   true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			step, ok := stepFromPayload(tt.payload)
			if ok != tt.wantOK {
				t.Fatalf("stepFromPayload() ok = %v, want %v", ok, tt.wantOK)
			}
			if ok && step != tt.wantStep {
				t.Errorf("stepFromPayload() step = %d, want %d", step, tt.wantStep)
			}
		})
	}
}
