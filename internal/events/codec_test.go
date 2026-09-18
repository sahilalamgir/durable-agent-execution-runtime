package events

import (
	"encoding/json"
	"errors"
	"testing"
	"time"
)

// TestCodecRoundTrip is AC-1's round-trip half: each of the 9 event types
// survives Encode -> Decode -> DecodePayload unchanged.
func TestCodecRoundTrip(t *testing.T) {
	now := time.Date(2026, 9, 17, 18, 4, 5, 123456000, time.UTC)

	tests := []struct {
		name    string
		payload Payload
	}{
		{
			name: "RunStarted",
			payload: RunStartedPayload{
				WorkloadType: "repo-maintenance-agent",
				Input:        json.RawMessage(`{"prompt":"fix the widget parser"}`),
				MaxSteps:     10,
			},
		},
		{
			name: "LLMResponded",
			payload: LLMRespondedPayload{
				Step:       1,
				StopReason: "tool_use",
				Content:    json.RawMessage(`[{"type":"tool_use","id":"toolu_01A","name":"clone_repo","input":{"repo_url":"https://github.com/example/widgets.git"}}]`),
				IsTerminal: false,
			},
		},
		{
			name: "ToolInvoked",
			payload: ToolInvokedPayload{
				Step:           1,
				ToolUseID:      "toolu_01A",
				ToolName:       "clone_repo",
				ToolArgs:       json.RawMessage(`{"repo_url":"https://github.com/example/widgets.git"}`),
				IdempotencyKey: nil,
				HasSideEffect:  false,
			},
		},
		{
			name: "ToolResulted",
			payload: ToolResultedPayload{
				Step:                 1,
				ToolUseID:            "toolu_01A",
				ToolName:             "clone_repo",
				Result:               "cloned into /workspace/widgets",
				Status:               "success",
				WasReplayedFromCache: false,
			},
		},
		{
			name: "AwaitingApproval",
			payload: AwaitingApprovalPayload{
				Step:            3,
				Reason:          "opening a PR requires approval",
				ApprovalPayload: json.RawMessage(`{"pr_title":"Fix widget parsing"}`),
			},
		},
		{
			name: "RunApproved",
			payload: RunApprovedPayload{
				Step:       3,
				ApprovedBy: "sahil",
				Decision:   "APPROVE",
				Notes:      "looks good",
			},
		},
		{
			name: "RunCompleted",
			payload: RunCompletedPayload{
				FinalOutput: "Done, PR #42 opened.",
				TotalSteps:  6,
			},
		},
		{
			name: "RunFailed",
			payload: RunFailedPayload{
				Step:         4,
				ErrorClass:   "max_steps_exceeded",
				ErrorMessage: "exceeded max_steps=10",
				Retryable:    false,
			},
		},
		{
			name: "RunCancelled",
			payload: RunCancelledPayload{
				Step:        2,
				CancelledBy: "sahil",
				Reason:      "no longer needed",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			env, err := NewEnvelope("7f1c4b8e-2d3a-4f6b-9c1e-0a2b3c4d5e6f", "local-dev", 0, tt.payload, now)
			if err != nil {
				t.Fatalf("NewEnvelope() error = %v", err)
			}
			if env.EventType != tt.payload.EventType() {
				t.Fatalf("EventType = %q, want %q", env.EventType, tt.payload.EventType())
			}
			if env.IdempotencyKey != nil {
				t.Fatalf("IdempotencyKey = %v, want nil (Phase 2)", *env.IdempotencyKey)
			}
			if !env.OccurredAt.Equal(now) {
				t.Fatalf("OccurredAt = %v, want %v", env.OccurredAt, now)
			}

			b, err := Encode(env)
			if err != nil {
				t.Fatalf("Encode() error = %v", err)
			}

			decoded, err := Decode(b)
			if err != nil {
				t.Fatalf("Decode() error = %v", err)
			}
			if decoded.EventID != env.EventID || decoded.RunID != env.RunID || decoded.SequenceNumber != env.SequenceNumber {
				t.Fatalf("Decode() envelope = %+v, want %+v", decoded, env)
			}
			if !decoded.OccurredAt.Equal(env.OccurredAt) {
				t.Fatalf("Decode() OccurredAt = %v, want %v", decoded.OccurredAt, env.OccurredAt)
			}

			payload, err := DecodePayload(decoded)
			if err != nil {
				t.Fatalf("DecodePayload() error = %v", err)
			}

			wantJSON, err := json.Marshal(tt.payload)
			if err != nil {
				t.Fatalf("marshaling want payload: %v", err)
			}
			gotJSON, err := json.Marshal(payload)
			if err != nil {
				t.Fatalf("marshaling got payload: %v", err)
			}
			if string(gotJSON) != string(wantJSON) {
				t.Fatalf("DecodePayload() = %s, want %s", gotJSON, wantJSON)
			}
		})
	}
}

// TestDecodeErrors is AC-1's error half: Decode rejects an unknown
// event_type, a missing event_id, a missing run_id, and invalid JSON.
func TestDecodeErrors(t *testing.T) {
	tests := []struct {
		name    string
		raw     string
		wantErr error // nil means "some error", checked via err == nil
	}{
		{
			name:    "unknown event_type",
			raw:     `{"event_id":"e1","run_id":"r1","tenant_id":"local-dev","sequence_number":0,"event_type":"NotARealEvent","payload":{},"occurred_at":"2026-09-17T18:04:05.123456Z","idempotency_key":null}`,
			wantErr: ErrUnknownEventType,
		},
		{
			name:    "missing event_id",
			raw:     `{"run_id":"r1","tenant_id":"local-dev","sequence_number":0,"event_type":"RunStarted","payload":{},"occurred_at":"2026-09-17T18:04:05.123456Z","idempotency_key":null}`,
			wantErr: ErrMissingField,
		},
		{
			name:    "missing run_id",
			raw:     `{"event_id":"e1","tenant_id":"local-dev","sequence_number":0,"event_type":"RunStarted","payload":{},"occurred_at":"2026-09-17T18:04:05.123456Z","idempotency_key":null}`,
			wantErr: ErrMissingField,
		},
		{
			name:    "invalid json",
			raw:     `{not valid json`,
			wantErr: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Decode([]byte(tt.raw))
			if err == nil {
				t.Fatal("Decode() error = nil, want non-nil")
			}
			if tt.wantErr != nil && !errors.Is(err, tt.wantErr) {
				t.Fatalf("Decode() error = %v, want errors.Is(_, %v)", err, tt.wantErr)
			}
		})
	}
}

// TestDecodePayloadInvalid checks DecodePayload's ErrInvalidPayload path: a
// payload whose JSON doesn't match its event type's struct shape.
func TestDecodePayloadInvalid(t *testing.T) {
	env := Envelope{
		EventID:        "e1",
		RunID:          "r1",
		TenantID:       "local-dev",
		SequenceNumber: 0,
		EventType:      EventRunStarted,
		Payload:        json.RawMessage(`{"max_steps":"not-a-number"}`),
		OccurredAt:     time.Now().UTC(),
	}
	_, err := DecodePayload(env)
	if !errors.Is(err, ErrInvalidPayload) {
		t.Fatalf("DecodePayload() error = %v, want errors.Is(_, ErrInvalidPayload)", err)
	}
}
