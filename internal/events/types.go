// Package events defines the run-events envelope and per-type payload
// structs, and the codec that (de)serializes them. This package has no
// dependency on Kafka, Postgres, or the agent loop: it is pure data plus
// (de)serialization, so every other package can depend on it without pulling
// in I/O.
package events

import "encoding/json"

// EventType identifies one of the 9 event types a run can produce, per
// system-spec.md's Kafka Topic Contracts section.
type EventType string

const (
	EventRunStarted       EventType = "RunStarted"
	EventLLMResponded     EventType = "LLMResponded"
	EventToolInvoked      EventType = "ToolInvoked"
	EventToolResulted     EventType = "ToolResulted"
	EventAwaitingApproval EventType = "AwaitingApproval"
	EventRunApproved      EventType = "RunApproved"
	EventRunCompleted     EventType = "RunCompleted"
	EventRunFailed        EventType = "RunFailed"
	EventRunCancelled     EventType = "RunCancelled"
)

// knownEventTypes is the set Decode validates event_type against.
var knownEventTypes = map[EventType]bool{
	EventRunStarted:       true,
	EventLLMResponded:     true,
	EventToolInvoked:      true,
	EventToolResulted:     true,
	EventAwaitingApproval: true,
	EventRunApproved:      true,
	EventRunCompleted:     true,
	EventRunFailed:        true,
	EventRunCancelled:     true,
}

// Payload is implemented by every per-type payload struct below. It lets
// NewEnvelope accept any of them without a type switch at the call site.
type Payload interface {
	EventType() EventType
}

// RunStartedPayload is EventRunStarted's payload. Input is workload-specific;
// for the Phase 1/2 repo-maintenance workload it is {"prompt": string}.
type RunStartedPayload struct {
	WorkloadType string          `json:"workload_type"`
	Input        json.RawMessage `json:"input"`
	MaxSteps     int             `json:"max_steps"`
}

func (RunStartedPayload) EventType() EventType { return EventRunStarted }

// LLMRespondedPayload is EventLLMResponded's payload. Content is the
// response's content-block array, verbatim JSON as returned by the Messages
// API (system-spec.md's Phase 2 amendment), so replay can rebuild the exact
// assistant turn including each tool_use block's id.
type LLMRespondedPayload struct {
	Step       int             `json:"step"`
	StopReason string          `json:"stop_reason"`
	Content    json.RawMessage `json:"content"`
	IsTerminal bool            `json:"is_terminal"`
}

func (LLMRespondedPayload) EventType() EventType { return EventLLMResponded }

// ToolInvokedPayload is EventToolInvoked's payload. IdempotencyKey is always
// nil in Phase 2 (Phase 3 introduces real fencing keys).
type ToolInvokedPayload struct {
	Step           int             `json:"step"`
	ToolUseID      string          `json:"tool_use_id"`
	ToolName       string          `json:"tool_name"`
	ToolArgs       json.RawMessage `json:"tool_args"`
	IdempotencyKey *string         `json:"idempotency_key"`
	HasSideEffect  bool            `json:"has_side_effect"`
}

func (ToolInvokedPayload) EventType() EventType { return EventToolInvoked }

// ToolResultedPayload is EventToolResulted's payload. Status is "success" or
// "error".
type ToolResultedPayload struct {
	Step                 int    `json:"step"`
	ToolUseID            string `json:"tool_use_id"`
	ToolName             string `json:"tool_name"`
	Result               string `json:"result"`
	Status               string `json:"status"`
	WasReplayedFromCache bool   `json:"was_replayed_from_cache"`
}

func (ToolResultedPayload) EventType() EventType { return EventToolResulted }

// AwaitingApprovalPayload is EventAwaitingApproval's payload (Phase 4).
type AwaitingApprovalPayload struct {
	Step            int             `json:"step"`
	Reason          string          `json:"reason"`
	ApprovalPayload json.RawMessage `json:"approval_payload"`
}

func (AwaitingApprovalPayload) EventType() EventType { return EventAwaitingApproval }

// RunApprovedPayload is EventRunApproved's payload (Phase 4). Decision is
// "APPROVE" or "REJECT".
type RunApprovedPayload struct {
	Step       int    `json:"step"`
	ApprovedBy string `json:"approved_by"`
	Decision   string `json:"decision"`
	Notes      string `json:"notes"`
}

func (RunApprovedPayload) EventType() EventType { return EventRunApproved }

// RunCompletedPayload is EventRunCompleted's payload.
type RunCompletedPayload struct {
	FinalOutput string `json:"final_output"`
	TotalSteps  int    `json:"total_steps"`
}

func (RunCompletedPayload) EventType() EventType { return EventRunCompleted }

// RunFailedPayload is EventRunFailed's payload.
type RunFailedPayload struct {
	Step         int    `json:"step"`
	ErrorClass   string `json:"error_class"`
	ErrorMessage string `json:"error_message"`
	Retryable    bool   `json:"retryable"`
}

func (RunFailedPayload) EventType() EventType { return EventRunFailed }

// RunCancelledPayload is EventRunCancelled's payload (Phase 4/5).
type RunCancelledPayload struct {
	Step        int    `json:"step"`
	CancelledBy string `json:"cancelled_by"`
	Reason      string `json:"reason"`
}

func (RunCancelledPayload) EventType() EventType { return EventRunCancelled }
