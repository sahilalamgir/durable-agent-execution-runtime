// Package replay implements the pure fold that reconstructs a run's state
// from its events (FR-12): Apply/Fold have no I/O, no LLM calls, no tool
// calls, and no clock reads, so the exact same code path drives both the
// live worker's conversation history (FR-13, D-1) and a completely
// after-the-fact replay from Kafka or Postgres.
package replay

import (
	"encoding/json"
	"time"

	"github.com/anthropics/anthropic-sdk-go"

	"github.com/sahilalamgir/durable-agent-execution-runtime/internal/events"
)

// RunStatus is a run's coarse lifecycle state, mirroring the runs table's
// status column (system-spec.md's Postgres Schema).
type RunStatus string

const (
	StatusRunning          RunStatus = "RUNNING"
	StatusAwaitingApproval RunStatus = "AWAITING_APPROVAL"
	StatusCompleted        RunStatus = "COMPLETED"
	StatusFailed           RunStatus = "FAILED"
	StatusCancelled        RunStatus = "CANCELLED"
)

// NextAction is what the fold computes a worker should do next, given a
// run's state so far. See the NextAction truth table in
// phase2-event-journal.md's API Contracts section.
type NextAction string

const (
	ActionCallLLM             NextAction = "CALL_LLM"
	ActionExecuteTool         NextAction = "EXECUTE_TOOL"
	ActionResolveInFlightTool NextAction = "RESOLVE_IN_FLIGHT_TOOL"
	ActionFailMaxSteps        NextAction = "FAIL_MAX_STEPS"
	ActionNone                NextAction = "NONE"
)

// ToolCall identifies one tool_use block: a pending call the worker hasn't
// invoked yet, or the one currently in flight.
type ToolCall struct {
	ToolUseID string
	ToolName  string
	ToolArgs  json.RawMessage
}

// stepRecord holds everything the fold has journaled for one LLMResponded
// step: its content blocks (decoded once, here, from the payload's raw
// JSON) and whatever tool results have arrived so far for it.
type stepRecord struct {
	// content is this step's response content blocks, verbatim from
	// LLMResponded.Content, decoded once so Messages() and the
	// invariant/order checks never need to re-decode it.
	content []anthropic.ContentBlockUnion
	// isTerminal mirrors this step's LLMResponded.IsTerminal.
	isTerminal bool
	// order lists the tool_use_ids in content, in content order. Empty
	// means this step's response had no tool_use blocks at all.
	order []string
	// invokedOrder lists the tool_use_ids that have received a ToolInvoked
	// so far, in the order they were invoked.
	invokedOrder []string
	// results holds the journaled ToolResultedPayload for each tool_use_id
	// that has resulted so far, keyed by tool_use_id. It stores the raw
	// payload (Result/Status), not a reconstructed content block: there is
	// no ContentBlockUnion shape for a tool result (that is a request-side
	// type in the SDK, never a response block). Messages() builds the
	// request-side anthropic.NewToolResultBlock from this payload at read
	// time instead.
	results map[string]events.ToolResultedPayload
}

// RunState is a run's complete state as of some event, computed purely by
// folding that run's events in order. See FR-14 for the field list this
// mirrors, and the NextAction truth table in phase2-event-journal.md.
type RunState struct {
	RunID    string
	TenantID string
	Status   RunStatus
	// CurrentStep is the step of the latest LLMResponded; 0 before any.
	CurrentStep int
	MaxSteps    int
	// NextSequence is the sequence_number the next event must use.
	NextSequence int64
	LastEventID  string
	LastEventAt  time.Time
	// PendingToolCalls holds the current step's tool_use blocks that have
	// no ToolInvoked yet, in content order.
	PendingToolCalls []ToolCall
	// InFlightTool is the tool_use block with a ToolInvoked but no
	// ToolResulted yet, or nil if none.
	InFlightTool *ToolCall
	NextAction   NextAction
	// FinalOutput is set by RunCompleted.
	FinalOutput string
	// Failure is set by RunFailed.
	Failure *events.RunFailedPayload

	// prompt is the run's original task prompt, decoded once from
	// RunStarted's input ({"prompt": string} for this workload) so
	// Messages() never needs to touch raw JSON.
	prompt string
	// steps holds one stepRecord per LLMResponded applied so far, in
	// order.
	steps []stepRecord
	// seen maps sequence_number to the event_id that was applied at that
	// sequence, so a redelivered event (same seq, same event_id) can be
	// detected and skipped, and a conflicting one (same seq, different
	// event_id) rejected (FR-16).
	seen map[int64]string
}

// cloneRunState deep-copies every slice and map s owns, so Apply can safely
// mutate the returned copy without ever writing into memory the caller's
// state still holds (EC-17). This goes one level deeper than copying slice
// headers: a ToolCall's ToolArgs and a content block's Input are both
// json.RawMessage ([]byte) — copying the ToolCall/ContentBlockUnion struct
// alone (e.g. `append([]ToolCall(nil), s.PendingToolCalls...)`) copies the
// slice header but leaves it pointing at the same backing byte array the
// caller's copy still references. cloneToolCall/cloneContentBlocks copy
// those bytes too, so nothing here shares a backing array OR a backing byte
// slice with s.
func cloneRunState(s RunState) RunState {
	next := s

	next.PendingToolCalls = make([]ToolCall, len(s.PendingToolCalls))
	for i, tc := range s.PendingToolCalls {
		next.PendingToolCalls[i] = cloneToolCall(tc)
	}

	if s.InFlightTool != nil {
		cp := cloneToolCall(*s.InFlightTool)
		next.InFlightTool = &cp
	}
	if s.Failure != nil {
		cp := *s.Failure
		next.Failure = &cp
	}

	next.steps = make([]stepRecord, len(s.steps))
	for i, step := range s.steps {
		next.steps[i] = cloneStepRecord(step)
	}

	next.seen = make(map[int64]string, len(s.seen))
	for k, v := range s.seen {
		next.seen[k] = v
	}

	return next
}

func cloneStepRecord(s stepRecord) stepRecord {
	next := stepRecord{
		content:      cloneContentBlocks(s.content),
		isTerminal:   s.isTerminal,
		order:        append([]string(nil), s.order...),
		invokedOrder: append([]string(nil), s.invokedOrder...),
	}
	next.results = make(map[string]events.ToolResultedPayload, len(s.results))
	for k, v := range s.results {
		next.results[k] = v
	}
	return next
}

// cloneToolCall copies tc, including a fresh backing array for ToolArgs.
func cloneToolCall(tc ToolCall) ToolCall {
	tc.ToolArgs = append(json.RawMessage(nil), tc.ToolArgs...)
	return tc
}

// cloneContentBlocks copies blocks, including a fresh backing array for
// each ToolUseBlock-variant element's Input. Ranging over blocks already
// gives a copy of each ContentBlockUnion value (it's a struct, not a
// pointer), so mutating that copy's Input field and storing it back never
// touches the original slice.
func cloneContentBlocks(blocks []anthropic.ContentBlockUnion) []anthropic.ContentBlockUnion {
	cloned := make([]anthropic.ContentBlockUnion, len(blocks))
	for i, b := range blocks {
		b.Input = append(json.RawMessage(nil), b.Input...)
		cloned[i] = b
	}
	return cloned
}
