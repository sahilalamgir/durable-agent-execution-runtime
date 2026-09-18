package replay

import (
	"encoding/json"
	"fmt"

	"github.com/anthropics/anthropic-sdk-go"

	"github.com/sahilalamgir/durable-agent-execution-runtime/internal/events"
)

// Apply folds e onto s and returns the resulting state. It never mutates s
// (EC-17): every slice and map s owns is copied via cloneRunState before
// anything is written. On any invariant violation (FR-16), Apply returns s
// unchanged alongside a wrapped sentinel error.
//
// Order of operations, deliberately in this sequence:
//  1. Redelivery/conflict check, before anything else can short-circuit it.
//  2. Structural checks that don't need the payload: first-event, run_id,
//     terminal, sequence gap.
//  3. Clone.
//  4. Decode the payload and apply its type-specific transition.
//  5. Recompute derived tool state and NextAction, once, as a pure function
//     of the resulting state.
//  6. Bookkeeping: NextSequence, LastEventID, LastEventAt, seen.
func Apply(s RunState, e events.Envelope) (RunState, error) {
	// (1) Redelivery / conflict check.
	if existingID, ok := s.seen[e.SequenceNumber]; ok {
		if existingID == e.EventID {
			return s, nil // redelivery: identical no-op (EC-2).
		}
		return s, wrapSeq(e.SequenceNumber, ErrConflictingEvent)
	}

	// (2) Structural checks.
	firstEvent := isEmpty(s)
	if firstEvent {
		if e.SequenceNumber != 0 || e.EventType != events.EventRunStarted {
			return s, wrapSeq(e.SequenceNumber, ErrFirstEventNotStart)
		}
	} else {
		if e.RunID != s.RunID {
			return s, wrapSeq(e.SequenceNumber, ErrRunIDMismatch)
		}
		if isTerminalStatus(s.Status) {
			return s, wrapSeq(e.SequenceNumber, ErrEventAfterTerminal)
		}
		if e.SequenceNumber != s.NextSequence {
			return s, wrapSeq(e.SequenceNumber, ErrSequenceGap)
		}
	}

	// (3) Copy before mutating anything.
	next := cloneRunState(s)

	// (4) Decode and apply.
	payload, err := events.DecodePayload(e)
	if err != nil {
		return s, wrapSeq(e.SequenceNumber, err)
	}
	if firstEvent {
		next.RunID = e.RunID
		next.TenantID = e.TenantID
	}
	if err := applyPayload(&next, payload); err != nil {
		return s, wrapSeq(e.SequenceNumber, err)
	}

	// (5) Derived state, computed once.
	deriveToolState(&next)
	next.NextAction = computeNextAction(next)

	// (6) Bookkeeping.
	next.NextSequence = e.SequenceNumber + 1
	next.LastEventID = e.EventID
	next.LastEventAt = e.OccurredAt
	next.seen[e.SequenceNumber] = e.EventID

	return next, nil
}

// Fold applies evs in order, starting from an empty RunState, and returns
// the resulting state. It returns ErrRunNotFound for an empty evs, matching
// EventSource.Events's contract of never returning an empty slice
// successfully.
func Fold(evs []events.Envelope) (RunState, error) {
	if len(evs) == 0 {
		return RunState{}, ErrRunNotFound
	}
	var s RunState
	for _, e := range evs {
		var err error
		s, err = Apply(s, e)
		if err != nil {
			return RunState{}, err
		}
	}
	return s, nil
}

// isEmpty reports whether s has never had an event applied to it.
func isEmpty(s RunState) bool {
	return len(s.seen) == 0
}

// isTerminalStatus reports whether status is one no further event may
// follow (FR-16).
func isTerminalStatus(status RunStatus) bool {
	switch status {
	case StatusCompleted, StatusFailed, StatusCancelled:
		return true
	default:
		return false
	}
}

// applyPayload applies payload's type-specific transition to next, which
// must already have been cloned from the pre-event state.
func applyPayload(next *RunState, payload events.Payload) error {
	switch p := payload.(type) {
	case events.RunStartedPayload:
		return applyRunStarted(next, p)
	case events.LLMRespondedPayload:
		return applyLLMResponded(next, p)
	case events.ToolInvokedPayload:
		return applyToolInvoked(next, p)
	case events.ToolResultedPayload:
		return applyToolResulted(next, p)
	case events.AwaitingApprovalPayload:
		next.Status = StatusAwaitingApproval
		return nil
	case events.RunApprovedPayload:
		next.Status = StatusRunning
		return nil
	case events.RunCompletedPayload:
		next.Status = StatusCompleted
		next.FinalOutput = p.FinalOutput
		return nil
	case events.RunFailedPayload:
		next.Status = StatusFailed
		failure := p
		next.Failure = &failure
		return nil
	case events.RunCancelledPayload:
		next.Status = StatusCancelled
		return nil
	default:
		return fmt.Errorf("applying event: unhandled payload type %T", payload)
	}
}

// runStartedInput is the {"prompt": string} shape system-spec.md's Phase 2
// amendment specifies for this workload's RunStarted.Input.
type runStartedInput struct {
	Prompt string `json:"prompt"`
}

func applyRunStarted(next *RunState, p events.RunStartedPayload) error {
	next.Status = StatusRunning
	next.CurrentStep = 0
	next.MaxSteps = p.MaxSteps

	var input runStartedInput
	if err := json.Unmarshal(p.Input, &input); err != nil {
		return fmt.Errorf("decoding RunStarted input: %w", err)
	}
	next.prompt = input.Prompt
	return nil
}

func applyLLMResponded(next *RunState, p events.LLMRespondedPayload) error {
	// A new LLMResponded must not arrive while the current step's tool
	// batch is still open: Messages()'s partial-batch handling (EC-5)
	// depends on the last step in RunState.steps always being the one a
	// later ToolInvoked/ToolResulted could still apply to.
	if len(next.steps) > 0 {
		prev := next.steps[len(next.steps)-1]
		if len(prev.order) > 0 && !allResulted(prev) {
			return ErrToolOrder
		}
	}

	var content []anthropic.ContentBlockUnion
	if err := json.Unmarshal(p.Content, &content); err != nil {
		return fmt.Errorf("decoding LLMResponded content: %w", err)
	}

	step := stepRecord{
		content:    content,
		isTerminal: p.IsTerminal,
		results:    make(map[string]events.ToolResultedPayload),
	}
	for _, block := range content {
		if tu, ok := block.AsAny().(anthropic.ToolUseBlock); ok {
			step.order = append(step.order, tu.ID)
		}
	}

	next.CurrentStep = p.Step
	next.steps = append(next.steps, step)
	return nil
}

func applyToolInvoked(next *RunState, p events.ToolInvokedPayload) error {
	step, err := currentStepFor(next, p.ToolUseID)
	if err != nil {
		return err
	}
	if contains(step.invokedOrder, p.ToolUseID) {
		return ErrToolOrder // at most one ToolInvoked per tool_use_id.
	}
	step.invokedOrder = append(step.invokedOrder, p.ToolUseID)
	return nil
}

func applyToolResulted(next *RunState, p events.ToolResultedPayload) error {
	step, err := currentStepFor(next, p.ToolUseID)
	if err != nil {
		return err
	}
	if !contains(step.invokedOrder, p.ToolUseID) {
		return ErrToolOrder // a ToolResulted must follow its ToolInvoked.
	}
	if _, already := step.results[p.ToolUseID]; already {
		return ErrToolOrder // exactly one ToolResulted per tool_use_id (FR-9).
	}
	step.results[p.ToolUseID] = p
	return nil
}

// currentStepFor returns a pointer to next's current (last) stepRecord,
// after checking that toolUseID belongs to it. It returns
// ErrUnknownToolUseID if there is no current step at all, or toolUseID
// isn't among its tool_use blocks.
func currentStepFor(next *RunState, toolUseID string) (*stepRecord, error) {
	if len(next.steps) == 0 {
		return nil, ErrUnknownToolUseID
	}
	step := &next.steps[len(next.steps)-1]
	if !contains(step.order, toolUseID) {
		return nil, ErrUnknownToolUseID
	}
	return step, nil
}

func contains(ids []string, id string) bool {
	for _, existing := range ids {
		if existing == id {
			return true
		}
	}
	return false
}

func allResulted(step stepRecord) bool {
	for _, id := range step.order {
		if _, ok := step.results[id]; !ok {
			return false
		}
	}
	return true
}

// deriveToolState recomputes next.PendingToolCalls and next.InFlightTool
// from the current step's content, invokedOrder, and results. It is called
// once per Apply, after the payload's transition has been applied, so
// NextAction can then be computed as a pure function of these fields.
func deriveToolState(next *RunState) {
	next.PendingToolCalls = nil
	next.InFlightTool = nil
	if len(next.steps) == 0 {
		return
	}
	last := next.steps[len(next.steps)-1]

	for _, id := range last.order {
		if _, resulted := last.results[id]; resulted {
			continue
		}
		tc, ok := toolCallFromContent(last, id)
		if !ok {
			continue
		}
		if contains(last.invokedOrder, id) {
			if next.InFlightTool == nil {
				cp := tc
				next.InFlightTool = &cp
			}
			continue
		}
		next.PendingToolCalls = append(next.PendingToolCalls, tc)
	}
}

// toolCallFromContent finds the tool_use block for toolUseID within step's
// content and returns it as a ToolCall.
func toolCallFromContent(step stepRecord, toolUseID string) (ToolCall, bool) {
	for _, block := range step.content {
		tu, ok := block.AsAny().(anthropic.ToolUseBlock)
		if !ok || tu.ID != toolUseID {
			continue
		}
		return ToolCall{ToolUseID: tu.ID, ToolName: tu.Name, ToolArgs: json.RawMessage(tu.Input)}, true
	}
	return ToolCall{}, false
}

// computeNextAction implements the NextAction truth table
// (phase2-event-journal.md's API Contracts section) as a pure function of
// s's already-derived fields (Status, CurrentStep, MaxSteps,
// PendingToolCalls, InFlightTool, and the last step's content/isTerminal).
// It is called exactly once per Apply, never duplicated per event-type
// branch.
func computeNextAction(s RunState) NextAction {
	switch s.Status {
	case StatusCompleted, StatusFailed, StatusCancelled:
		return ActionNone
	case StatusAwaitingApproval:
		// Phase 4 defines the real transition once approval flow exists;
		// Phase 2 never acts on NextAction anyway (D-4), so NONE is a safe
		// placeholder.
		return ActionNone
	}

	if len(s.steps) == 0 {
		return ActionCallLLM
	}

	if s.InFlightTool != nil {
		return ActionResolveInFlightTool
	}
	if len(s.PendingToolCalls) > 0 {
		return ActionExecuteTool
	}

	last := s.steps[len(s.steps)-1]
	if len(last.order) == 0 {
		if last.isTerminal {
			// The worker should emit a terminal event next; Status is
			// still RUNNING until it does (see the truth table's footnote).
			return ActionNone
		}
		// stop_reason == tool_use with zero tool_use blocks: FR-10 maps
		// this straight to RunFailed. Replay should never observe it as a
		// non-terminal state, but there is nothing to do if it did.
		return ActionNone
	}

	if s.CurrentStep < s.MaxSteps {
		return ActionCallLLM
	}
	return ActionFailMaxSteps
}
