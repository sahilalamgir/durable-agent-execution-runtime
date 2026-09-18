package replay

import (
	"errors"
	"fmt"
)

var (
	// ErrRunNotFound is returned by an EventSource when a run has no
	// events at all.
	ErrRunNotFound = errors.New("run not found")
	// ErrFirstEventNotStart is returned when the first event applied to an
	// empty RunState is not RunStarted at sequence_number 0.
	ErrFirstEventNotStart = errors.New("first event is not RunStarted at sequence 0")
	// ErrSequenceGap is returned when an event's sequence_number isn't
	// exactly one more than the last applied sequence_number.
	ErrSequenceGap = errors.New("sequence gap")
	// ErrConflictingEvent is returned when an event's sequence_number
	// matches one already applied, but its event_id differs (a redelivery
	// with a matching event_id is skipped silently instead, not an
	// error).
	ErrConflictingEvent = errors.New("conflicting event at existing sequence number")
	// ErrEventAfterTerminal is returned when an event is applied to a
	// RunState whose Status is already terminal (COMPLETED, FAILED, or
	// CANCELLED).
	ErrEventAfterTerminal = errors.New("event after terminal event")
	// ErrRunIDMismatch is returned when an event's run_id doesn't match
	// the RunState's.
	ErrRunIDMismatch = errors.New("event run_id does not match run")
	// ErrUnknownToolUseID is returned when a ToolInvoked or ToolResulted
	// references a tool_use_id that isn't among the current step's
	// LLMResponded content.
	ErrUnknownToolUseID = errors.New("tool event references unknown tool_use_id")
	// ErrToolOrder is returned when tool events violate the required
	// ordering: a ToolResulted before its ToolInvoked, a second
	// ToolInvoked or ToolResulted for the same tool_use_id, or a new
	// LLMResponded while the current step's tool batch isn't fully
	// resolved yet (the last case isn't named explicitly by FR-16's text,
	// but Messages()'s partial-batch handling (EC-5) depends on it always
	// being true of any state the fold accepts).
	ErrToolOrder = errors.New("tool events out of order")
)

// wrapSeq wraps err with the offending sequence_number, per
// phase2-event-journal.md's error-wrapping convention.
func wrapSeq(seq int64, err error) error {
	return fmt.Errorf("applying seq %d: %w", seq, err)
}
