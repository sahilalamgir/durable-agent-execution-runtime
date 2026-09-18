package store

import (
	"github.com/sahilalamgir/durable-agent-execution-runtime/internal/events"
	"github.com/sahilalamgir/durable-agent-execution-runtime/internal/replay"
)

// statusForEvent implements FR-25's status mapping table. It is pure and
// DB-free specifically so it can be unit-tested without Postgres.
func statusForEvent(t events.EventType) string {
	switch t {
	case events.EventAwaitingApproval:
		return string(replay.StatusAwaitingApproval)
	case events.EventRunCompleted:
		return string(replay.StatusCompleted)
	case events.EventRunFailed:
		return string(replay.StatusFailed)
	case events.EventRunCancelled:
		return string(replay.StatusCancelled)
	default:
		// RunStarted, LLMResponded, ToolInvoked, ToolResulted, RunApproved.
		return string(replay.StatusRunning)
	}
}

// stepFromPayload extracts a payload's step field, if it has one (FR-25:
// "sets current_step = payload.step when the payload has one"). It is pure
// and DB-free for the same reason as statusForEvent.
//
// RunStartedPayload and RunCompletedPayload have no step field: the
// projector leaves current_step at 0 (RunStarted's insert-time default) or
// unchanged (RunCompleted, since the run's last real step was already
// recorded by an earlier LLMResponded/ToolInvoked/ToolResulted).
func stepFromPayload(payload events.Payload) (step int, ok bool) {
	switch p := payload.(type) {
	case events.LLMRespondedPayload:
		return p.Step, true
	case events.ToolInvokedPayload:
		return p.Step, true
	case events.ToolResultedPayload:
		return p.Step, true
	case events.AwaitingApprovalPayload:
		return p.Step, true
	case events.RunApprovedPayload:
		return p.Step, true
	case events.RunFailedPayload:
		return p.Step, true
	case events.RunCancelledPayload:
		return p.Step, true
	default:
		return 0, false
	}
}
