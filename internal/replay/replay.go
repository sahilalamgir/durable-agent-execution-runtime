package replay

import (
	"context"
	"fmt"

	"github.com/sahilalamgir/durable-agent-execution-runtime/internal/events"
)

// EventSource returns a run's events in log order. Two implementations
// exist: NewKafkaEventSource (the authoritative source, D-2) and
// internal/store's EventReader (Postgres, cross-check only, D-2, FR-20).
type EventSource interface {
	// Events returns runID's events in log order. It returns
	// ErrRunNotFound if there are none.
	Events(ctx context.Context, runID string) ([]events.Envelope, error)
}

// Replay reads runID's events from src and folds them into a RunState.
func Replay(ctx context.Context, src EventSource, runID string) (RunState, error) {
	evs, err := src.Events(ctx, runID)
	if err != nil {
		return RunState{}, fmt.Errorf("reading events for run %s: %w", runID, err)
	}
	s, err := Fold(evs)
	if err != nil {
		return RunState{}, fmt.Errorf("folding events for run %s: %w", runID, err)
	}
	return s, nil
}

// FoldUpTo folds only the prefix of evs whose sequence_number is <= upToSeq
// (evs is assumed to already be in sequence_number order, as every
// EventSource guarantees). scripts/replay's --up-to-seq flag uses this to
// reconstruct state as of an earlier point than the source's latest event —
// e.g. to compare against a worker's last printed digest from just before a
// DAE_CRASH_AFTER_SEQ-induced crash (AC-13), where the crashed event itself
// was applied but never logged.
func FoldUpTo(evs []events.Envelope, upToSeq int64) (RunState, error) {
	var truncated []events.Envelope
	for _, e := range evs {
		if e.SequenceNumber > upToSeq {
			break
		}
		truncated = append(truncated, e)
	}
	return Fold(truncated)
}
