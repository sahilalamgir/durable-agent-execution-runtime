package replay

import (
	"errors"
	"testing"

	"github.com/sahilalamgir/durable-agent-execution-runtime/internal/events"
)

// TestFoldInvariantViolations is AC-3: one row per FR-16 violation, each
// checked with errors.Is against its sentinel.
func TestFoldInvariantViolations(t *testing.T) {
	tests := []struct {
		name        string
		buildEvents func(t *testing.T) []events.Envelope
		wantErr     error
	}{
		{
			name: "first event is not RunStarted",
			buildEvents: func(t *testing.T) []events.Envelope {
				return []events.Envelope{
					fxLLMResponded(t, testRunID, 0, 1, "end_turn", fxTextContent("hi"), true),
				}
			},
			wantErr: ErrFirstEventNotStart,
		},
		{
			name: "first event is RunStarted but not at sequence 0",
			buildEvents: func(t *testing.T) []events.Envelope {
				return []events.Envelope{
					fxRunStarted(t, testRunID, 1, 10, "do the thing"),
				}
			},
			wantErr: ErrFirstEventNotStart,
		},
		{
			name: "sequence gap",
			buildEvents: func(t *testing.T) []events.Envelope {
				return []events.Envelope{
					fxRunStarted(t, testRunID, 0, 10, "do the thing"),
					fxLLMResponded(t, testRunID, 2, 1, "end_turn", fxTextContent("hi"), true),
				}
			},
			wantErr: ErrSequenceGap,
		},
		{
			name: "run_id mismatch",
			buildEvents: func(t *testing.T) []events.Envelope {
				return []events.Envelope{
					fxRunStarted(t, testRunID, 0, 10, "do the thing"),
					fxLLMResponded(t, "some-other-run-id", 1, 1, "end_turn", fxTextContent("hi"), true),
				}
			},
			wantErr: ErrRunIDMismatch,
		},
		{
			name: "event after terminal event",
			buildEvents: func(t *testing.T) []events.Envelope {
				return []events.Envelope{
					fxRunStarted(t, testRunID, 0, 10, "do the thing"),
					fxLLMResponded(t, testRunID, 1, 1, "end_turn", fxTextContent("hi"), true),
					fxRunCompleted(t, testRunID, 2, "hi", 1),
					fxRunFailed(t, testRunID, 3, 1, "some_error", "should never happen", false),
				}
			},
			wantErr: ErrEventAfterTerminal,
		},
		{
			name: "ToolInvoked references unknown tool_use_id",
			buildEvents: func(t *testing.T) []events.Envelope {
				return []events.Envelope{
					fxRunStarted(t, testRunID, 0, 10, "do the thing"),
					fxLLMResponded(t, testRunID, 1, 1, "tool_use",
						fxToolUseContent("", [3]string{"toolu_1", "clone_repo", `{}`}), false),
					fxToolInvoked(t, testRunID, 2, 1, "toolu_does_not_exist", "clone_repo", `{}`, false),
				}
			},
			wantErr: ErrUnknownToolUseID,
		},
		{
			name: "ToolResulted before its ToolInvoked",
			buildEvents: func(t *testing.T) []events.Envelope {
				return []events.Envelope{
					fxRunStarted(t, testRunID, 0, 10, "do the thing"),
					fxLLMResponded(t, testRunID, 1, 1, "tool_use",
						fxToolUseContent("", [3]string{"toolu_1", "clone_repo", `{}`}), false),
					fxToolResulted(t, testRunID, 2, 1, "toolu_1", "clone_repo", "cloned ok", "success"),
				}
			},
			wantErr: ErrToolOrder,
		},
		{
			name: "duplicate ToolInvoked for the same tool_use_id",
			buildEvents: func(t *testing.T) []events.Envelope {
				return []events.Envelope{
					fxRunStarted(t, testRunID, 0, 10, "do the thing"),
					fxLLMResponded(t, testRunID, 1, 1, "tool_use",
						fxToolUseContent("", [3]string{"toolu_1", "clone_repo", `{}`}), false),
					fxToolInvoked(t, testRunID, 2, 1, "toolu_1", "clone_repo", `{}`, false),
					fxToolInvoked(t, testRunID, 3, 1, "toolu_1", "clone_repo", `{}`, false),
				}
			},
			wantErr: ErrToolOrder,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Fold(tt.buildEvents(t))
			if err == nil {
				t.Fatal("Fold() error = nil, want non-nil")
			}
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("Fold() error = %v, want errors.Is(_, %v)", err, tt.wantErr)
			}
		})
	}
}

// TestFoldConflictingEvent covers the eighth FR-16 violation separately: a
// repeated sequence_number with a different event_id is ErrConflictingEvent,
// while the same sequence_number and event_id (a redelivery) is skipped
// silently and produces an identical state (EC-2).
func TestFoldConflictingEvent(t *testing.T) {
	start := fxRunStarted(t, testRunID, 0, 10, "do the thing")
	resp := fxLLMResponded(t, testRunID, 1, 1, "end_turn", fxTextContent("hi"), true)

	s, err := Fold([]events.Envelope{start, resp})
	if err != nil {
		t.Fatalf("Fold() error = %v", err)
	}

	t.Run("redelivery is skipped silently and state is unchanged", func(t *testing.T) {
		redelivered, err := Apply(s, resp)
		if err != nil {
			t.Fatalf("Apply(redelivery) error = %v", err)
		}

		wantDigest, err := s.Digest()
		if err != nil {
			t.Fatalf("Digest() error = %v", err)
		}
		gotDigest, err := redelivered.Digest()
		if err != nil {
			t.Fatalf("Digest() error = %v", err)
		}
		if gotDigest != wantDigest {
			t.Fatalf("Digest() after redelivery = %s, want %s (unchanged)", gotDigest, wantDigest)
		}
	})

	t.Run("same sequence_number, different event_id is a conflict", func(t *testing.T) {
		conflicting := fxRunCompleted(t, testRunID, 1, "different content entirely", 1)
		_, err := Apply(s, conflicting)
		if !errors.Is(err, ErrConflictingEvent) {
			t.Fatalf("Apply(conflicting) error = %v, want errors.Is(_, ErrConflictingEvent)", err)
		}
	})
}
