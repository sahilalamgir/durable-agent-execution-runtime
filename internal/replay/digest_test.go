package replay

import (
	"encoding/json"
	"testing"

	"github.com/sahilalamgir/durable-agent-execution-runtime/internal/events"
)

// TestApplyDoesNotAliasInput is EC-17's test: applying an event to state S
// must not change anything S's caller can observe through S afterward —
// neither S.Messages() nor S.Digest().
func TestApplyDoesNotAliasInput(t *testing.T) {
	start := fxRunStarted(t, testRunID, 0, 10, "do the thing")
	s, err := Fold([]events.Envelope{start})
	if err != nil {
		t.Fatalf("Fold() error = %v", err)
	}

	beforeDigest, err := s.Digest()
	if err != nil {
		t.Fatalf("Digest() error = %v", err)
	}
	beforeMsgs, err := s.Messages()
	if err != nil {
		t.Fatalf("Messages() error = %v", err)
	}

	resp := fxLLMResponded(t, testRunID, 1, 1, "tool_use",
		fxToolUseContent("", [3]string{"toolu_1", "clone_repo", `{"repo_url":"https://example.com/widgets.git"}`}), false)
	if _, err := Apply(s, resp); err != nil {
		t.Fatalf("Apply() error = %v", err)
	}

	afterDigest, err := s.Digest()
	if err != nil {
		t.Fatalf("Digest() error = %v", err)
	}
	afterMsgs, err := s.Messages()
	if err != nil {
		t.Fatalf("Messages() error = %v", err)
	}

	if afterDigest != beforeDigest {
		t.Fatalf("s.Digest() changed after Apply(s, ...) was called: before=%s after=%s (Apply mutated its input)", beforeDigest, afterDigest)
	}
	if len(afterMsgs) != len(beforeMsgs) {
		t.Fatalf("s.Messages() changed after Apply(s, ...) was called: before had %d message(s), after has %d (Apply mutated its input)", len(beforeMsgs), len(afterMsgs))
	}
}

// TestDigestStableAcrossKeyReorder is AC-7's other half: Digest() is the
// same for a state folded from original envelopes and one folded from
// envelopes whose payloads were re-marshaled through map[string]any (key
// order changed) — simulating a state folded from Kafka vs. from Postgres's
// key-reordering JSONB (EC-12). This specifically includes a mid-batch case
// (PendingToolCalls non-empty) and an in-flight case (InFlightTool
// non-nil), since those are exactly the states whose ToolCall.ToolArgs (and
// whose Messages()-embedded tool_use blocks) carry raw, reorderable JSON.
func TestDigestStableAcrossKeyReorder(t *testing.T) {
	tests := []struct {
		name        string
		buildEvents func(t *testing.T) []events.Envelope
	}{
		{
			name: "fully resolved run, no tool calls",
			buildEvents: func(t *testing.T) []events.Envelope {
				return []events.Envelope{
					fxRunStarted(t, testRunID, 0, 10, "do the thing"),
					fxLLMResponded(t, testRunID, 1, 1, "end_turn", fxTextContent("all done"), true),
					fxRunCompleted(t, testRunID, 2, "all done", 1),
				}
			},
		},
		{
			name: "in-flight tool call with multi-key args",
			buildEvents: func(t *testing.T) []events.Envelope {
				return []events.Envelope{
					fxRunStarted(t, testRunID, 0, 10, "do the thing"),
					fxLLMResponded(t, testRunID, 1, 1, "tool_use",
						fxToolUseContent("", [3]string{"toolu_1", "open_pr", `{"title":"Fix widget parsing","body":"Fixes failing tests."}`}),
						false),
					fxToolInvoked(t, testRunID, 2, 1, "toolu_1", "open_pr", `{"title":"Fix widget parsing","body":"Fixes failing tests."}`, true),
				}
			},
		},
		{
			name: "mid-batch: one tool resolved, one pending, both multi-key args",
			buildEvents: func(t *testing.T) []events.Envelope {
				return []events.Envelope{
					fxRunStarted(t, testRunID, 0, 10, "do the thing"),
					fxLLMResponded(t, testRunID, 1, 1, "tool_use",
						fxToolUseContent("",
							[3]string{"toolu_1", "clone_repo", `{"repo_url":"https://example.com/widgets.git","branch":"main"}`},
							[3]string{"toolu_2", "open_pr", `{"title":"Fix widget parsing","body":"Fixes failing tests."}`},
						),
						false),
					fxToolInvoked(t, testRunID, 2, 1, "toolu_1", "clone_repo", `{"repo_url":"https://example.com/widgets.git","branch":"main"}`, false),
					fxToolResulted(t, testRunID, 3, 1, "toolu_1", "clone_repo", "cloned ok", "success"),
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			original := tt.buildEvents(t)
			reordered := reorderPayloadKeys(t, original)

			sOriginal, err := Fold(original)
			if err != nil {
				t.Fatalf("Fold(original) error = %v", err)
			}
			sReordered, err := Fold(reordered)
			if err != nil {
				t.Fatalf("Fold(reordered) error = %v", err)
			}

			// Sanity check this test actually exercises a pending/in-flight
			// tool call where relevant, so it can't pass vacuously.
			if len(sOriginal.PendingToolCalls) == 0 && sOriginal.InFlightTool == nil && tt.name != "fully resolved run, no tool calls" {
				t.Fatalf("test setup: expected a pending or in-flight tool call, found neither")
			}

			digestOriginal, err := sOriginal.Digest()
			if err != nil {
				t.Fatalf("Digest(original) error = %v", err)
			}
			digestReordered, err := sReordered.Digest()
			if err != nil {
				t.Fatalf("Digest(reordered) error = %v", err)
			}

			if digestOriginal != digestReordered {
				t.Fatalf("Digest() differs between original and key-reordered payloads: original=%s reordered=%s", digestOriginal, digestReordered)
			}
		})
	}
}

// reorderPayloadKeys rebuilds each envelope in evs with its payload
// re-marshaled through map[string]any, which changes JSON object key order
// (encoding/json sorts map keys alphabetically) without changing meaning —
// simulating what a round trip through Postgres's JSONB column does.
func reorderPayloadKeys(t *testing.T, evs []events.Envelope) []events.Envelope {
	t.Helper()
	out := make([]events.Envelope, len(evs))
	for i, e := range evs {
		var generic map[string]any
		if err := json.Unmarshal(e.Payload, &generic); err != nil {
			t.Fatalf("unmarshaling payload for reordering: %v", err)
		}
		reordered, err := json.Marshal(generic)
		if err != nil {
			t.Fatalf("re-marshaling reordered payload: %v", err)
		}
		e.Payload = reordered
		out[i] = e
	}
	return out
}
