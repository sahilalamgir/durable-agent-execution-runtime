package replay

import (
	"testing"

	"github.com/sahilalamgir/durable-agent-execution-runtime/internal/events"
)

// TestFoldUpTo checks scripts/replay's --up-to-seq mechanism: folding only
// a prefix of a run's events reproduces exactly the state Fold would have
// produced if the source had only ever returned that prefix.
func TestFoldUpTo(t *testing.T) {
	all := buildFoldUpToFixture(t)

	got, err := FoldUpTo(all, 1)
	if err != nil {
		t.Fatalf("FoldUpTo() error = %v", err)
	}
	want, err := Fold(all[:2])
	if err != nil {
		t.Fatalf("Fold(prefix) error = %v", err)
	}

	gotDigest, err := got.Digest()
	if err != nil {
		t.Fatalf("Digest() error = %v", err)
	}
	wantDigest, err := want.Digest()
	if err != nil {
		t.Fatalf("Digest() error = %v", err)
	}
	if gotDigest != wantDigest {
		t.Fatalf("FoldUpTo(all, 1).Digest() = %s, want %s (Fold(all[:2]).Digest())", gotDigest, wantDigest)
	}
	if got.NextSequence != 2 {
		t.Errorf("NextSequence = %d, want 2", got.NextSequence)
	}
}

func buildFoldUpToFixture(t *testing.T) []events.Envelope {
	t.Helper()
	return []events.Envelope{
		fxRunStarted(t, testRunID, 0, 10, "do the thing"),
		fxLLMResponded(t, testRunID, 1, 1, "tool_use",
			fxToolUseContent("", [3]string{"toolu_1", "clone_repo", `{"repo_url":"https://example.com/widgets.git"}`}), false),
		fxToolInvoked(t, testRunID, 2, 1, "toolu_1", "clone_repo", `{"repo_url":"https://example.com/widgets.git"}`, false),
		fxToolResulted(t, testRunID, 3, 1, "toolu_1", "clone_repo", "cloned ok", "success"),
	}
}
