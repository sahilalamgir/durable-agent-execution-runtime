package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"testing"

	"github.com/anthropics/anthropic-sdk-go"

	"github.com/sahilalamgir/durable-agent-execution-runtime/internal/events"
	"github.com/sahilalamgir/durable-agent-execution-runtime/internal/replay"
)

// stubLLMClient replays a scripted list of responses, one per
// CreateMessage call. It records how many times it was called so tests can
// assert the loop never calls the LLM more than expected. onCall, if set,
// is invoked synchronously with each call's 1-based call number and
// params, before the scripted response is returned — tests use it to
// snapshot state (e.g. a fakePublisher's history) as of right before that
// call, which only makes sense captured at that exact moment.
type stubLLMClient struct {
	responses []*anthropic.Message
	calls     int
	onCall    func(callNum int, params anthropic.MessageNewParams)
}

func (s *stubLLMClient) CreateMessage(ctx context.Context, params anthropic.MessageNewParams) (*anthropic.Message, error) {
	if s.calls >= len(s.responses) {
		return nil, fmt.Errorf("stubLLMClient: no more scripted responses (called %d times)", s.calls+1)
	}
	resp := s.responses[s.calls]
	s.calls++
	if s.onCall != nil {
		s.onCall(s.calls, params)
	}
	return resp, nil
}

// mustMessage builds an *anthropic.Message fixture from raw JSON shaped
// like a real Messages API response. anthropic.Message's AsAny()/union
// helpers read from an unexported field populated only by UnmarshalJSON, so
// hand-built struct literals will not work with the type switches in
// loop.go — every fixture must go through json.Unmarshal.
func mustMessage(t *testing.T, rawJSON string) *anthropic.Message {
	t.Helper()
	var msg anthropic.Message
	if err := json.Unmarshal([]byte(rawJSON), &msg); err != nil {
		t.Fatalf("unmarshaling fixture message: %v", err)
	}
	return &msg
}

func toolUseFixture(msgID, toolUseID, toolName, inputJSON string) string {
	return fmt.Sprintf(`{"id":%q,"type":"message","role":"assistant","model":"claude-sonnet-5",
"content":[{"type":"tool_use","id":%q,"name":%q,"input":%s}],
"stop_reason":"tool_use","stop_sequence":null,"usage":{"input_tokens":10,"output_tokens":5}}`,
		msgID, toolUseID, toolName, inputJSON)
}

func terminalFixture(msgID, text string) string {
	return fmt.Sprintf(`{"id":%q,"type":"message","role":"assistant","model":"claude-sonnet-5",
"content":[{"type":"text","text":%q}],
"stop_reason":"end_turn","stop_sequence":null,"usage":{"input_tokens":8,"output_tokens":4}}`,
		msgID, text)
}

func maxTokensFixture(msgID, text string) string {
	return fmt.Sprintf(`{"id":%q,"type":"message","role":"assistant","model":"claude-sonnet-5",
"content":[{"type":"text","text":%q}],
"stop_reason":"max_tokens","stop_sequence":null,"usage":{"input_tokens":8,"output_tokens":4}}`,
		msgID, text)
}

const testModel = anthropic.ModelClaudeSonnet5

// newTestJournal builds a JournalConfig backed by a fresh fakePublisher, so
// each test gets its own isolated publish history.
func newTestJournal(runID string) (JournalConfig, *fakePublisher) {
	pub := &fakePublisher{}
	return NewJournalConfig(pub, runID, "local-dev", "repo-maintenance-agent"), pub
}

// mustMessagesJSON canonicalizes msgs as JSON for byte-for-byte comparison
// (AC-4): anthropic.MessageParam's fields have a fixed declaration order,
// so two semantically-identical slices marshal identically.
func mustMessagesJSON(t *testing.T, msgs []anthropic.MessageParam) string {
	t.Helper()
	b, err := json.Marshal(msgs)
	if err != nil {
		t.Fatalf("marshaling messages: %v", err)
	}
	return string(b)
}

func TestLoopRun(t *testing.T) {
	tests := []struct {
		name              string
		maxSteps          int
		fixtures          []string
		wantTerminal      bool
		wantMaxStepsExc   bool
		wantSteps         int
		wantStubCallCount int
		// wantEventTypes, when non-nil, is the exact expected published
		// event type sequence (AC-5's canonical-sequence check). When nil,
		// only the generic gapless-sequence-numbers and
		// ends-with-the-right-terminal-event-type checks below apply.
		wantEventTypes []events.EventType
	}{
		{
			name:     "terminal on first response",
			maxSteps: 10,
			fixtures: []string{
				terminalFixture("msg_1", "Nothing to fix; tests already pass."),
			},
			wantTerminal:      true,
			wantSteps:         1,
			wantStubCallCount: 1,
			wantEventTypes: []events.EventType{
				events.EventRunStarted,
				events.EventLLMResponded,
				events.EventRunCompleted,
			},
		},
		{
			name:     "multi-step ends terminal",
			maxSteps: 10,
			fixtures: []string{
				toolUseFixture("msg_1", "toolu_1", "clone_repo", `{"repo_url":"https://github.com/example/widgets.git"}`),
				toolUseFixture("msg_2", "toolu_2", "run_tests", `{}`),
				toolUseFixture("msg_3", "toolu_3", "apply_fix", `{"description":"fix widget parsing"}`),
				toolUseFixture("msg_4", "toolu_4", "run_tests", `{}`),
				toolUseFixture("msg_5", "toolu_5", "open_pr", `{"title":"Fix widget parsing","body":"Fixes failing tests."}`),
				terminalFixture("msg_6", "Done, PR #42 opened."),
			},
			wantTerminal:      true,
			wantSteps:         6,
			wantStubCallCount: 6,
			// AC-5: the canonical 18-event sequence from
			// phase2-event-journal.md's API Contracts section.
			wantEventTypes: []events.EventType{
				events.EventRunStarted,
				events.EventLLMResponded, events.EventToolInvoked, events.EventToolResulted,
				events.EventLLMResponded, events.EventToolInvoked, events.EventToolResulted,
				events.EventLLMResponded, events.EventToolInvoked, events.EventToolResulted,
				events.EventLLMResponded, events.EventToolInvoked, events.EventToolResulted,
				events.EventLLMResponded, events.EventToolInvoked, events.EventToolResulted,
				events.EventLLMResponded,
				events.EventRunCompleted,
			},
		},
		{
			name:     "max_steps exceeded",
			maxSteps: 3,
			fixtures: []string{
				toolUseFixture("msg_1", "toolu_1", "clone_repo", `{"repo_url":"https://github.com/example/widgets.git"}`),
				toolUseFixture("msg_2", "toolu_2", "clone_repo", `{"repo_url":"https://github.com/example/widgets.git"}`),
				toolUseFixture("msg_3", "toolu_3", "clone_repo", `{"repo_url":"https://github.com/example/widgets.git"}`),
			},
			wantMaxStepsExc:   true,
			wantSteps:         3,
			wantStubCallCount: 3,
		},
		{
			name:     "unknown tool name",
			maxSteps: 10,
			fixtures: []string{
				toolUseFixture("msg_1", "toolu_1", "delete_repo", `{}`),
				terminalFixture("msg_2", "Sorry, I made a mistake, done now."),
			},
			wantTerminal:      true,
			wantSteps:         2,
			wantStubCallCount: 2,
		},
		{
			name:     "malformed args",
			maxSteps: 10,
			fixtures: []string{
				toolUseFixture("msg_1", "toolu_1", "apply_fix", `{"description":123}`),
				terminalFixture("msg_2", "Retried, done."),
			},
			wantTerminal:      true,
			wantSteps:         2,
			wantStubCallCount: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var responses []*anthropic.Message
			for _, raw := range tt.fixtures {
				responses = append(responses, mustMessage(t, raw))
			}
			stub := &stubLLMClient{responses: responses}

			journal, pub := newTestJournal("run-" + tt.name)
			loop := newFenceFixture(t).newLoop(stub, journal)
			loop.maxSteps = tt.maxSteps

			outcome, err := loop.Run(context.Background(), Task{Prompt: "do the thing"})
			if err != nil {
				t.Fatalf("Run() error = %v", err)
			}
			if outcome.Terminal != tt.wantTerminal {
				t.Errorf("Terminal = %v, want %v", outcome.Terminal, tt.wantTerminal)
			}
			if outcome.MaxStepsExceeded != tt.wantMaxStepsExc {
				t.Errorf("MaxStepsExceeded = %v, want %v", outcome.MaxStepsExceeded, tt.wantMaxStepsExc)
			}
			if outcome.Steps != tt.wantSteps {
				t.Errorf("Steps = %d, want %d", outcome.Steps, tt.wantSteps)
			}
			if stub.calls != tt.wantStubCallCount {
				t.Errorf("stub called %d times, want %d", stub.calls, tt.wantStubCallCount)
			}

			// AC-5 (generic part): every case's published sequence_numbers
			// are gapless from 0, and it ends with the terminal event
			// FR-10 maps its outcome to.
			published := pub.snapshot()
			for i, e := range published {
				if e.SequenceNumber != int64(i) {
					t.Errorf("published[%d].SequenceNumber = %d, want %d (gapless)", i, e.SequenceNumber, i)
				}
			}
			if len(published) == 0 {
				t.Fatal("no events were published")
			}
			wantTerminalType := events.EventRunCompleted
			if tt.wantMaxStepsExc {
				wantTerminalType = events.EventRunFailed
			}
			if got := published[len(published)-1].EventType; got != wantTerminalType {
				t.Errorf("last published event type = %s, want %s", got, wantTerminalType)
			}

			// AC-5 (exact part, multi-step case only).
			if tt.wantEventTypes != nil {
				if len(published) != len(tt.wantEventTypes) {
					t.Fatalf("published %d event(s), want %d", len(published), len(tt.wantEventTypes))
				}
				for i, want := range tt.wantEventTypes {
					if got := published[i].EventType; got != want {
						t.Errorf("published[%d].EventType = %s, want %s", i, got, want)
					}
				}
			}
		})
	}
}

func TestLoopRunTruncated(t *testing.T) {
	stub := &stubLLMClient{responses: []*anthropic.Message{
		mustMessage(t, maxTokensFixture("msg_1", "cut off mid-sente")),
	}}
	journal, pub := newTestJournal("run-truncated")
	loop := newFenceFixture(t).newLoop(stub, journal)

	outcome, err := loop.Run(context.Background(), Task{Prompt: "do the thing"})
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	// D-3: a truncated response is a failure, not a clean completion, so
	// Terminal (this phase's narrower "reached RunCompleted" meaning) is
	// false here even though the LLM did stop calling tools.
	if outcome.Terminal {
		t.Errorf("Terminal = %v, want false (truncation is RunFailed, not RunCompleted)", outcome.Terminal)
	}
	if !outcome.Truncated {
		t.Errorf("Truncated = %v, want true", outcome.Truncated)
	}
	if !outcome.Failed {
		t.Errorf("Failed = %v, want true", outcome.Failed)
	}

	published := pub.snapshot()
	last := published[len(published)-1]
	if last.EventType != events.EventRunFailed {
		t.Fatalf("last published event type = %s, want %s (D-3: truncation is a RunFailed, not a clean completion)", last.EventType, events.EventRunFailed)
	}
	payload, err := events.DecodePayload(last)
	if err != nil {
		t.Fatalf("DecodePayload() error = %v", err)
	}
	failed, ok := payload.(events.RunFailedPayload)
	if !ok {
		t.Fatalf("payload type = %T, want RunFailedPayload", payload)
	}
	if failed.ErrorClass != "llm_output_truncated" {
		t.Errorf("ErrorClass = %q, want %q", failed.ErrorClass, "llm_output_truncated")
	}
}

func TestNewLoopPanicsOnInvalidMaxSteps(t *testing.T) {
	for _, maxSteps := range []int{0, -1} {
		t.Run(fmt.Sprintf("maxSteps=%d", maxSteps), func(t *testing.T) {
			defer func() {
				if r := recover(); r == nil {
					t.Fatalf("NewLoop(maxSteps=%d) did not panic, want panic", maxSteps)
				}
			}()
			journal, _ := newTestJournal("run-invalid-maxsteps")
			NewLoop(&stubLLMClient{}, newFenceFixture(t).registry(), testModel, 1024, maxSteps, io.Discard, journal, nil)
		})
	}
}

func TestLoopRunLLMError(t *testing.T) {
	stub := &stubLLMClient{} // no scripted responses: first call fails.
	journal, pub := newTestJournal("run-llm-error")
	loop := newFenceFixture(t).newLoop(stub, journal)

	_, err := loop.Run(context.Background(), Task{Prompt: "do the thing"})
	if err == nil {
		t.Fatal("Run() error = nil, want non-nil")
	}

	// The failure is still journaled (FR-10), even though it also
	// surfaces as a Go error (Loop's preserved "genuine LLM errors return
	// error" contract).
	published := pub.snapshot()
	last := published[len(published)-1]
	if last.EventType != events.EventRunFailed {
		t.Fatalf("last published event type = %s, want %s", last.EventType, events.EventRunFailed)
	}
}

// TestLiveReplayEquivalence is AC-4: for every call k the stub LLM
// receives, params.Messages must equal
// replay.Fold(everything published strictly before call k).Messages(),
// compared as canonical JSON. This is D-1's central guarantee: live
// execution and replay share one code path by construction.
func TestLiveReplayEquivalence(t *testing.T) {
	fixtures := []string{
		toolUseFixture("msg_1", "toolu_1", "clone_repo", `{"repo_url":"https://github.com/example/widgets.git"}`),
		toolUseFixture("msg_2", "toolu_2", "run_tests", `{}`),
		toolUseFixture("msg_3", "toolu_3", "apply_fix", `{"description":"fix widget parsing"}`),
		toolUseFixture("msg_4", "toolu_4", "run_tests", `{}`),
		toolUseFixture("msg_5", "toolu_5", "open_pr", `{"title":"Fix widget parsing","body":"Fixes failing tests."}`),
		terminalFixture("msg_6", "Done, PR #42 opened."),
	}
	var responses []*anthropic.Message
	for _, raw := range fixtures {
		responses = append(responses, mustMessage(t, raw))
	}

	journal, pub := newTestJournal("run-ac4")

	type observedCall struct {
		callNum int
		params  anthropic.MessageNewParams
		before  []events.Envelope
	}
	var seen []observedCall

	stub := &stubLLMClient{
		responses: responses,
		onCall: func(callNum int, params anthropic.MessageNewParams) {
			seen = append(seen, observedCall{callNum: callNum, params: params, before: pub.snapshot()})
		},
	}

	loop := newFenceFixture(t).newLoop(stub, journal)
	if _, err := loop.Run(context.Background(), Task{Prompt: "do the thing"}); err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	if len(seen) != len(responses) {
		t.Fatalf("stub observed %d call(s), want %d", len(seen), len(responses))
	}

	for _, obs := range seen {
		state, err := replay.Fold(obs.before)
		if err != nil {
			t.Fatalf("call %d: Fold(events published so far) error = %v", obs.callNum, err)
		}
		wantMsgs, err := state.Messages()
		if err != nil {
			t.Fatalf("call %d: Messages() error = %v", obs.callNum, err)
		}

		want := mustMessagesJSON(t, wantMsgs)
		got := mustMessagesJSON(t, obs.params.Messages)
		if got != want {
			t.Fatalf("call %d: params.Messages =\n%s\nwant:\n%s", obs.callNum, got, want)
		}
	}
}

// TestWriteAhead is AC-6: no event with a real-world consequence is ever
// acted on before it is durably published.
func TestWriteAhead(t *testing.T) {
	t.Run("Publish fails every ToolInvoked: the tool never executes", func(t *testing.T) {
		pub := &fakePublisher{failEventType: events.EventToolInvoked}
		journal := NewJournalConfig(pub, "run-fail-toolinvoked", "local-dev", "repo-maintenance-agent")

		fx := newFenceFixture(t)

		stub := &stubLLMClient{responses: []*anthropic.Message{
			mustMessage(t, toolUseFixture("msg_1", "toolu_1", "clone_repo", `{"repo_url":"https://github.com/example/widgets.git"}`)),
		}}
		loop := fx.newLoop(stub, journal)

		_, err := loop.Run(context.Background(), Task{Prompt: "do the thing"})
		if !errors.Is(err, ErrPublishFailed) {
			t.Fatalf("Run() error = %v, want errors.Is(_, ErrPublishFailed)", err)
		}
		if fx.execs["clone_repo"] != 0 {
			t.Fatalf("clone_repo Execute was called %d time(s), want 0", fx.execs["clone_repo"])
		}
		for _, e := range pub.snapshot() {
			if e.EventType == events.EventToolResulted {
				t.Fatalf("a ToolResulted was published even though every ToolInvoked publish failed")
			}
		}
	})

	t.Run("Publish fails every LLMResponded: no tool runs", func(t *testing.T) {
		pub := &fakePublisher{failEventType: events.EventLLMResponded}
		journal := NewJournalConfig(pub, "run-fail-llmresponded", "local-dev", "repo-maintenance-agent")

		fx := newFenceFixture(t)

		stub := &stubLLMClient{responses: []*anthropic.Message{
			mustMessage(t, toolUseFixture("msg_1", "toolu_1", "clone_repo", `{"repo_url":"https://github.com/example/widgets.git"}`)),
		}}
		loop := fx.newLoop(stub, journal)

		_, err := loop.Run(context.Background(), Task{Prompt: "do the thing"})
		if !errors.Is(err, ErrPublishFailed) {
			t.Fatalf("Run() error = %v, want errors.Is(_, ErrPublishFailed)", err)
		}
		if fx.execs["clone_repo"] != 0 {
			t.Fatalf("clone_repo Execute was called %d time(s), want 0", fx.execs["clone_repo"])
		}
		for _, e := range pub.snapshot() {
			if e.EventType == events.EventToolInvoked {
				t.Fatalf("a ToolInvoked was published even though LLMResponded never got acknowledged")
			}
		}
	})

	t.Run("Publish fails once then succeeds: the retry uses the identical envelope", func(t *testing.T) {
		pub := &fakePublisher{failCall: 1} // fails only the very first Publish attempt (RunStarted).
		journal := NewJournalConfig(pub, "run-retry-then-succeed", "local-dev", "repo-maintenance-agent")

		stub := &stubLLMClient{responses: []*anthropic.Message{
			mustMessage(t, terminalFixture("msg_1", "done")),
		}}
		loop := newFenceFixture(t).newLoop(stub, journal)

		if _, err := loop.Run(context.Background(), Task{Prompt: "do the thing"}); err != nil {
			t.Fatalf("Run() error = %v", err)
		}

		published := pub.snapshot()
		if len(published) < 2 {
			t.Fatalf("published %d event(s), want at least 2 (the failed attempt and its retry)", len(published))
		}
		first, retry := published[0], published[1]
		if first.EventID != retry.EventID {
			t.Errorf("retry EventID = %q, want %q (identical to the failed attempt)", retry.EventID, first.EventID)
		}
		if first.SequenceNumber != retry.SequenceNumber {
			t.Errorf("retry SequenceNumber = %d, want %d (identical to the failed attempt)", retry.SequenceNumber, first.SequenceNumber)
		}
		if first.EventType != events.EventRunStarted || retry.EventType != events.EventRunStarted {
			t.Errorf("expected both attempts to be RunStarted, got %s and %s", first.EventType, retry.EventType)
		}
	})
}
