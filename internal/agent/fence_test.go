package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"testing"
	"time"

	"github.com/anthropics/anthropic-sdk-go"

	"github.com/sahilalamgir/durable-agent-execution-runtime/internal/events"
	"github.com/sahilalamgir/durable-agent-execution-runtime/internal/idempotency"
	"github.com/sahilalamgir/durable-agent-execution-runtime/internal/replay"
	"github.com/sahilalamgir/durable-agent-execution-runtime/internal/tools"
)

const resumeRunID = "7f1c4b8e-2d3a-4f6b-9c1e-0a2b3c4d5e6f"

func llmResponses(t *testing.T, fixtures ...string) []*anthropic.Message {
	t.Helper()
	var out []*anthropic.Message
	for _, f := range fixtures {
		out = append(out, mustMessage(t, f))
	}
	return out
}

// AC-4: the fence order, asserted across the publisher, store and tool.
func TestFencedToolOrdering(t *testing.T) {
	tests := []struct {
		name     string
		fixture  string
		wantCall []string
	}{
		{
			name:    "side-effecting tool: publish, claim, execute, resolve, publish",
			fixture: toolUseFixture("m1", "toolu_1", "apply_fix", `{"description":"d"}`),
			wantCall: []string{
				"Publish(RunStarted)", "Publish(LLMResponded)",
				"Publish(ToolInvoked)", "Claim", "Execute(apply_fix)", "Resolve", "Publish(ToolResulted)",
				"Publish(LLMResponded)", "Publish(RunCompleted)",
			},
		},
		{
			name:    "read-only tool never touches the store",
			fixture: toolUseFixture("m1", "toolu_1", "clone_repo", `{"repo_url":"u"}`),
			wantCall: []string{
				"Publish(RunStarted)", "Publish(LLMResponded)",
				"Publish(ToolInvoked)", "Execute(clone_repo)", "Publish(ToolResulted)",
				"Publish(LLMResponded)", "Publish(RunCompleted)",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fx := newFenceFixture(t)
			journal, pub := newTestJournal(resumeRunID)
			pub.rec = fx.rec
			stub := &stubLLMClient{responses: llmResponses(t, tt.fixture, terminalFixture("m2", "done"))}

			if _, err := fx.newLoop(stub, journal).Run(context.Background(), Task{Prompt: "go"}); err != nil {
				t.Fatalf("Run() error = %v", err)
			}
			if got := fx.rec.snapshot(); !reflect.DeepEqual(got, tt.wantCall) {
				t.Fatalf("call order =\n%v\nwant\n%v", got, tt.wantCall)
			}
		})
	}

	t.Run("a ToolInvoked that never gets acked means no claim and no execute", func(t *testing.T) {
		fx := newFenceFixture(t)
		pub := &fakePublisher{failEventType: events.EventToolInvoked, rec: fx.rec}
		journal := NewJournalConfig(pub, resumeRunID, "local-dev", "repo-maintenance-agent")
		stub := &stubLLMClient{responses: llmResponses(t, toolUseFixture("m1", "toolu_1", "apply_fix", `{"description":"d"}`))}

		_, err := fx.newLoop(stub, journal).Run(context.Background(), Task{Prompt: "go"})

		if !errors.Is(err, ErrPublishFailed) {
			t.Fatalf("Run() error = %v, want ErrPublishFailed", err)
		}
		if fx.store.ops != 0 || fx.execs["apply_fix"] != 0 {
			t.Fatalf("store ops = %d, Execute = %d; want 0 and 0", fx.store.ops, fx.execs["apply_fix"])
		}
	})
}

// AC-5: keys and resolutions in the journal.
func TestJournaledKeysAndResolution(t *testing.T) {
	fx := newFenceFixture(t)
	journal, pub := newTestJournal(resumeRunID)
	stub := &stubLLMClient{responses: llmResponses(t,
		toolUseFixture("m1", "toolu_1", "clone_repo", `{"repo_url":"u"}`),
		toolUseFixture("m2", "toolu_2", "apply_fix", `{"description":"d"}`),
		terminalFixture("m3", "done"),
	)}
	if _, err := fx.newLoop(stub, journal).Run(context.Background(), Task{Prompt: "go"}); err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	for _, env := range pub.snapshot() {
		payload, err := events.DecodePayload(env)
		if err != nil {
			t.Fatal(err)
		}
		switch p := payload.(type) {
		case events.ToolInvokedPayload:
			if p.HasSideEffect {
				want := idempotency.Key(resumeRunID, p.Step, p.ToolUseID)
				if p.IdempotencyKey == nil || *p.IdempotencyKey != want {
					t.Errorf("%s: payload key = %v, want %q", p.ToolName, p.IdempotencyKey, want)
				}
				if env.IdempotencyKey == nil || *env.IdempotencyKey != want {
					t.Errorf("%s: envelope key = %v, want %q", p.ToolName, env.IdempotencyKey, want)
				}
			} else if p.IdempotencyKey != nil || env.IdempotencyKey != nil {
				t.Errorf("%s: read-only tool carries a key (payload %v, envelope %v)", p.ToolName, p.IdempotencyKey, env.IdempotencyKey)
			}
		case events.ToolResultedPayload:
			if p.Resolution != "executed" {
				t.Errorf("%s: resolution = %q, want executed", p.ToolName, p.Resolution)
			}
			if p.WasReplayedFromCache != (p.Resolution == "cached") {
				t.Errorf("%s: was_replayed_from_cache = %v inconsistent with resolution %q", p.ToolName, p.WasReplayedFromCache, p.Resolution)
			}
		}
	}
}

// TestTerminalResponseNeverRunsItsTools guards the truncation case: a
// max_tokens response can end in a cut-off tool_use block, and its args must
// not be executed.
func TestTerminalResponseNeverRunsItsTools(t *testing.T) {
	fx := newFenceFixture(t)
	journal, pub := newTestJournal(resumeRunID)
	truncated := `{"id":"m1","type":"message","role":"assistant","model":"claude-sonnet-5",
"content":[{"type":"tool_use","id":"toolu_1","name":"apply_fix","input":{"description":"cut"}}],
"stop_reason":"max_tokens","stop_sequence":null,"usage":{"input_tokens":8,"output_tokens":4}}`
	stub := &stubLLMClient{responses: llmResponses(t, truncated)}

	out, err := fx.newLoop(stub, journal).Run(context.Background(), Task{Prompt: "go"})

	if err != nil || !out.Truncated || !out.Failed {
		t.Fatalf("Run() = %+v, %v; want truncated failure", out, err)
	}
	if fx.execs["apply_fix"] != 0 || fx.store.ops != 0 {
		t.Fatalf("truncated tool ran: Execute = %d, store ops = %d", fx.execs["apply_fix"], fx.store.ops)
	}
	if types := eventTypes(pub.snapshot()); types[len(types)-1] != events.EventRunFailed {
		t.Fatalf("event types = %v, want to end in RunFailed", types)
	}
}

func eventTypes(envs []events.Envelope) []events.EventType {
	var out []events.EventType
	for _, e := range envs {
		out = append(out, e.EventType)
	}
	return out
}

// journalStage says how far a crashed run's journal got.
type journalStage int

const (
	stagePending  journalStage = iota // LLMResponded acked, ToolInvoked not
	stageInvoked                      // ToolInvoked acked, ToolResulted not
	stageResulted                     // ToolResulted acked
)

const (
	openPRArgs   = `{"title":"T","body":"B"}`
	applyFixArgs = `{"description":"d"}`
)

func argsFor(tool string) string {
	if tool == "apply_fix" {
		return applyFixArgs
	}
	return openPRArgs
}

// crashedJournal builds the events a crashed worker left in Kafka, ending at stage.
func crashedJournal(t *testing.T, tool string, stage journalStage, hasSideEffect bool, key *string) []events.Envelope {
	t.Helper()
	at := time.Now().Add(-time.Minute)
	args := json.RawMessage(argsFor(tool))
	content, err := json.Marshal([]map[string]any{{"type": "tool_use", "id": "toolu_1", "name": tool, "input": args}})
	if err != nil {
		t.Fatal(err)
	}
	evs := []events.Envelope{
		mkEvent(t, resumeRunID, 0, events.RunStartedPayload{
			WorkloadType: "repo-maintenance-agent", Input: json.RawMessage(`{"prompt":"go"}`), MaxSteps: 10,
		}, at),
		mkEvent(t, resumeRunID, 1, events.LLMRespondedPayload{Step: 1, StopReason: "tool_use", Content: content}, at),
	}
	if stage >= stageInvoked {
		evs = append(evs, mkEvent(t, resumeRunID, 2, events.ToolInvokedPayload{
			Step: 1, ToolUseID: "toolu_1", ToolName: tool, ToolArgs: args, IdempotencyKey: key, HasSideEffect: hasSideEffect,
		}, at))
	}
	if stage >= stageResulted {
		evs = append(evs, mkEvent(t, resumeRunID, 3, events.ToolResultedPayload{
			Step: 1, ToolUseID: "toolu_1", ToolName: tool, Result: "done", Status: "success", Resolution: "executed",
		}, at))
	}
	return evs
}

func (fx *fenceFixture) seedRecord(t *testing.T, tool, key string, state idempotency.RecordState, claimedAgo time.Duration) {
	t.Helper()
	hash, err := idempotency.ArgsHash(tool, json.RawMessage(argsFor(tool)))
	if err != nil {
		t.Fatal(err)
	}
	rec := idempotency.IdemRecord{
		State: state, RunID: resumeRunID, Step: 1, ToolUseID: "toolu_1", ToolName: tool, ArgsHash: hash,
		ClaimToken: "dead-worker-token", ClaimedBy: "crashed-1", ClaimedAt: time.Now().Add(-claimedAgo),
	}
	if state == idempotency.StateResolved {
		result, status, how := "opened PR #1: T", "success", "executed"
		rec.Result, rec.Status, rec.Resolution = &result, &status, &how
	}
	fx.store.data[key] = rec
}

func (fx *fenceFixture) seedLedger(t *testing.T, tool, key, result string) {
	t.Helper()
	err := fx.ledger.Record(context.Background(), tools.LedgerEntry{
		RunID: resumeRunID, ToolUseID: "toolu_1", ToolName: tool, IdempotencyKey: key, Result: result, RecordedAt: time.Now().UTC(),
	})
	if err != nil {
		t.Fatal(err)
	}
}

// AC-6: resume from every crash window in the phase 3 walk-through.
func TestResumeCrashWindows(t *testing.T) {
	key := idempotency.Key(resumeRunID, 1, "toolu_1")
	const stale = 10 * time.Minute // well past tool_timeout(5s) + 30s

	tests := []struct {
		name        string
		tool        string
		stage       journalStage
		seed        func(t *testing.T, fx *fenceFixture)
		wantTypes   []events.EventType
		wantExec    int
		wantRes     string // ToolResulted.resolution, "" if none published
		wantFail    string // RunFailed.error_class, "" if the run completes
		wantResult  string // ToolResulted.result, "" = don't check
		wantNoRedis bool
	}{
		{
			name: "W0: crashed before ToolInvoked was acked", tool: "open_pr", stage: stagePending,
			seed:      func(t *testing.T, fx *fenceFixture) {},
			wantTypes: []events.EventType{events.EventToolInvoked, events.EventToolResulted, events.EventLLMResponded, events.EventRunCompleted},
			wantExec:  1, wantRes: "executed",
		},
		{
			name: "W1: ToolInvoked acked, never claimed", tool: "open_pr", stage: stageInvoked,
			seed:      func(t *testing.T, fx *fenceFixture) {},
			wantTypes: []events.EventType{events.EventToolResulted, events.EventLLMResponded, events.EventRunCompleted},
			wantExec:  1, wantRes: "executed",
		},
		{
			name: "W2: claimed, effect never happened, tool can reconcile", tool: "open_pr", stage: stageInvoked,
			seed: func(t *testing.T, fx *fenceFixture) {
				fx.seedRecord(t, "open_pr", key, idempotency.StateClaimed, stale)
			},
			wantTypes: []events.EventType{events.EventToolResulted, events.EventLLMResponded, events.EventRunCompleted},
			wantExec:  1, wantRes: "reexecuted",
		},
		{
			name: "W2: claimed, tool cannot reconcile, fail without executing", tool: "apply_fix", stage: stageInvoked,
			seed: func(t *testing.T, fx *fenceFixture) {
				fx.seedRecord(t, "apply_fix", key, idempotency.StateClaimed, stale)
			},
			wantTypes: []events.EventType{events.EventRunFailed},
			wantExec:  0, wantFail: "unresolved_side_effect",
		},
		{
			name: "W3: claimed, effect happened, reconciler adopts the recorded result", tool: "open_pr", stage: stageInvoked,
			seed: func(t *testing.T, fx *fenceFixture) {
				fx.seedRecord(t, "open_pr", key, idempotency.StateClaimed, stale)
				fx.seedLedger(t, "open_pr", key, "opened PR #1: T")
			},
			wantTypes: []events.EventType{events.EventToolResulted, events.EventLLMResponded, events.EventRunCompleted},
			wantExec:  0, wantRes: "reconciled", wantResult: "opened PR #1: T",
		},
		{
			name: "W3: claimed, effect happened, no reconciler: fail, never re-run", tool: "apply_fix", stage: stageInvoked,
			seed: func(t *testing.T, fx *fenceFixture) {
				fx.seedRecord(t, "apply_fix", key, idempotency.StateClaimed, stale)
				fx.seedLedger(t, "apply_fix", key, "applied fix: d")
			},
			wantTypes: []events.EventType{events.EventRunFailed},
			wantExec:  0, wantFail: "unresolved_side_effect",
		},
		{
			name: "W4: resolved before ToolResulted, cache hit", tool: "open_pr", stage: stageInvoked,
			seed: func(t *testing.T, fx *fenceFixture) {
				fx.seedRecord(t, "open_pr", key, idempotency.StateResolved, time.Minute)
			},
			wantTypes: []events.EventType{events.EventToolResulted, events.EventLLMResponded, events.EventRunCompleted},
			wantExec:  0, wantRes: "cached", wantResult: "opened PR #1: T",
		},
		{
			name: "W5: ToolResulted acked, Redis is not consulted", tool: "open_pr", stage: stageResulted,
			seed:      func(t *testing.T, fx *fenceFixture) {},
			wantTypes: []events.EventType{events.EventLLMResponded, events.EventRunCompleted},
			wantExec:  0, wantNoRedis: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fx := newFenceFixture(t)
			tt.seed(t, fx)
			state, err := replay.Fold(crashedJournal(t, tt.tool, tt.stage, true, &key))
			if err != nil {
				t.Fatalf("Fold() error = %v", err)
			}
			journal, pub := newTestJournal(resumeRunID)
			stub := &stubLLMClient{responses: llmResponses(t, terminalFixture("m2", "all done"))}

			out, err := fx.newLoop(stub, journal).Resume(context.Background(), state)
			if err != nil {
				t.Fatalf("Resume() error = %v", err)
			}

			published := pub.snapshot()
			if got := eventTypes(published); !reflect.DeepEqual(got, tt.wantTypes) {
				t.Fatalf("published %v, want %v", got, tt.wantTypes)
			}
			if got := fx.execs[tt.tool]; got != tt.wantExec {
				t.Errorf("Execute(%s) count = %d, want %d", tt.tool, got, tt.wantExec)
			}
			if out.Failed != (tt.wantFail != "") {
				t.Errorf("Outcome.Failed = %v, want %v", out.Failed, tt.wantFail != "")
			}
			if tt.wantNoRedis && fx.store.ops != 0 {
				t.Errorf("store was consulted %d time(s) for a resulted tool", fx.store.ops)
			}
			if published[0].SequenceNumber != state.NextSequence {
				t.Errorf("first published seq = %d, want %d", published[0].SequenceNumber, state.NextSequence)
			}
			checkResumedPayloads(t, published, tt.wantRes, tt.wantResult, tt.wantFail)
			assertNoDuplicateEffects(t, fx)
		})
	}
}

func checkResumedPayloads(t *testing.T, published []events.Envelope, wantRes, wantResult, wantFail string) {
	t.Helper()
	for _, env := range published {
		payload, err := events.DecodePayload(env)
		if err != nil {
			t.Fatal(err)
		}
		switch p := payload.(type) {
		case events.ToolResultedPayload:
			if p.Resolution != wantRes {
				t.Errorf("resolution = %q, want %q", p.Resolution, wantRes)
			}
			if p.WasReplayedFromCache != (wantRes == "cached") {
				t.Errorf("was_replayed_from_cache = %v with resolution %q", p.WasReplayedFromCache, wantRes)
			}
			if wantResult != "" && p.Result != wantResult {
				t.Errorf("result = %q, want %q", p.Result, wantResult)
			}
		case events.RunFailedPayload:
			if p.ErrorClass != wantFail || p.Retryable {
				t.Errorf("RunFailed = %+v, want non-retryable %q", p, wantFail)
			}
		}
	}
}

// assertNoDuplicateEffects is the phase's core invariant: no idempotency key
// has two ledger entries.
func assertNoDuplicateEffects(t *testing.T, fx *fenceFixture) {
	t.Helper()
	entries, err := fx.ledger.Entries(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	seen := map[string]int{}
	for _, e := range entries {
		if seen[e.IdempotencyKey]++; seen[e.IdempotencyKey] > 1 {
			t.Errorf("duplicated side effect: key %q has %d ledger entries", e.IdempotencyKey, seen[e.IdempotencyKey])
		}
	}
}

// The journal wins over current code and registry (D-3, EC-12, EC-13).
func TestResumeTrustsTheJournal(t *testing.T) {
	computed := idempotency.Key(resumeRunID, 1, "toolu_1")
	custom := "idem:" + resumeRunID + ":1:toolu_1:from-an-older-build"

	tests := []struct {
		name        string
		tool        string
		hasSide     bool
		key         *string
		wantTypes   []events.EventType
		wantFail    string
		wantExec    int
		wantStoreOp bool
		wantKey     string // key that must exist in the store afterwards
	}{
		{
			name: "the journaled key is used, not one recomputed from current code", tool: "open_pr", hasSide: true, key: &custom,
			wantTypes: []events.EventType{events.EventToolResulted, events.EventLLMResponded, events.EventRunCompleted},
			wantExec:  1, wantStoreOp: true, wantKey: custom,
		},
		{
			name: "journaled has_side_effect=false re-executes without Redis even though the registry says otherwise", tool: "open_pr", hasSide: false, key: nil,
			wantTypes: []events.EventType{events.EventToolResulted, events.EventLLMResponded, events.EventRunCompleted},
			wantExec:  1,
		},
		{
			name: "side effect but no journaled key (Phase 2-era run): fail, never execute", tool: "open_pr", hasSide: true, key: nil,
			wantTypes: []events.EventType{events.EventRunFailed}, wantFail: "unresolved_side_effect",
		},
		{
			name: "journaled side effect but the tool is no longer registered: fail", tool: "delete_repo", hasSide: true, key: &computed,
			wantTypes: []events.EventType{events.EventRunFailed}, wantFail: "unresolved_side_effect",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fx := newFenceFixture(t)
			state, err := replay.Fold(crashedJournal(t, tt.tool, stageInvoked, tt.hasSide, tt.key))
			if err != nil {
				t.Fatalf("Fold() error = %v", err)
			}
			journal, pub := newTestJournal(resumeRunID)
			stub := &stubLLMClient{responses: llmResponses(t, terminalFixture("m2", "done"))}

			if _, err := fx.newLoop(stub, journal).Resume(context.Background(), state); err != nil {
				t.Fatalf("Resume() error = %v", err)
			}

			if got := eventTypes(pub.snapshot()); !reflect.DeepEqual(got, tt.wantTypes) {
				t.Fatalf("published %v, want %v", got, tt.wantTypes)
			}
			if got := fx.execs[tt.tool]; got != tt.wantExec {
				t.Errorf("Execute(%s) = %d, want %d", tt.tool, got, tt.wantExec)
			}
			if (fx.store.ops > 0) != tt.wantStoreOp {
				t.Errorf("store ops = %d, want consulted=%v", fx.store.ops, tt.wantStoreOp)
			}
			if tt.wantKey != "" {
				if _, ok := fx.store.data[tt.wantKey]; !ok {
					t.Errorf("no record under the journaled key %q; keys: %v", tt.wantKey, keysOf(fx.store.data))
				}
				if _, ok := fx.store.data[computed]; ok {
					t.Errorf("a record was written under the recomputed key %q", computed)
				}
			}
		})
	}
}

func keysOf(m map[string]idempotency.IdemRecord) []string {
	var out []string
	for k := range m {
		out = append(out, k)
	}
	return out
}

func TestResumeFinishRun(t *testing.T) {
	at := time.Now().Add(-time.Minute)
	state, err := replay.Fold([]events.Envelope{
		mkEvent(t, resumeRunID, 0, events.RunStartedPayload{
			WorkloadType: "repo-maintenance-agent", Input: json.RawMessage(`{"prompt":"go"}`), MaxSteps: 10,
		}, at),
		mkEvent(t, resumeRunID, 1, events.LLMRespondedPayload{
			Step: 1, StopReason: "end_turn", Content: json.RawMessage(`[{"type":"text","text":"all done"}]`), IsTerminal: true,
		}, at),
	})
	if err != nil || state.NextAction != replay.ActionFinishRun {
		t.Fatalf("Fold() = %v, next_action %v", err, state.NextAction)
	}
	fx := newFenceFixture(t)
	journal, pub := newTestJournal(resumeRunID)
	stub := &stubLLMClient{}

	out, err := fx.newLoop(stub, journal).Resume(context.Background(), state)

	if err != nil || !out.Terminal || out.FinalText != "all done" {
		t.Fatalf("Resume() = %+v, %v", out, err)
	}
	if got := eventTypes(pub.snapshot()); !reflect.DeepEqual(got, []events.EventType{events.EventRunCompleted}) {
		t.Fatalf("published %v, want [RunCompleted]", got)
	}
	if stub.calls != 0 {
		t.Fatalf("LLM called %d time(s) while finishing a finished run", stub.calls)
	}
}

type stubFence struct{ err error }

func (s stubFence) Run(context.Context, tools.Invocation, replay.ToolCall, tools.Tool) (idempotency.Outcome, error) {
	return idempotency.Outcome{}, s.err
}

// A fence that can't vouch for the tool must leave the run resumable: an
// error and NO terminal event.
func TestFenceFailureLeavesRunResumable(t *testing.T) {
	key := idempotency.Key(resumeRunID, 1, "toolu_1")
	tests := []struct {
		name  string
		fence Fence
		want  error
	}{
		{"fence unavailable", stubFence{fmt.Errorf("%w: redis down", idempotency.ErrFenceUnavailable)}, idempotency.ErrFenceUnavailable},
		{"outcome unknown", stubFence{fmt.Errorf("%w: timed out", idempotency.ErrOutcomeUnknown)}, idempotency.ErrOutcomeUnknown},
		{"no fence configured fails closed", nil, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fx := newFenceFixture(t)
			state, err := replay.Fold(crashedJournal(t, "open_pr", stageInvoked, true, &key))
			if err != nil {
				t.Fatal(err)
			}
			journal, pub := newTestJournal(resumeRunID)
			loop := NewLoop(&stubLLMClient{}, fx.registry(), testModel, 1024, 10, io.Discard, journal, tt.fence)

			_, err = loop.Resume(context.Background(), state)

			if err == nil || (tt.want != nil && !errors.Is(err, tt.want)) {
				t.Fatalf("Resume() error = %v, want %v", err, tt.want)
			}
			if n := len(pub.snapshot()); n != 0 {
				t.Fatalf("published %d event(s) %v; a fence failure must publish no terminal event", n, eventTypes(pub.snapshot()))
			}
			if fx.execs["open_pr"] != 0 {
				t.Fatalf("Execute ran %d time(s) with no fence verdict", fx.execs["open_pr"])
			}
		})
	}
}
