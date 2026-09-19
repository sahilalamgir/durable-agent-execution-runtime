package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"time"

	"github.com/anthropics/anthropic-sdk-go"

	"github.com/sahilalamgir/durable-agent-execution-runtime/internal/events"
	"github.com/sahilalamgir/durable-agent-execution-runtime/internal/idempotency"
	"github.com/sahilalamgir/durable-agent-execution-runtime/internal/kafka"
	"github.com/sahilalamgir/durable-agent-execution-runtime/internal/replay"
	"github.com/sahilalamgir/durable-agent-execution-runtime/internal/tools"
)

const systemPrompt = "You are a repo-maintenance agent. Use the available tools to complete the user's task, step by step. When the task is complete, respond with a final summary and do not call any more tools."

// ErrPublishFailed wraps a Publish error that survived every retry attempt
// (FR-8/EC-1). cmd/worker checks errors.Is(err, ErrPublishFailed) to choose
// exit code 3: the run may or may not be in the log (an ambiguous write),
// but no terminal event was written either way.
var ErrPublishFailed = errors.New("publishing event: retries exhausted after all attempts")

// Publish retry settings (system-spec.md's Publisher section): 5 attempts
// total, 200ms initial backoff doubling up to a 2s cap, always against the
// identical envelope.
const (
	publishMaxAttempts    = 5
	publishInitialBackoff = 200 * time.Millisecond
	publishMaxBackoff     = 2 * time.Second
)

// noCrashInjection is JournalConfig.CrashAfterSeq's sentinel for "never
// crash": sequence numbers are never negative, so 0 (a legitimate crash
// point — AC-13 tests it) can't double as "disabled".
const noCrashInjection int64 = -1

// JournalConfig carries everything Loop needs to journal a run's events to
// Kafka: who is publishing them, which run they belong to, and (for
// repeatable crash testing, FR-30) which acknowledged sequence_number
// should trigger an immediate os.Exit(137).
type JournalConfig struct {
	Publisher    kafka.EventPublisher
	RunID        string
	TenantID     string
	WorkloadType string
	// CrashAfterSeq is the sequence_number after which the worker should
	// exit(137), or noCrashInjection (-1) to disable crash injection. Use
	// NewJournalConfig to get a correctly-defaulted zero value.
	CrashAfterSeq int64
}

// NewJournalConfig builds a JournalConfig with crash injection disabled by
// default. Callers that want FR-30's crash injection set CrashAfterSeq on
// the returned value explicitly.
func NewJournalConfig(publisher kafka.EventPublisher, runID, tenantID, workloadType string) JournalConfig {
	return JournalConfig{
		Publisher:     publisher,
		RunID:         runID,
		TenantID:      tenantID,
		WorkloadType:  workloadType,
		CrashAfterSeq: noCrashInjection,
	}
}

// Fence runs a side-effecting tool under the idempotency fence. It is
// implemented by *idempotency.Guard; the interface lets tests substitute it.
type Fence interface {
	Run(ctx context.Context, inv tools.Invocation, call replay.ToolCall, tool tools.Tool) (idempotency.Outcome, error)
}

// Loop runs a single agent task to completion by journaling every step
// through Kafka before acting on it (FR-8): it publishes an event, applies
// it to a replay.RunState, and asks that RunState for the conversation
// history (FR-13, D-1) — there is no separate in-memory copy of history,
// so live execution and replay share exactly one code path.
type Loop struct {
	llm       LLMClient
	registry  *tools.Registry
	model     anthropic.Model
	maxTokens int64
	maxSteps  int
	out       io.Writer
	journal   JournalConfig
	fence     Fence
}

// NewLoop constructs a Loop. maxSteps bounds the number of LLM round-trips
// taken by a single Run call; out receives the FR-18 log line for every
// journaled event of the run. maxSteps must be positive; NewLoop panics
// otherwise, since an invalid maxSteps is a construction-time configuration
// error, not a runtime condition.
//
// fence guards every side-effecting tool. A Loop with a nil fence never
// executes a side-effecting tool (it fails closed with an error).
func NewLoop(llm LLMClient, registry *tools.Registry, model anthropic.Model, maxTokens int64, maxSteps int, out io.Writer, journal JournalConfig, fence Fence) *Loop {
	if maxSteps <= 0 {
		panic("agent: maxSteps must be positive")
	}
	return &Loop{
		llm:       llm,
		registry:  registry,
		model:     model,
		maxTokens: maxTokens,
		maxSteps:  maxSteps,
		out:       out,
		journal:   journal,
		fence:     fence,
	}
}

// Run starts a fresh run for task: it journals RunStarted and then drives
// the run to a terminal state. It never panics: an unknown tool name or
// malformed tool arguments become an is_error tool_result fed back to the
// model (FR-9, EC-16), and exceeding maxSteps is a normal (non-error)
// Outcome. Run returns a non-nil error when the LLM call itself fails, when
// journaling fails (ErrPublishFailed after retries), or when the fence
// cannot vouch for a side-effecting tool (idempotency.ErrFenceUnavailable /
// ErrOutcomeUnknown — no terminal event is journaled, so the run stays
// resumable).
func (l *Loop) Run(ctx context.Context, task Task) (Outcome, error) {
	inputJSON, err := json.Marshal(struct {
		Prompt string `json:"prompt"`
	}{task.Prompt})
	if err != nil {
		return Outcome{}, fmt.Errorf("marshaling RunStarted input: %w", err)
	}

	state, err := l.publishApplyPrint(ctx, replay.RunState{}, events.RunStartedPayload{
		WorkloadType: l.journal.WorkloadType,
		Input:        inputJSON,
		MaxSteps:     l.maxSteps,
	})
	if err != nil {
		return Outcome{}, err
	}
	return l.drive(ctx, state)
}

// Resume continues a run from state, which the caller obtained by replaying
// the run's events from Kafka. It publishes from state.NextSequence.
func (l *Loop) Resume(ctx context.Context, state replay.RunState) (Outcome, error) {
	return l.drive(ctx, state)
}

// drive is the one loop that handles every NextAction (FR-23), for a fresh
// run and a resumed one alike: everything it does is decided by the folded
// RunState (D-1), with no separate step counter or messages slice.
func (l *Loop) drive(ctx context.Context, state replay.RunState) (Outcome, error) {
	for {
		var (
			done *Outcome
			err  error
		)
		switch state.NextAction {
		case replay.ActionCallLLM:
			state, err = l.callLLMAndJournal(ctx, state)
		case replay.ActionExecuteTool:
			state, done, err = l.invokeToolAndJournal(ctx, state)
		case replay.ActionResolveInFlightTool:
			state, done, err = l.runInFlightTool(ctx, state)
		case replay.ActionFinishRun:
			return l.finishRun(ctx, state)
		case replay.ActionFailMaxSteps:
			return l.failMaxSteps(ctx, state)
		default:
			return Outcome{}, fmt.Errorf("run: state produced unexpected next_action=%s", state.NextAction)
		}
		if err != nil {
			return Outcome{}, err
		}
		if done != nil {
			return *done, nil
		}
	}
}

// callLLMAndJournal makes the next LLM call and journals its LLMResponded
// event. A terminal response leaves NextAction at FINISH_RUN; drive then
// journals the matching terminal event via RunState.TerminalPayload.
func (l *Loop) callLLMAndJournal(ctx context.Context, state replay.RunState) (replay.RunState, error) {
	messages, err := state.Messages()
	if err != nil {
		return state, fmt.Errorf("computing messages from state: %w", err)
	}

	resp, callErr := l.llm.CreateMessage(ctx, anthropic.MessageNewParams{
		Model:     l.model,
		MaxTokens: l.maxTokens,
		System:    []anthropic.TextBlockParam{{Text: systemPrompt}},
		Messages:  messages,
		Tools:     l.registry.Definitions(),
	})
	if callErr != nil {
		// FR-10: the call itself failed (after the SDK's own retries).
		// This is journaled as RunFailed, but — preserving Loop's existing
		// "only genuine LLM errors return error" contract — it is also
		// surfaced as a Go error, so cmd/worker's caller sees a non-nil
		// error exactly as it did in Phase 1.
		if _, err := l.publishApplyPrint(ctx, state, events.RunFailedPayload{
			Step:         state.CurrentStep,
			ErrorClass:   "llm_call_failed",
			ErrorMessage: callErr.Error(),
			Retryable:    isRetryableLLMError(callErr),
		}); err != nil {
			return state, err
		}
		return state, fmt.Errorf("step %d: calling llm: %w", state.CurrentStep+1, callErr)
	}
	if resp == nil {
		return state, fmt.Errorf("step %d: llm returned a nil response with no error", state.CurrentStep+1)
	}

	contentJSON, err := marshalContentVerbatim(resp.Content)
	if err != nil {
		return state, fmt.Errorf("marshaling llm response content: %w", err)
	}
	return l.publishApplyPrint(ctx, state, events.LLMRespondedPayload{
		Step:       state.CurrentStep + 1,
		StopReason: string(resp.StopReason),
		Content:    contentJSON,
		IsTerminal: resp.StopReason != anthropic.StopReasonToolUse,
	})
}

// finishRun journals the terminal event for a run whose last LLMResponded
// needs no more tools (FR-10). Live and resumed finishes both come through
// here, using RunState.TerminalPayload, so they cannot drift apart.
func (l *Loop) finishRun(ctx context.Context, state replay.RunState) (Outcome, error) {
	payload, err := state.TerminalPayload()
	if err != nil {
		return Outcome{}, fmt.Errorf("finishing run: %w", err)
	}
	final, err := l.publishApplyPrint(ctx, state, payload)
	if err != nil {
		return Outcome{}, err
	}

	outcome := Outcome{Steps: final.CurrentStep}
	switch p := payload.(type) {
	case events.RunCompletedPayload:
		outcome.Terminal = true
		outcome.FinalText = p.FinalOutput
	case events.RunFailedPayload:
		outcome.Failed = true
		if p.ErrorClass == "llm_output_truncated" {
			outcome.Truncated = true
			outcome.FinalText = state.LastStepText()
		}
	}
	return outcome, nil
}

func (l *Loop) failMaxSteps(ctx context.Context, state replay.RunState) (Outcome, error) {
	final, err := l.publishApplyPrint(ctx, state, events.RunFailedPayload{
		Step:         state.CurrentStep,
		ErrorClass:   "max_steps_exceeded",
		ErrorMessage: fmt.Sprintf("exceeded max_steps=%d without reaching a terminal state", state.MaxSteps),
		Retryable:    false,
	})
	if err != nil {
		return Outcome{}, err
	}
	return Outcome{MaxStepsExceeded: true, Failed: true, Steps: final.CurrentStep}, nil
}

// failRun journals RunFailed for an unrecoverable tool-path problem and
// reports the run as failed.
func (l *Loop) failRun(ctx context.Context, state replay.RunState, errorClass, message string) (replay.RunState, *Outcome, error) {
	final, err := l.publishApplyPrint(ctx, state, events.RunFailedPayload{
		Step:         state.CurrentStep,
		ErrorClass:   errorClass,
		ErrorMessage: message,
		Retryable:    false,
	})
	if err != nil {
		return state, nil, err
	}
	return final, &Outcome{Failed: true, Steps: final.CurrentStep}, nil
}

// invokeToolAndJournal handles a tool call that has no ToolInvoked yet
// (state.PendingToolCalls[0], the next unstarted tool_use block in content
// order, FR-15). It computes the idempotency key exactly once, here, for a
// side-effecting tool (FR-3), journals ToolInvoked and waits for the ack,
// and only then hands off to runInFlightTool — the same path a resumed run
// takes (D-3).
func (l *Loop) invokeToolAndJournal(ctx context.Context, state replay.RunState) (replay.RunState, *Outcome, error) {
	call := state.PendingToolCalls[0]
	tool, found := l.registry.Lookup(call.ToolName)

	hasSideEffect := found && tool.HasSideEffect()
	var key *string
	if hasSideEffect {
		k := idempotency.Key(l.journal.RunID, state.CurrentStep, call.ToolUseID)
		key = &k
	}
	state, err := l.publishApplyPrint(ctx, state, events.ToolInvokedPayload{
		Step:           state.CurrentStep,
		ToolUseID:      call.ToolUseID,
		ToolName:       call.ToolName,
		ToolArgs:       call.ToolArgs,
		IdempotencyKey: key,
		HasSideEffect:  hasSideEffect,
	})
	if err != nil {
		return state, nil, err
	}
	return l.runInFlightTool(ctx, state)
}

// runInFlightTool executes state.InFlightTool, whose ToolInvoked is already
// journaled, and journals its ToolResulted. It trusts the journaled
// has_side_effect and idempotency_key, never the current registry or key
// code (FR-3): a redeploy between crash and resume must not change what
// counts as a side effect. A tool that only reads is simply re-executed
// (FR-22); a side-effecting one always goes through the fence.
func (l *Loop) runInFlightTool(ctx context.Context, state replay.RunState) (replay.RunState, *Outcome, error) {
	call := *state.InFlightTool
	tool, found := l.registry.Lookup(call.ToolName)
	inv := tools.Invocation{
		RunID:          l.journal.RunID,
		Step:           state.CurrentStep,
		ToolUseID:      call.ToolUseID,
		IdempotencyKey: call.IdempotencyKey,
	}

	if !call.HasSideEffect {
		return l.executeUnfenced(ctx, state, call, inv, tool, found)
	}
	switch {
	case !found:
		return l.failRun(ctx, state, "unresolved_side_effect", fmt.Sprintf(
			"tool %q (tool_use_id=%s) was journaled with has_side_effect=true but is not registered, so whether it ran cannot be checked",
			call.ToolName, call.ToolUseID))
	case call.IdempotencyKey == nil:
		return l.failRun(ctx, state, "unresolved_side_effect", fmt.Sprintf(
			"tool %q (tool_use_id=%s) has a side effect but its ToolInvoked has no idempotency key, so whether it ran cannot be checked",
			call.ToolName, call.ToolUseID))
	case l.fence == nil:
		return state, nil, fmt.Errorf("running side-effecting tool %q: no idempotency fence configured", call.ToolName)
	}
	return l.executeFenced(ctx, state, call, inv, tool)
}

// executeUnfenced runs a tool with no side effect. An unknown tool name or a
// failed Execute becomes status "error" (FR-9, EC-16), so every tool_use_id
// gets exactly one result.
func (l *Loop) executeUnfenced(ctx context.Context, state replay.RunState, call replay.ToolCall, inv tools.Invocation, tool tools.Tool, found bool) (replay.RunState, *Outcome, error) {
	var result, status string
	if !found {
		result = fmt.Sprintf("error: unknown tool %q", call.ToolName)
		status = "error"
	} else if r, execErr := tool.Execute(ctx, inv, call.ToolArgs); execErr != nil {
		result = fmt.Sprintf("error: %v", execErr)
		status = "error"
	} else {
		result = r
		status = "success"
	}
	return l.journalToolResult(ctx, state, call, idempotency.Outcome{
		Result: result, Status: status, Resolution: idempotency.ResolutionExecuted,
	})
}

// executeFenced runs a side-effecting tool through the Guard and maps its
// verdict: an unresolvable or mismatched record fails the run; a fence that
// can't be consulted, or an unknown outcome, returns an error with NO
// terminal event so the run stays resumable (FR-8, FR-12).
func (l *Loop) executeFenced(ctx context.Context, state replay.RunState, call replay.ToolCall, inv tools.Invocation, tool tools.Tool) (replay.RunState, *Outcome, error) {
	outcome, err := l.fence.Run(ctx, inv, call, tool)
	switch {
	case errors.Is(err, idempotency.ErrUnresolvable):
		return l.failRun(ctx, state, "unresolved_side_effect", err.Error())
	case errors.Is(err, idempotency.ErrRecordMismatch):
		return l.failRun(ctx, state, "idempotency_record_mismatch", err.Error())
	case err != nil:
		return state, nil, fmt.Errorf("running side-effecting tool %q: %w", call.ToolName, err)
	}
	return l.journalToolResult(ctx, state, call, outcome)
}

func (l *Loop) journalToolResult(ctx context.Context, state replay.RunState, call replay.ToolCall, o idempotency.Outcome) (replay.RunState, *Outcome, error) {
	next, err := l.publishApplyPrint(ctx, state, events.ToolResultedPayload{
		Step:                 state.CurrentStep,
		ToolUseID:            call.ToolUseID,
		ToolName:             call.ToolName,
		Result:               o.Result,
		Status:               o.Status,
		WasReplayedFromCache: o.Resolution == idempotency.ResolutionCached,
		Resolution:           string(o.Resolution),
	})
	return next, nil, err
}

// publishApplyPrint is the one chokepoint every journaled step in Loop goes
// through (FR-8, FR-18, FR-30): build one envelope for payload, retry
// Publish against that identical envelope up to publishMaxAttempts times
// with exponential backoff, apply the acknowledged event to state, check
// DAE_CRASH_AFTER_SEQ (before printing anything), print the FR-18 log line,
// and return the new state.
//
// Retry/backoff lives here rather than inside the EventPublisher
// implementation deliberately: it lets AC-6 be tested with a plain
// in-memory fake publisher, with no Kafka-specific retry logic anywhere in
// tests.
func (l *Loop) publishApplyPrint(ctx context.Context, state replay.RunState, payload events.Payload) (replay.RunState, error) {
	env, err := events.NewEnvelope(l.journal.RunID, l.journal.TenantID, state.NextSequence, payload, time.Now())
	if err != nil {
		return state, fmt.Errorf("building envelope for %s: %w", payload.EventType(), err)
	}

	// The envelope's idempotency_key mirrors the payload's, set here so the
	// two can never diverge (FR-1). The projector writes it to Postgres.
	if invoked, ok := payload.(events.ToolInvokedPayload); ok {
		env.IdempotencyKey = invoked.IdempotencyKey
	}

	if err := l.publishWithRetry(ctx, env); err != nil {
		return state, fmt.Errorf("publishing %s (seq %d): %w: %v", payload.EventType(), env.SequenceNumber, ErrPublishFailed, err)
	}

	next, err := replay.Apply(state, env)
	if err != nil {
		return state, fmt.Errorf("applying %s (seq %d) to state: %w", payload.EventType(), env.SequenceNumber, err)
	}

	if l.journal.CrashAfterSeq != noCrashInjection && env.SequenceNumber == l.journal.CrashAfterSeq {
		os.Exit(137)
	}

	digest, err := next.Digest()
	if err != nil {
		return state, fmt.Errorf("computing digest after %s (seq %d): %w", payload.EventType(), env.SequenceNumber, err)
	}
	fmt.Fprintf(l.out, "seq=%d event=%s step=%d next_action=%s state_digest=%s\n",
		env.SequenceNumber, env.EventType, next.CurrentStep, next.NextAction, digest)

	return next, nil
}

// publishWithRetry retries l.journal.Publisher.Publish against the
// identical env up to publishMaxAttempts times, with backoff doubling from
// publishInitialBackoff up to publishMaxBackoff. It never builds a new
// envelope to retry.
func (l *Loop) publishWithRetry(ctx context.Context, env events.Envelope) error {
	backoff := publishInitialBackoff
	var lastErr error
	for attempt := 1; attempt <= publishMaxAttempts; attempt++ {
		lastErr = l.journal.Publisher.Publish(ctx, env)
		if lastErr == nil {
			return nil
		}
		if attempt == publishMaxAttempts {
			break
		}

		timer := time.NewTimer(backoff)
		select {
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		}

		backoff *= 2
		if backoff > publishMaxBackoff {
			backoff = publishMaxBackoff
		}
	}
	return lastErr
}

// isRetryableLLMError classifies an LLM call failure per FR-10: true for a
// 429 or 5xx API error, or a genuine network error; false otherwise. A
// network failure never comes back as an *anthropic.Error with a status
// code — there was no HTTP response to have one — so it's classified via
// the stdlib net.Error interface instead.
func isRetryableLLMError(err error) bool {
	var apiErr *anthropic.Error
	if errors.As(err, &apiErr) {
		return apiErr.StatusCode == 429 || apiErr.StatusCode >= 500
	}
	var netErr net.Error
	return errors.As(err, &netErr)
}

// marshalContentVerbatim journals content as the exact bytes the Messages
// API returned for each block, not a re-encoding of the parsed Go struct.
// anthropic.ContentBlockUnion has no custom MarshalJSON, so a plain
// json.Marshal serializes every struct field using its json tag regardless
// of whether the original response actually included that key — e.g.
// ToolUseBlock's ToolsetName ("toolset_name") has no `omitempty` tag, so it
// always comes out as "toolset_name":"" even when the API never sent that
// key at all (the normal case: nothing here uses Anthropic's toolset
// feature).
//
// That silently turns "absent" into "present but empty" in the journal. On
// replay, RunState.Messages() rebuilds each block via its exported
// ToParam(), which decides whether to include a field like toolset_name in
// the next outgoing request based on whether the SDK's presence-tracking
// metadata marked it as present in whatever bytes it was decoded from — and
// since our journaled bytes now explicitly contain that key, ToParam()
// reproduces it as an explicit empty string, which the API rejects
// ("toolset_name: String should have at least 1 character").
//
// RawJSON() returns each block's actual original bytes as the SDK's decoder
// captured them, so re-assembling the array from those preserves every
// field's presence or absence exactly, for any such field — not just this
// one — matching the phase spec's "encoded verbatim" requirement for real.
func marshalContentVerbatim(content []anthropic.ContentBlockUnion) (json.RawMessage, error) {
	raw := make([]json.RawMessage, len(content))
	for i, block := range content {
		raw[i] = json.RawMessage(block.RawJSON())
	}
	return json.Marshal(raw)
}
