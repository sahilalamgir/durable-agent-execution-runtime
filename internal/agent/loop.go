package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"time"

	"github.com/anthropics/anthropic-sdk-go"

	"github.com/sahilalamgir/durable-agent-execution-runtime/internal/events"
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
}

// NewLoop constructs a Loop. maxSteps bounds the number of LLM round-trips
// taken by a single Run call; out receives the FR-18 log line for every
// journaled event of the run. maxSteps must be positive; NewLoop panics
// otherwise, since an invalid maxSteps is a construction-time configuration
// error, not a runtime condition.
//
// registry's tool instances are stateful for the lifetime of this Loop (see
// Registry's doc comment): build a fresh Registry, via NewRegistry with
// fresh tool constructors, for each independent Run call if any tool
// carries state across invocations.
func NewLoop(llm LLMClient, registry *tools.Registry, model anthropic.Model, maxTokens int64, maxSteps int, out io.Writer, journal JournalConfig) *Loop {
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
	}
}

// Run executes task to completion, or until maxSteps is exceeded. It never
// panics: an unknown tool name or malformed tool arguments become an
// is_error tool_result fed back to the model (FR-9, EC-16), and exceeding
// maxSteps is a normal (non-error) Outcome. Run returns a non-nil error
// when the LLM call itself fails in a way that can't be journaled (a nil
// response with no error), or when journaling a step fails (ErrPublishFailed
// after retries, or a replay.Apply invariant violation, which should never
// happen against Loop's own well-formed envelopes).
//
// Every action Run takes — the LLM call, a tool's Execute — is driven
// purely by the folded RunState's NextAction (D-1): there is no separate
// step counter or messages slice threaded through this method.
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

	for {
		switch state.NextAction {
		case replay.ActionCallLLM:
			var outcome Outcome
			var done bool
			state, outcome, done, err = l.callLLMAndJournal(ctx, state)
			if err != nil {
				return Outcome{}, err
			}
			if done {
				return outcome, nil
			}

		case replay.ActionExecuteTool:
			state, err = l.executeNextToolAndJournal(ctx, state)
			if err != nil {
				return Outcome{}, err
			}

		case replay.ActionFailMaxSteps:
			state, err = l.publishApplyPrint(ctx, state, events.RunFailedPayload{
				Step:         state.CurrentStep,
				ErrorClass:   "max_steps_exceeded",
				ErrorMessage: fmt.Sprintf("exceeded max_steps=%d without reaching a terminal state", l.maxSteps),
				Retryable:    false,
			})
			if err != nil {
				return Outcome{}, err
			}
			return Outcome{MaxStepsExceeded: true, Failed: true, Steps: state.CurrentStep}, nil

		default:
			return Outcome{}, fmt.Errorf("run: state produced unexpected next_action=%s for a live run", state.NextAction)
		}
	}
}

// callLLMAndJournal makes the next LLM call, journals its LLMResponded
// event, and — if that response is terminal (stop_reason != tool_use) —
// journals the matching terminal event per FR-10 and reports done=true.
// Otherwise it reports done=false so Run's loop continues.
func (l *Loop) callLLMAndJournal(ctx context.Context, state replay.RunState) (replay.RunState, Outcome, bool, error) {
	messages, err := state.Messages()
	if err != nil {
		return state, Outcome{}, false, fmt.Errorf("computing messages from state: %w", err)
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
			return state, Outcome{}, false, err
		}
		return state, Outcome{}, false, fmt.Errorf("step %d: calling llm: %w", state.CurrentStep+1, callErr)
	}
	if resp == nil {
		return state, Outcome{}, false, fmt.Errorf("step %d: llm returned a nil response with no error", state.CurrentStep+1)
	}

	contentJSON, err := marshalContentVerbatim(resp.Content)
	if err != nil {
		return state, Outcome{}, false, fmt.Errorf("marshaling llm response content: %w", err)
	}
	isTerminal := resp.StopReason != anthropic.StopReasonToolUse

	next, err := l.publishApplyPrint(ctx, state, events.LLMRespondedPayload{
		Step:       state.CurrentStep + 1,
		StopReason: string(resp.StopReason),
		Content:    contentJSON,
		IsTerminal: isTerminal,
	})
	if err != nil {
		return state, Outcome{}, false, err
	}

	if !isTerminal {
		if len(next.PendingToolCalls) == 0 && next.InFlightTool == nil {
			// FR-10: stop_reason == tool_use but the response had no
			// tool_use content block at all.
			final, err := l.publishApplyPrint(ctx, next, events.RunFailedPayload{
				Step:         next.CurrentStep,
				ErrorClass:   "malformed_llm_response",
				ErrorMessage: "llm reported stop_reason=tool_use but the response had no tool_use content block",
				Retryable:    false,
			})
			if err != nil {
				return next, Outcome{}, false, err
			}
			return final, Outcome{Failed: true, Steps: final.CurrentStep}, true, nil
		}
		return next, Outcome{}, false, nil
	}

	if resp.StopReason == anthropic.StopReasonMaxTokens {
		final, err := l.publishApplyPrint(ctx, next, events.RunFailedPayload{
			Step:         next.CurrentStep,
			ErrorClass:   "llm_output_truncated",
			ErrorMessage: "llm response was truncated (stop_reason=max_tokens)",
			Retryable:    false,
		})
		if err != nil {
			return next, Outcome{}, false, err
		}
		return final, Outcome{Truncated: true, Failed: true, Steps: final.CurrentStep, FinalText: extractText(resp)}, true, nil
	}

	final, err := l.publishApplyPrint(ctx, next, events.RunCompletedPayload{
		FinalOutput: extractText(resp),
		TotalSteps:  next.CurrentStep,
	})
	if err != nil {
		return next, Outcome{}, false, err
	}
	return final, Outcome{Terminal: true, Steps: final.CurrentStep, FinalText: extractText(resp)}, true, nil
}

// executeNextToolAndJournal journals and executes exactly one tool call:
// state.PendingToolCalls[0], the next unstarted tool_use block in the
// current step's content order (FR-15). It always journals a ToolInvoked
// before calling Execute (FR-8), and always journals a matching
// ToolResulted afterward — with status "error" for an unknown tool name or
// a failed Execute call (FR-9, EC-16) — so every tool_use_id gets exactly
// one result.
func (l *Loop) executeNextToolAndJournal(ctx context.Context, state replay.RunState) (replay.RunState, error) {
	call := state.PendingToolCalls[0]
	tool, found := l.registry.Lookup(call.ToolName)

	hasSideEffect := found && tool.HasSideEffect()
	state, err := l.publishApplyPrint(ctx, state, events.ToolInvokedPayload{
		Step:          state.CurrentStep,
		ToolUseID:     call.ToolUseID,
		ToolName:      call.ToolName,
		ToolArgs:      call.ToolArgs,
		HasSideEffect: hasSideEffect,
	})
	if err != nil {
		return state, err
	}

	var result, status string
	if !found {
		result = fmt.Sprintf("error: unknown tool %q", call.ToolName)
		status = "error"
	} else if r, execErr := tool.Execute(ctx, call.ToolArgs); execErr != nil {
		result = fmt.Sprintf("error: %v", execErr)
		status = "error"
	} else {
		result = r
		status = "success"
	}

	return l.publishApplyPrint(ctx, state, events.ToolResultedPayload{
		Step:      state.CurrentStep,
		ToolUseID: call.ToolUseID,
		ToolName:  call.ToolName,
		Result:    result,
		Status:    status,
	})
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

// extractText walks resp's content blocks and joins every TextBlock's text,
// in order. It is the model's accumulated final answer once StopReason is
// no longer tool_use.
func extractText(resp *anthropic.Message) string {
	var parts []string
	for _, block := range resp.Content {
		if b, ok := block.AsAny().(anthropic.TextBlock); ok {
			parts = append(parts, b.Text)
		}
	}
	return strings.Join(parts, "")
}
