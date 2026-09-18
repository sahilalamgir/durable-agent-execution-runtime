package replay

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"

	"github.com/anthropics/anthropic-sdk-go"

	"github.com/sahilalamgir/durable-agent-execution-runtime/internal/events"
)

// Messages reconstructs the exact conversation history the live LLM saw, so
// far, from s's journaled steps: the original prompt, then per step, the
// assistant turn followed by a user turn of tool_results once every one of
// that step's tool_use blocks has resulted.
//
// A step whose tool batch is only partially resolved contributes its
// assistant turn and then stops (EC-5): the Messages API requires every
// tool_result for one assistant turn to arrive in a single following user
// turn, so a partial batch cannot become a partial user turn. Apply's
// ErrToolOrder check (a new LLMResponded is rejected while the previous
// step's batch is still open) guarantees such a step is always the last one
// in s.steps, so stopping there never drops a later, fully-resolved step.
//
// Messages's error return is defensive only: a RunState that Apply/Fold
// produced should never fail here, since Apply already validates everything
// this method relies on. It exists for hand-built RunStates used outside
// the fold.
func (s RunState) Messages() ([]anthropic.MessageParam, error) {
	msgs := []anthropic.MessageParam{
		anthropic.NewUserMessage(anthropic.NewTextBlock(s.prompt)),
	}

	for _, step := range s.steps {
		blocks := make([]anthropic.ContentBlockParamUnion, len(step.content))
		for i, block := range step.content {
			blocks[i] = block.ToParam()
		}
		msgs = append(msgs, anthropic.NewAssistantMessage(blocks...))

		if len(step.order) == 0 {
			continue // terminal (or non-tool_use) step: nothing more to add.
		}
		if !allResulted(step) {
			break // EC-5: partial batch, no partial tool_result user turn.
		}

		results := make([]anthropic.ContentBlockParamUnion, len(step.order))
		for i, id := range step.order {
			r := step.results[id]
			results[i] = anthropic.NewToolResultBlock(id, r.Result, r.Status == "error")
		}
		msgs = append(msgs, anthropic.NewUserMessage(results...))
	}

	return msgs, nil
}

// digestView is the canonical, hashable projection of a RunState that
// Digest() hashes. Its fields are in a fixed declaration order, and its
// Messages field goes through canonicalize (see below) rather than being
// hashed as whatever raw bytes happened to produce it.
type digestView struct {
	RunID            string
	TenantID         string
	Status           RunStatus
	CurrentStep      int
	MaxSteps         int
	NextSequence     int64
	Messages         []anthropic.MessageParam
	PendingToolCalls []ToolCall
	InFlightTool     *ToolCall
	NextAction       NextAction
	FinalOutput      string
	Failure          *events.RunFailedPayload
	// LastStepIsTerminal mirrors the current step's LLMResponded.IsTerminal.
	// Without it, a step with zero tool_use blocks produces the exact same
	// NextAction (ActionNone) and the exact same Messages() output whether
	// IsTerminal was true or false — so two RunStates that mean genuinely
	// different things (one mid-flight after a terminal LLMResponded still
	// awaiting its terminal event, one after a malformed non-terminal
	// response awaiting its RunFailed) would otherwise hash identically.
	LastStepIsTerminal bool
}

// Digest returns a hex SHA-256 of a canonical JSON encoding of s (FR-17):
// two states that are semantically the same must hash the same, whether
// their events came from Kafka or from Postgres (EC-12).
//
// Getting "canonical" right here is subtler than "marshal a struct with
// fixed field order": every ToolCall.ToolArgs (in PendingToolCalls and
// InFlightTool) is still the tool_use block's raw json.RawMessage Input,
// verbatim from whatever source produced it. Postgres's JSONB reorders an
// object's keys, so the exact same tool call could arrive as
// {"a":1,"b":2} from Kafka and {"b":2,"a":1} from Postgres — two byte
// strings, one meaning, and a naive byte-for-byte hash would (wrongly)
// disagree between them.
//
// The same problem exists one level deeper than the plan's original
// description of this fix expected: Messages()'s assistant turns embed
// each tool_use block's Input via anthropic.ToolUseBlockParam, whose Input
// field, despite its `any` static type, is still populated with the exact
// same raw json.RawMessage by the SDK's ToParam() (confirmed directly
// against messageutil.go: `toolUse.Input = r.Input`, a verbatim copy, not a
// re-encode) — so a state with any pending or in-flight tool call carries
// this same raw-byte problem inside its Messages(), not only in the
// separate PendingToolCalls/InFlightTool fields. Re-marshaling ToolArgs
// alone (below) is not sufficient on its own to make two such states hash
// equal.
//
// So this does both: ToolCall.ToolArgs is re-marshaled through `any`
// per-field (as originally planned, and kept because it is simple, cheap,
// and harmless even once redundant), and the entire digestView is
// additionally round-tripped through a generic `any` value before hashing.
// Marshaling a Go map always sorts its keys, so unmarshaling arbitrary JSON
// into `any` (which turns any JSON object into a map[string]any) and
// marshaling it back collapses any two byte-different-but-semantically-
// identical JSON documents onto the same canonical bytes — wherever they
// occur in the structure, not just in the one field the original plan
// named.
func (s RunState) Digest() (string, error) {
	msgs, err := s.Messages()
	if err != nil {
		return "", fmt.Errorf("computing digest: %w", err)
	}

	view := digestView{
		RunID:              s.RunID,
		TenantID:           s.TenantID,
		Status:             s.Status,
		CurrentStep:        s.CurrentStep,
		MaxSteps:           s.MaxSteps,
		NextSequence:       s.NextSequence,
		Messages:           msgs,
		NextAction:         s.NextAction,
		FinalOutput:        s.FinalOutput,
		Failure:            s.Failure,
		LastStepIsTerminal: lastStepIsTerminal(s),
	}
	for _, tc := range s.PendingToolCalls {
		normalized, err := normalizeToolCall(tc)
		if err != nil {
			return "", fmt.Errorf("computing digest: %w", err)
		}
		view.PendingToolCalls = append(view.PendingToolCalls, normalized)
	}
	if s.InFlightTool != nil {
		normalized, err := normalizeToolCall(*s.InFlightTool)
		if err != nil {
			return "", fmt.Errorf("computing digest: %w", err)
		}
		view.InFlightTool = &normalized
	}

	canonical, err := canonicalize(view)
	if err != nil {
		return "", fmt.Errorf("computing digest: %w", err)
	}

	sum := sha256.Sum256(canonical)
	return hex.EncodeToString(sum[:]), nil
}

// lastStepIsTerminal returns the current (last) step's LLMResponded.IsTerminal,
// or false if s has no step yet. See digestView's doc comment (F7) for why
// Digest needs this exposed explicitly.
func lastStepIsTerminal(s RunState) bool {
	if len(s.steps) == 0 {
		return false
	}
	return s.steps[len(s.steps)-1].isTerminal
}

// normalizeToolCall re-marshals tc.ToolArgs through `any` (Unmarshal then
// Marshal), so its JSON key order no longer depends on whichever source
// (Kafka or Postgres) produced the original bytes.
func normalizeToolCall(tc ToolCall) (ToolCall, error) {
	if len(tc.ToolArgs) == 0 {
		return tc, nil
	}
	var generic any
	if err := json.Unmarshal(tc.ToolArgs, &generic); err != nil {
		return ToolCall{}, fmt.Errorf("normalizing tool_args for %s: %w", tc.ToolUseID, err)
	}
	normalized, err := json.Marshal(generic)
	if err != nil {
		return ToolCall{}, fmt.Errorf("re-marshaling normalized tool_args for %s: %w", tc.ToolUseID, err)
	}
	tc.ToolArgs = normalized
	return tc, nil
}

// canonicalize marshals v, then round-trips the result through a generic
// `any` value and marshals that. See Digest's doc comment for why this is
// necessary beyond digestView's already-fixed field order.
func canonicalize(v any) ([]byte, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("marshaling for canonicalization: %w", err)
	}
	var generic any
	if err := json.Unmarshal(b, &generic); err != nil {
		return nil, fmt.Errorf("round-tripping through any: %w", err)
	}
	canonical, err := json.Marshal(generic)
	if err != nil {
		return nil, fmt.Errorf("marshaling canonical form: %w", err)
	}
	return canonical, nil
}
