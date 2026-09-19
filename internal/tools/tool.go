// Package tools implements the mocked repo-maintenance tools the agent loop
// can invoke: clone_repo, run_tests, apply_fix, open_pr. Phase 9 will swap
// these mocks for the real implementations behind the same Tool interface.
package tools

import (
	"context"
	"encoding/json"
	"errors"
	"sort"

	"github.com/anthropics/anthropic-sdk-go"
)

// Invocation identifies one specific tool call to the tool that executes
// it. Side-effecting tools use it to tag their real-world effects (Phase 9's
// open_pr derives its branch name from IdempotencyKey) and to answer
// Reconcile questions about a previous attempt.
type Invocation struct {
	RunID     string
	Step      int
	ToolUseID string
	// IdempotencyKey is nil when the tool has no side effect.
	IdempotencyKey *string
}

// ErrOutcomeUnknown is returned by Execute when the tool cannot tell whether
// its side effect happened (e.g. a timeout after the request was sent). The
// idempotency Guard leaves the key claimed and the run resumable.
var ErrOutcomeUnknown = errors.New("tool: side effect outcome unknown")

// Tool is a single capability the agent loop can invoke by name. Execute
// takes raw JSON arguments and unmarshals them itself, so the loop never
// needs to know any individual tool's argument shape.
type Tool interface {
	Name() string
	Description() string
	InputSchema() anthropic.ToolInputSchemaParam
	// Execute returning a non-nil error other than ErrOutcomeUnknown asserts
	// the side effect did NOT happen. A real tool that can't be sure must
	// return ErrOutcomeUnknown.
	Execute(ctx context.Context, inv Invocation, rawArgs json.RawMessage) (result string, err error)
	// HasSideEffect reports whether a real invocation of this tool has a
	// real-world side effect (e.g. opening a PR) as opposed to only reading
	// state (e.g. cloning a repo, running tests). Redis fencing only guards
	// side-effecting tools; every ToolInvoked event journals this flag so
	// recovery never has to consult the current registry.
	HasSideEffect() bool
}

// ReconcileResult is a Reconciler's answer to "did this invocation's side
// effect already happen?".
type ReconcileResult struct {
	Applied bool
	// Result is the original result of the effect, set when Applied.
	Result string
}

// Reconciler is implemented by side-effecting tools that can ask the outside
// world whether a previous attempt took effect. A stale claim on a tool that
// is not a Reconciler is never re-executed (the run fails instead).
type Reconciler interface {
	Reconcile(ctx context.Context, inv Invocation, rawArgs json.RawMessage) (ReconcileResult, error)
}

// Registry looks up tools by name and builds the tool definitions sent to
// the LLM on every call.
//
// Mock tools keep no in-process state: what they did lives in the Ledger, so
// a resumed process sees the same world as the crashed one.
type Registry struct {
	byName map[string]Tool
}

// NewRegistry builds a Registry from toolList, keyed by each tool's Name().
func NewRegistry(toolList ...Tool) *Registry {
	byName := make(map[string]Tool, len(toolList))
	for _, t := range toolList {
		byName[t.Name()] = t
	}
	return &Registry{byName: byName}
}

// Lookup returns the tool registered under name, if any.
func (r *Registry) Lookup(name string) (Tool, bool) {
	t, ok := r.byName[name]
	return t, ok
}

// Definitions returns the tool schemas to send to the LLM, sorted by name so
// the request payload is deterministic across calls.
func (r *Registry) Definitions() []anthropic.ToolUnionParam {
	names := make([]string, 0, len(r.byName))
	for name := range r.byName {
		names = append(names, name)
	}
	sort.Strings(names)

	defs := make([]anthropic.ToolUnionParam, 0, len(names))
	for _, name := range names {
		t := r.byName[name]
		defs = append(defs, anthropic.ToolUnionParam{
			OfTool: &anthropic.ToolParam{
				Name:        t.Name(),
				Description: anthropic.String(t.Description()),
				InputSchema: t.InputSchema(),
			},
		})
	}
	return defs
}
