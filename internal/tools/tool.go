// Package tools implements the mocked repo-maintenance tools the agent loop
// can invoke: clone_repo, run_tests, apply_fix, open_pr. Phase 9 will swap
// these mocks for the real implementations behind the same Tool interface.
package tools

import (
	"context"
	"encoding/json"
	"sort"

	"github.com/anthropics/anthropic-sdk-go"
)

// Tool is a single capability the agent loop can invoke by name. Execute
// takes raw JSON arguments and unmarshals them itself, so the loop never
// needs to know any individual tool's argument shape.
type Tool interface {
	Name() string
	Description() string
	InputSchema() anthropic.ToolInputSchemaParam
	Execute(ctx context.Context, rawArgs json.RawMessage) (result string, err error)
}

// Registry looks up tools by name and builds the tool definitions sent to
// the LLM on every call.
//
// A Registry's tool instances are stateful for the lifetime of that
// Registry (see e.g. RunTestsTool's call counter). Build a fresh Registry,
// via NewRegistry with fresh tool constructors, for each independent
// agent.Loop.Run call if any tool carries state across invocations —
// reusing a Registry across runs leaks that state between them.
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
