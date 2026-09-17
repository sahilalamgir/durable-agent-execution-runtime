package tools

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/anthropics/anthropic-sdk-go"
)

// ApplyFixTool mocks applying a code fix to the repository. It always
// succeeds.
type ApplyFixTool struct{}

// NewApplyFixTool constructs an ApplyFixTool.
func NewApplyFixTool() *ApplyFixTool {
	return &ApplyFixTool{}
}

func (t *ApplyFixTool) Name() string { return "apply_fix" }

func (t *ApplyFixTool) Description() string {
	return "Applies a fix to the repository's code."
}

func (t *ApplyFixTool) InputSchema() anthropic.ToolInputSchemaParam {
	return anthropic.ToolInputSchemaParam{
		Properties: map[string]any{
			"description": map[string]any{
				"type":        "string",
				"description": "A description of the fix being applied.",
			},
		},
		Required: []string{"description"},
	}
}

type applyFixArgs struct {
	Description string `json:"description"`
}

func (t *ApplyFixTool) Execute(ctx context.Context, rawArgs json.RawMessage) (string, error) {
	var args applyFixArgs
	if err := json.Unmarshal(rawArgs, &args); err != nil {
		return "", fmt.Errorf("unmarshaling apply_fix args: %w", err)
	}
	return fmt.Sprintf("applied fix: %s", args.Description), nil
}
