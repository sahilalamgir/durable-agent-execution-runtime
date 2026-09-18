package tools

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/anthropics/anthropic-sdk-go"
)

// OpenPRTool mocks opening a pull request. It always succeeds.
type OpenPRTool struct{}

// NewOpenPRTool constructs an OpenPRTool.
func NewOpenPRTool() *OpenPRTool {
	return &OpenPRTool{}
}

func (t *OpenPRTool) Name() string { return "open_pr" }

// HasSideEffect reports true: opening a pull request is exactly the
// real-world side effect this project exists to guard against duplicating
// (Phase 3).
func (t *OpenPRTool) HasSideEffect() bool { return true }

func (t *OpenPRTool) Description() string {
	return "Opens a pull request with the given title and body."
}

func (t *OpenPRTool) InputSchema() anthropic.ToolInputSchemaParam {
	return anthropic.ToolInputSchemaParam{
		Properties: map[string]any{
			"title": map[string]any{
				"type":        "string",
				"description": "The pull request title.",
			},
			"body": map[string]any{
				"type":        "string",
				"description": "The pull request body.",
			},
		},
		Required: []string{"title", "body"},
	}
}

type openPRArgs struct {
	Title string `json:"title"`
	Body  string `json:"body"`
}

func (t *OpenPRTool) Execute(ctx context.Context, rawArgs json.RawMessage) (string, error) {
	if err := mockToolDelay(ctx); err != nil {
		return "", fmt.Errorf("waiting out mock delay: %w", err)
	}
	var args openPRArgs
	if err := json.Unmarshal(rawArgs, &args); err != nil {
		return "", fmt.Errorf("unmarshaling open_pr args: %w", err)
	}
	return fmt.Sprintf("opened PR #42: %s", args.Title), nil
}
