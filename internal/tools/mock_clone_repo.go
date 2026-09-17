package tools

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/anthropics/anthropic-sdk-go"
)

// CloneRepoTool mocks cloning a git repository into the local workspace. It
// always succeeds.
type CloneRepoTool struct{}

// NewCloneRepoTool constructs a CloneRepoTool.
func NewCloneRepoTool() *CloneRepoTool {
	return &CloneRepoTool{}
}

func (t *CloneRepoTool) Name() string { return "clone_repo" }

func (t *CloneRepoTool) Description() string {
	return "Clones a git repository into the local workspace."
}

func (t *CloneRepoTool) InputSchema() anthropic.ToolInputSchemaParam {
	return anthropic.ToolInputSchemaParam{
		Properties: map[string]any{
			"repo_url": map[string]any{
				"type":        "string",
				"description": "The URL of the repository to clone.",
			},
		},
		Required: []string{"repo_url"},
	}
}

type cloneRepoArgs struct {
	RepoURL string `json:"repo_url"`
}

func (t *CloneRepoTool) Execute(ctx context.Context, rawArgs json.RawMessage) (string, error) {
	var args cloneRepoArgs
	if err := json.Unmarshal(rawArgs, &args); err != nil {
		return "", fmt.Errorf("unmarshaling clone_repo args: %w", err)
	}
	return fmt.Sprintf("cloned %s into /workspace/widgets at commit a1b2c3d", args.RepoURL), nil
}
