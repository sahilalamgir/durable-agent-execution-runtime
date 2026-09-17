package agent

import (
	"context"
	"fmt"

	"github.com/anthropics/anthropic-sdk-go"
)

// LLMClient abstracts the single LLM call the loop needs, so Loop is
// unit-testable without live network calls. anthropicClient below is the
// only production implementation; tests substitute a stub.
type LLMClient interface {
	CreateMessage(ctx context.Context, params anthropic.MessageNewParams) (*anthropic.Message, error)
}

// anthropicClient adapts anthropic.Client to LLMClient.
type anthropicClient struct {
	client anthropic.Client
}

// NewAnthropicClient wraps client so it satisfies LLMClient.
func NewAnthropicClient(client anthropic.Client) LLMClient {
	return &anthropicClient{client: client}
}

func (a *anthropicClient) CreateMessage(ctx context.Context, params anthropic.MessageNewParams) (*anthropic.Message, error) {
	msg, err := a.client.Messages.New(ctx, params)
	if err != nil {
		return nil, fmt.Errorf("calling anthropic messages api: %w", err)
	}
	return msg, nil
}
