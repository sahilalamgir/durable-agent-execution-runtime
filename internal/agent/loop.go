package agent

import (
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/anthropics/anthropic-sdk-go"

	"github.com/sahilalamgir/durable-agent-execution-runtime/internal/tools"
)

const systemPrompt = "You are a repo-maintenance agent. Use the available tools to complete the user's task, step by step. When the task is complete, respond with a final summary and do not call any more tools."

// Loop runs a single agent task to completion in memory: it alternates
// between calling the LLM for a decision and executing whatever tools it
// requests, until the LLM stops requesting tools or the step cap is
// reached. Loop has no durability of its own — nothing here survives a
// crash or restart; that is the whole point of Phase 1's undurable
// baseline.
type Loop struct {
	llm       LLMClient
	registry  *tools.Registry
	model     anthropic.Model
	maxTokens int64
	maxSteps  int
	out       io.Writer
}

// NewLoop constructs a Loop. maxSteps bounds the number of LLM round-trips
// taken by a single Run call; out receives a line of output for every step
// of the run. maxSteps must be positive; NewLoop panics otherwise, since an
// invalid maxSteps is a construction-time configuration error, not a
// runtime condition.
//
// registry's tool instances are stateful for the lifetime of this Loop (see
// Registry's doc comment): build a fresh Registry, via NewRegistry with
// fresh tool constructors, for each independent Run call if any tool
// carries state across invocations.
func NewLoop(llm LLMClient, registry *tools.Registry, model anthropic.Model, maxTokens int64, maxSteps int, out io.Writer) *Loop {
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
	}
}

// Run executes task to completion, or until maxSteps is exceeded. It never
// panics: an unknown tool name or malformed tool arguments become an
// is_error tool_result fed back to the model, and exceeding maxSteps is a
// normal (non-error) Outcome. Run only returns a non-nil error when the LLM
// call itself fails.
func (l *Loop) Run(ctx context.Context, task Task) (Outcome, error) {
	messages := []anthropic.MessageParam{anthropic.NewUserMessage(anthropic.NewTextBlock(task.Prompt))}

	for step := 1; step <= l.maxSteps; step++ {
		resp, err := l.llm.CreateMessage(ctx, anthropic.MessageNewParams{
			Model:     l.model,
			MaxTokens: l.maxTokens,
			System:    []anthropic.TextBlockParam{{Text: systemPrompt}},
			Messages:  messages,
			Tools:     l.registry.Definitions(),
		})
		if err != nil {
			return Outcome{}, fmt.Errorf("step %d: calling llm: %w", step, err)
		}
		if resp == nil {
			return Outcome{}, fmt.Errorf("step %d: llm returned a nil response with no error", step)
		}
		l.printLLMResponse(step, resp)

		messages = append(messages, resp.ToParam())

		if resp.StopReason == anthropic.StopReasonMaxTokens {
			l.printTerminal(step, resp)
			return Outcome{Terminal: true, Truncated: true, Steps: step, FinalText: extractText(resp)}, nil
		}

		if resp.StopReason != anthropic.StopReasonToolUse {
			l.printTerminal(step, resp)
			return Outcome{Terminal: true, Steps: step, FinalText: extractText(resp)}, nil
		}

		// Execute this turn's tool_use blocks sequentially and collect every
		// result into a single slice before appending. Anthropic's API
		// requires all tool_results for one assistant turn to be batched
		// into exactly one following user turn. Sequential execution (no
		// concurrency) is a deliberate Phase 1 simplification: the mocks do
		// no real I/O, so there is nothing to gain from running them in
		// parallel yet.
		var toolResults []anthropic.ContentBlockParamUnion
		for _, block := range resp.Content {
			tu, ok := block.AsAny().(anthropic.ToolUseBlock)
			if !ok {
				continue
			}
			l.printToolInvocation(step, tu)

			tool, found := l.registry.Lookup(tu.Name)
			if !found {
				err := fmt.Errorf("unknown tool %q", tu.Name)
				l.printToolFailure(step, tu.Name, err)
				toolResults = append(toolResults, anthropic.NewToolResultBlock(tu.ID, fmt.Sprintf("error: %v", err), true))
				continue
			}

			result, err := tool.Execute(ctx, tu.Input)
			if err != nil {
				l.printToolFailure(step, tu.Name, err)
				toolResults = append(toolResults, anthropic.NewToolResultBlock(tu.ID, fmt.Sprintf("error: %v", err), true))
				continue
			}
			l.printToolResult(step, tu.Name, result)
			toolResults = append(toolResults, anthropic.NewToolResultBlock(tu.ID, result, false))
		}
		if len(toolResults) == 0 {
			return Outcome{}, fmt.Errorf("step %d: llm reported stop_reason=tool_use but no recognized tool_use content block was found", step)
		}
		messages = append(messages, anthropic.NewUserMessage(toolResults...))
	}

	l.printMaxStepsExceeded()
	return Outcome{MaxStepsExceeded: true, Steps: l.maxSteps}, nil
}

func (l *Loop) printLLMResponse(step int, resp *anthropic.Message) {
	fmt.Fprintf(l.out, "step %d: llm responded (stop_reason=%s)\n", step, resp.StopReason)
	for _, block := range resp.Content {
		switch b := block.AsAny().(type) {
		case anthropic.TextBlock:
			fmt.Fprintf(l.out, "step %d:   text: %q\n", step, b.Text)
		case anthropic.ToolUseBlock:
			fmt.Fprintf(l.out, "step %d:   tool_use: %s(%s)\n", step, b.Name, string(b.Input))
		}
	}
}

func (l *Loop) printTerminal(step int, resp *anthropic.Message) {
	fmt.Fprintf(l.out, "run complete after %d step(s): %s\n", step, extractText(resp))
}

func (l *Loop) printToolInvocation(step int, tu anthropic.ToolUseBlock) {
	fmt.Fprintf(l.out, "step %d: invoking tool %s with args %s\n", step, tu.Name, string(tu.Input))
}

func (l *Loop) printToolResult(step int, name, result string) {
	fmt.Fprintf(l.out, "step %d: tool %s result: %s\n", step, name, result)
}

func (l *Loop) printToolFailure(step int, name string, err error) {
	fmt.Fprintf(l.out, "step %d: tool %s FAILED: %v\n", step, name, err)
}

func (l *Loop) printMaxStepsExceeded() {
	fmt.Fprintf(l.out, "run DID NOT complete: exceeded max_steps=%d without reaching a terminal state\n", l.maxSteps)
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
