package agent

// Task is a single unit of work handed to the agent loop.
type Task struct {
	Prompt string
}

// Outcome describes how a Loop.Run call ended.
type Outcome struct {
	// Terminal is true when the LLM reached a natural stopping point
	// (StopReason != tool_use).
	Terminal bool
	// MaxStepsExceeded is true when the loop hit its step cap without
	// reaching a terminal state.
	MaxStepsExceeded bool
	// Steps is the number of LLM round-trips taken.
	Steps int
	// FinalText is the model's accumulated text from the terminal step.
	// Only meaningful when Terminal is true.
	FinalText string
	// Truncated is true when Terminal is true because the LLM response was
	// cut off (StopReason == max_tokens) rather than a natural stop. A
	// truncated run is still terminal, but callers should not treat it as a
	// clean completion.
	Truncated bool
}
