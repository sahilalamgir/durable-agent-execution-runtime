package agent

// Task is a single unit of work handed to the agent loop.
type Task struct {
	Prompt string
}

// Outcome describes how a Loop.Run call ended.
type Outcome struct {
	// Terminal is true when the LLM reached a natural stopping point
	// (StopReason != tool_use) and the run completed cleanly (RunCompleted
	// was journaled). It is false for every RunFailed outcome below,
	// including a truncated or malformed final response — those are
	// failures, not clean completions (D-3).
	Terminal bool
	// MaxStepsExceeded is true when the loop hit its step cap without
	// reaching a terminal state (RunFailed{error_class:
	// max_steps_exceeded}).
	MaxStepsExceeded bool
	// Steps is the number of LLM round-trips taken.
	Steps int
	// FinalText is the model's accumulated text from the terminal step.
	// Only meaningful when Terminal is true.
	FinalText string
	// Truncated is true when the run ended because the LLM response was
	// cut off (StopReason == max_tokens) rather than a natural stop
	// (RunFailed{error_class: llm_output_truncated}).
	Truncated bool
	// Failed is true whenever the run ended via any RunFailed event
	// (MaxStepsExceeded, Truncated, or a malformed LLM response with no
	// tool_use block) — callers that just need "did this run succeed"
	// without caring which RunFailed sub-kind can check this alone,
	// instead of needing to know every specific flag above.
	Failed bool
}
