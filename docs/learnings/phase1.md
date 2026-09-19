# Phase 1 learnings

## What changed
Before this phase there was no agent. Now `go run ./cmd/worker` runs a fake repo-maintenance task from start to finish: it asks Claude what to do, runs a mocked tool, feeds the result back, and repeats until Claude says it is done or the step limit is hit. Everything lives in memory in one process. If it dies, the run is gone. That is deliberate, this is the baseline the later phases add durability to.

## How it works
1. `cmd/worker` reads `ANTHROPIC_API_KEY` (exits if it's missing), builds a `Registry` holding the four mock tools, wraps the Anthropic client as an `LLMClient`, and calls `Loop.Run` with a hardcoded task.
2. `Loop.Run` starts a `messages` list with the task as the first user message.
3. Each step (up to `max_steps`) it sends the whole `messages` list plus the tool definitions to Claude. The API remembers nothing between calls, so the full history is sent every time.
4. If `stop_reason` is anything other than `tool_use`, the run is finished and `Run` returns `Outcome{Terminal: true}`.
5. Otherwise it walks the response's content blocks. For each `tool_use` block it looks the tool up in the `Registry` and calls `Execute`. The model never runs anything, it only asks.
6. All the results from that response go back as a single user message, and the loop continues.
7. If the loop runs out of steps it returns `Outcome{MaxStepsExceeded: true}` and `main()` exits with status 1.

Files: `internal/tools` (the `Tool` interface, `Registry`, four mocks), `internal/agent` (`Loop`, `LLMClient`, `Outcome`), `cmd/worker` (wiring).

## Reading the output
`step 2: tool run_tests result: 2 tests failed: TestParseWidget, TestWidgetTotal`
- `step N`: which LLM round-trip this line belongs to, starting at 1. A round-trip is one call to Claude plus running whatever tools it asked for.
- `llm responded (stop_reason=tool_use)`: Claude wants a tool run. `end_turn` means it is done.
- `tool_use: run_tests({})`: what Claude asked for. The next line, `invoking tool ...`, is our code acting on it.
- `tool ... result: ...`: what the mock returned, which goes back to Claude on the next step.
- `run complete after 6 step(s)` is a finished run. `run DID NOT complete: exceeded max_steps=...` means the limit stopped it.

## The test that proves it
Ran `go run ./cmd/worker` against the real Anthropic API after the review fixes. It took 6 steps in the expected order: `clone_repo`, `run_tests` (2 failed), `apply_fix`, `run_tests` (2/2 passed), `open_pr`, then a final summary with `stop_reason=end_turn`. No truncation warning, no errors.

Also ran it with `maxSteps` lowered to 2. It stopped after `run_tests`, printed `run DID NOT complete: exceeded max_steps=2 without reaching a terminal state`, and exited with status 1. So the limit stops the loop by itself, without waiting for Claude.

The unit tests cover the same paths without the network: finishes on the first response, the full 6-step arc, hitting the step limit, an unknown tool, bad arguments, a truncated response, an LLM error, and `NewLoop` panicking on `maxSteps <= 0`.

## Bugs that taught me something
- **Empty tool-result message:** if a response said `tool_use` but no tool_use block could be found, the loop still appended a user message with no content. The next API call would be rejected with an error that says nothing about the real cause. Fix: return a clear error before appending.
- **Truncated looked like success:** a response cut off by `max_tokens` was reported the same as a finished task. Fix: `Outcome.Truncated` plus a warning.
- **Hand-built test fixtures don't work:** the SDK's type switch only works on messages that went through `UnmarshalJSON`, so the test fixtures are JSON strings, not struct literals.
- **All of these passed the tests I had.** They only showed up when someone read the code cold and asked what could go wrong, not from running it.

## Next steps
- Phase 2: journal every step to Kafka before acting on it. This loop is what gets wrapped, and the event schema had to change first so one `LLMResponded` can hold several tool calls.
- Phase 3: real idempotency. Right now nothing stops a tool from running twice, and the mocks make that harmless.
- Known gaps left on purpose: no `ctx.Done()` check between steps (Phase 5), no reset of tool state between runs (Phase 4, documented on `Registry`), and `max_steps` is a constant, not input.
