# Phase 1 decisions

## `max_steps` is a second, independent way to stop the loop
- The loop ends when the LLM stops asking for tools (`stop_reason != tool_use`) or when `step` passes `max_steps`, whichever comes first. The step limit is the `for` loop's own condition, checked before every LLM call.
  - Why: if the LLM alone decides when to stop, a model stuck on a bug can keep trying "one more fix" forever, and every iteration costs real API money.
  - Rejected: only checking the limit after something looks wrong. A guard that only runs when I've already noticed the problem doesn't guard anything.
- Hitting the limit is a normal result (`Outcome.MaxStepsExceeded`), not an error and not a panic. `cmd/worker` turns it into exit status 1.
- `max_steps` is a hardcoded constant (10) for now. It is named that way because it becomes `RunStarted.max_steps` in Phase 2.

## The step counter counts LLM round-trips, not tool calls
- One LLM response can ask for several tools at once. That is still one step.
  - Why: the future `LLMResponded` event is one event per LLM response, keyed by `step`. If steps counted tool calls, one LLM decision would have to be split across several step numbers.

## Real LLM call, mocked tools
- The decision step calls the real Anthropic API. Only the tools are fake (`clone_repo`, `run_tests`, `apply_fix`, `open_pr`), and they are the same four Phase 9 will make real.
  - Why: I wanted the LLM integration proven in isolation now. If I had mocked the LLM too, a failure in Phase 3 or 4 could be a new bug or an LLM-integration bug I had never actually exercised, and I couldn't tell which.
  - `run_tests` fails on its first call and passes after that, so the demo needs a real fix-and-retest sequence (6 steps) instead of finishing in one round-trip.

## `Loop` depends on an `LLMClient` interface, not the Anthropic client
- `Loop` only knows about a one-method `LLMClient` interface. `NewAnthropicClient` is the real implementation and the tests pass a stub that returns scripted responses.
  - Why: the thing under test is the loop logic (stop conditions, tool handling, batching), not Claude. With the interface the tests run with no network, no API key and no cost.
  - The stub's responses are built with `json.Unmarshal` of raw JSON, not struct literals. The SDK's `AsAny()` type switch reads a hidden field that only `UnmarshalJSON` fills in, so a hand-built struct would make the loop silently see no tool calls.

## All tool results from one response go back in a single user message
- A single LLM response can contain several `tool_use` blocks. Anthropic requires the reply to contain one `tool_result` for each of them, all in one user message. The loop runs the tools one at a time, collects every result, then appends one message.
  - Why it's easy to get wrong: sending one message per tool result looks natural and works when there is only one tool call. It breaks only when the model asks for two or more.
  - Tools run sequentially on purpose. The mocks do no real I/O, so parallel execution would add synchronization and gain nothing.

## A bad tool call becomes a tool result, not a crash
- An unknown tool name or arguments that don't parse produce an `is_error` tool result that is sent back to the model, and the loop continues. The model sees its own mistake and can try again.
  - Each tool unmarshals its own arguments inside `Execute`, so a parse failure is just a returned error and the loop never needs to know any tool's argument shape.
- If the model says `stop_reason=tool_use` but no tool_use block can be found in the content, the loop returns an error instead. Sending an empty user message would make the next API call fail with an unrelated-looking 400, and that message would stay in the history for good.

## What the adversarial review caught after the code passed its own tests
- The plan looked complete and all tests passed. A cold review still found real problems, and I fixed four of them.
  - No nil check on the LLM response. `LLMClient` is an interface, so any implementation could return `(nil, nil)` and crash the loop, which contradicts "`Run` never panics".
  - `stop_reason=max_tokens` was reported as a clean finish. A cut-off answer looked the same as a completed task. Fix: `Outcome.Truncated`, and `cmd/worker` prints a warning. It still exits 0, because AC-2 doesn't treat truncation as failure.
  - `max_steps <= 0` silently did nothing and reported "exceeded". Fix: `NewLoop` panics. It is a config mistake, and the "panic only during startup/config" rule applies.
  - `os.Exit(1)` was called from inside `run()`. It skips every `defer` in that function, and later phases will add cleanup there (closing a Kafka producer). Fix: `run()` returns a sentinel error and only `main()` exits.
- Not fixed on purpose: checking `ctx.Done()` between steps. Nothing can cancel a run until `CancelRun` exists in Phase 5, and a check with nothing to test it against would be dead code.

## Tool state living across runs is documented, not fixed
- `RunTestsTool` counts its calls on the instance, and one `Registry` holds one instance. A second `Run` on the same registry would see "passed" on its first `run_tests` call and skip the failing-tests step. Not a race (there is one goroutine), a lifecycle problem.
- Solution: a doc comment on `Registry` and `NewLoop` saying a fresh `Registry` is needed per independent run.
  - Rejected: a reset mechanism or a run ID now. Phase 1 only ever has one run, and that machinery belongs to Phase 2+.
  - The reviewer's version of this doesn't hold in Phase 1 but does in Phase 4, when a worker takes more than one run. Writing it down now costs one comment.

## Gap found for Phase 2: the event schema assumed one tool call per response
- The spec's `LLMResponded` event was `{ step, chosen_tool, tool_args, is_terminal }`, one tool per event, and it had nowhere to keep each `tool_use`'s own `id`. Phase 1's loop had already shown that one response can hold several, and replay needs each id to match a result to its call.
- Phase 1 emits no events, so nothing broke. Phase 2 planning caught it and amended `system-spec.md` first, as CLAUDE.md requires: `LLMResponded` now stores the response's `content` array as returned, and `ToolInvoked`/`ToolResulted` gained `tool_use_id`.
