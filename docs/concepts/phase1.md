# Phase 1 in plain terms

## What changed

Before this phase there was no agent. Phase 1 builds the dumbest possible one: a single Go process that asks Claude what to do, runs a fake tool, tells Claude what happened, and repeats until the task is done. Everything lives in memory. If the process dies, the run is gone. That is on purpose: this is the baseline every later phase wraps durability around.

## New concepts

1. The agent loop
  - Ask the LLM what to do, run whatever it picks, feed the result back, repeat. One trip around is a "step." One call to Claude plus the tools it asked for counts as one step.

2. LLM APIs are stateless
  - Claude remembers nothing between calls. To keep a conversation going, you resend the whole history every time. That growing list of messages is the run's entire state in Phase 1, and it is what gets lost in a crash.

3. Tool calling (the model has no hands)
  - You give Claude a menu of tools: a name, a description, and what arguments each takes. Claude never runs anything. It only replies "please call run_tests with these args." Your code does the actual running and sends the result back as a `tool_result` message.

4. Tool, Registry, and mocks
  - A `Tool` is anything with a name, a description, an argument schema, and an `Execute` function. The `Registry` is a lookup table: tool name to the tool's code, built once at startup with all four tools in it. The four tools (`clone_repo`, `run_tests`, `apply_fix`, `open_pr`) are fakes that return canned text, so the loop can be tested with no real side effects.

5. Two ways to stop the loop

- Claude decides it's done. Its `stop_reason` is anything other than `tool_use`.
- `max_steps` runs out. This is an independent cap, checked before every Claude call. Without it, a confused model could keep trying "one more fix" forever and you'd pay for every try.
- Hitting the cap is a normal result, not a crash.

6. Several tool calls in one response
  - One Claude response can ask for more than one tool. Anthropic requires every result back in a single user message, one `tool_result` per request. One message per result breaks it. That is why the step counter counts Claude calls, not tool calls.

7. Bad tool calls are data, not crashes
  - If Claude asks for a tool that doesn't exist, or sends arguments that don't parse, the loop sends back an error result instead of crashing. Claude sees its own mistake and can try again.

8. An interface so the loop is testable
  - The loop only knows a one-method `LLMClient` interface. In `main`, that is the real Anthropic client. In tests, it is a stub returning scripted responses. So tests run with no network, no key and no cost.

9. No durability, on purpose
  - Nothing is written to disk, Kafka, Redis or Postgres. Phase 1 exists to show what "it used to just be this" looks like, so the added complexity in Phase 2 and 3 has something to be compared to.

## What happens under the hood

1. `main` reads `ANTHROPIC_API_KEY`, builds the Registry with the four tools, and builds the Loop.
2. The Loop starts a message list with the task as the first message.
3. Each step: send the whole message list plus the tool menu to Claude.
4. If `stop_reason` is not `tool_use`, the run is finished.
5. Otherwise, for each tool request in the response: look it up in the Registry, call `Execute`, and collect the result.
6. Append Claude's response and one user message holding all the results, then go to step 3.
7. If the step count passes `max_steps`, stop and report that it didn't finish.

### Example: what a 6-step run looks like

┌──────┬──────────────────────────┬───────────────────────────────────────┐
│ Step │ Claude asks for │ Mock returns │
├──────┼──────────────────────────┼───────────────────────────────────────┤
│ 1 │ clone_repo │ cloned ... at commit a1b2c3d │
├──────┼──────────────────────────┼───────────────────────────────────────┤
│ 2 │ run_tests │ 2 tests failed (first call) │
├──────┼──────────────────────────┼───────────────────────────────────────┤
│ 3 │ apply_fix │ applied fix: ... │
├──────┼──────────────────────────┼───────────────────────────────────────┤
│ 4 │ run_tests │ all tests passed (second call) │
├──────┼──────────────────────────┼───────────────────────────────────────┤
│ 5 │ open_pr │ opened PR #42 │
├──────┼──────────────────────────┼───────────────────────────────────────┤
│ 6 │ nothing (stop_reason=end_turn) │ run complete │
└──────┴──────────────────────────┴───────────────────────────────────────┘

The `run_tests` mock counts its own calls, which is what makes it fail once and then pass.

### Example: the failure that motivates Phase 2

Kill the process at step 3. The message list, the step number and the fact that `clone_repo` and `run_tests` already ran are all gone. Restarting means step 1 again. With a mock that is harmless. With a real `open_pr` it would open a second PR. Nothing in Phase 1 can prevent that, and that is the problem the next phases solve.

## New things you'll run

- `ANTHROPIC_API_KEY=... go run ./cmd/worker` runs the fake task against the real Claude API and prints every step.
- `go test ./...` runs the loop and tool tests using a stub instead of the network.
- Lower `maxSteps` to 2 and run again to watch the cap stop the loop and exit with status 1.

## Reading a step line

step 2: tool run_tests result: 2 tests failed: TestParseWidget, TestWidgetTotal

- step: which Claude round-trip this belongs to, starting at 1.
- tool run_tests: which tool ran.
- result: what the mock returned. Claude sees this text on the next step.

Other lines: `llm responded (stop_reason=tool_use)` means Claude wants a tool run, `end_turn` means it's done. `run DID NOT complete` means `max_steps` stopped it.

## Known limits, on purpose

- No crash recovery. The run only exists as long as the process does (Phase 2 adds the journal).
- Tools run one at a time, and the mocks keep state on the instance. A second run on the same Registry would see `run_tests` pass immediately, so a fresh Registry is needed per run.
- A response cut off by `max_tokens` is flagged as `Truncated` but still counts as finished.
- No cancellation between steps yet (Phase 5).
