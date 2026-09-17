Phase 1: Dumbest-Possible Agent Loop

Context

This project's real subject is the durability layer (event sourcing, idempotent tool execution, Kafka worker pool, chaos testing) that will eventually wrap an agent loop. Before any of that can be built, there needs to be an undurable baseline to wrap — a single process that runs one agent task to completion in memory, with no crash recovery, no persistence, nothing surviving a Ctrl+C. Per the system spec (FR-2, AC-2) and docs/workflow.md, Phase 1's entire job is to prove that baseline works, so every later phase's added complexity has something concrete to be measured against ("it used to just be this").

Confirmed repo state: internal/ and cmd/ don't exist yet — this is the first real application code in the repo (Phase 0 was just a throwaway connectivity script). No LLM SDK is present yet. Decisions already made with the user:

- LLM provider: Anthropic Claude, via the official anthropic-sdk-go SDK — the decision step is a real API call; only the tools are mocked.
- Mocked tools mirror the eventual Phase 9 repo-maintenance agent: clone_repo, run_tests, apply_fix, open_pr, driving a fake task ("fix the failing test in repo X") so the loop takes multiple genuine steps instead of one trivial round-trip.
- Entrypoint: cmd/worker, matching the architecture doc's eventual worker binary — Phase 1's main() hardcodes the fake task instead of consuming from Kafka; later phases add Kafka consumption on top of the same loop without relocating files.

Package Layout

/cmd/worker/main.go — env var loading, wiring, hardcoded fake task, entrypoint
/internal/agent/
    loop.go — Loop type, NewLoop, Run (the control-flow algorithm)
    llmclient.go — LLMClient interface + anthropicClient adapter
    types.go — Task, Outcome
    loop_test.go — table-driven tests against a stub LLMClient
/internal/tools/
    tool.go — Tool interface, Registry, NewRegistry
    mock_clone_repo.go / mock_run_tests.go / mock_apply_fix.go / mock_open_pr.go
    registry_test.go — table-driven tests for Lookup/Definitions

internal/tools has no dependency on internal/agent. internal/agent depends on internal/tools for the Tool/Registry types, and on anthropic-sdk-go's data types (message/tool/content-block params) directly — but not on the concrete \*anthropic.Client, which is abstracted behind an LLMClient interface so the loop is unit-testable without live network calls. This follows the go-conventions skill's DI rule (dependencies passed via constructor, no globals) and mirrors it in a new place: it's not just idempotency logic thatneeds to be testable in isolation, the loop does too.

Message History

Use []anthropic.MessageParam directly as the running conversation history — no custom wrapper struct. The SDK already provides everything needed (anthropic.NewUserMessage, resp.ToParam(), anthropic.NewToolResultBlock, block.AsAny() for type-safe narrowing), and Phase 1 never serializes this history outside the process, so there's no reason to own a parallel shape.

Tool Interface + Registry

type Tool interface {
Name() string
Description() string
InputSchema() anthropic.ToolInputSchemaParam
Execute(ctx context.Context, rawArgs json.RawMessage) (result string, err error)
}

type Registry struct{ byName map[string]Tool }
func NewRegistry(toolList ...Tool) *Registry
func (r *Registry) Lookup(name string) (Tool, bool)
func (r \*Registry) Definitions() []anthropic.ToolUnionParam // sorted by name for determinism

Execute takes raw JSON and unmarshals it itself inside each tool — Loop never needs to know any tool's argument shape, and "arguments fail to unmarshal" naturally surfaces as a returned error, never a panic.

The Loop's Control Flow (the core correctness piece)
```
func (l \*Loop) Run(ctx context.Context, task Task) (Outcome, error) {
messages := []anthropic.MessageParam{anthropic.NewUserMessage(anthropic.NewTextBlock(task.Prompt))}

     for step := 1; step <= l.maxSteps; step++ {              // unconditional cap, checked every iteration
         resp, err := l.llm.CreateMessage(ctx, anthropic.MessageNewParams{
             Model: l.model, MaxTokens: l.maxTokens,
             System: []anthropic.TextBlockParam{{Text: systemPrompt}},
             Messages: messages, Tools: l.registry.Definitions(),
         })
         if err != nil { return Outcome{}, fmt.Errorf("step %d: calling llm: %w", step, err) }
         l.printLLMResponse(step, resp)

         messages = append(messages, resp.ToParam())

         if resp.StopReason != anthropic.StopReasonToolUse {
             l.printTerminal(step, resp)
             return Outcome{Terminal: true, Steps: step, FinalText: extractText(resp)}, nil
         }

         var toolResults []anthropic.ContentBlockParamUnion     // batch ALL tool_use blocks from this ONE turn
         for _, block := range resp.Content {
             tu, ok := block.AsAny().(anthropic.ToolUseBlock)
             if !ok { continue }
             l.printToolInvocation(step, tu)

             tool, found := l.registry.Lookup(tu.Name)
             if !found {
                 l.printToolFailure(step, tu.Name, fmt.Errorf("unknown tool %q", tu.Name))
                 toolResults = append(toolResults, anthropic.NewToolResultBlock(tu.ID,
                     fmt.Sprintf("error: unknown tool %q", tu.Name), true))
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
         messages = append(messages, anthropic.NewUserMessage(toolResults...))  // ONE user turn, not one per tool
     }
     l.printMaxStepsExceeded()
     return Outcome{Terminal: false, MaxStepsExceeded: true, Steps: l.maxSteps}, nil

}
```

Correctness points this must get right:

- Terminal condition: resp.StopReason != anthropic.StopReasonToolUse. The model's accumulated text is the final answer.
- Multiple tool_use blocks in one turn: Anthropic's API allows a single assistant turn to request several tools at once (parallel tool use), and requires every resulting tool_result to be batched into exactly one next user turn — never one user message per toolcall. The loop executes blocks sequentially (no concurrency needed since mocks have no real I/O — call this out as a deliberate Phase 1 simplification) but collects results into a single slice before appending.
- Step counter: increments once per LLM round-trip, not per tool call — a turn with 3 tool_use blocks is still one step. This keeps Phase 1's step concept compatible with the future LLMResponded/ToolInvoked/ToolResulted event shapes without redesign (that schemaquestion itself stays a Phase 2 concern, per CLAUDE.md's spec-first rule).
- max_steps: the for loop's own bound, checked before every LLM call — not a fallback around otherwise-unbounded recursion. Exceedingit is a normal outcome (MaxStepsExceeded: true, no error, no panic), matching the terminal-condition design discussed in tutoring.
- Malformed tool calls (unknown tool name, or args that fail to unmarshal): both become an is_error: true tool_result fed back to themodel, the loop continues — never a crash. The model gets to see and react to its own mistake on the next turn, matching Anthropic's own tool-error guidance.

Mock Tool Behavior

Fake task prompt: "The repo github.com/example/widgets has a failing test. Clone it, run the tests, fix whatever is failing, re-runthe tests to confirm, then open a pull request with the fix."

┌────────────┬───────────────┬────────────────┬───────────────────────────────────────────────────────────────────────────────────┐
│ Tool │ Input │ State │ Behavior │
├────────────┼───────────────┼────────────────┼───────────────────────────────────────────────────────────────────────────────────┤
│ clone_repo │ {repo_url} │ none │ Always succeeds: "cloned <repo_url> into /workspace/widgets at commit a1b2c3d" │
├────────────┼───────────────┼────────────────┼───────────────────────────────────────────────────────────────────────────────────┤
│ run_tests │ {} │ calls int │ 1st call: "2 tests failed: TestParseWidget, TestWidgetTotal". 2nd+ call: "all │
│ │ │ counter │ tests passed (2/2)" │
├────────────┼───────────────┼────────────────┼───────────────────────────────────────────────────────────────────────────────────┤
│ apply_fix │ {description} │ none │ Always succeeds: "applied fix: <description>" │
├────────────┼───────────────┼────────────────┼───────────────────────────────────────────────────────────────────────────────────┤
│ open_pr │ {title, body} │ none │ Always succeeds: "opened PR #42: <title>" │
└────────────┴───────────────┴────────────────┴───────────────────────────────────────────────────────────────────────────────────┘

This drives a genuine multi-step arc: clone → run_tests (fails) → apply_fix → run_tests (passes) → open_pr → terminal text (5 toolsteps + 1 terminal step, well under maxSteps=10). The run_tests counter is a plain unsynchronized int field — correct only because Phase 1 is single-goroutine; flag this explicitly in the code as a simplification that would need a mutex the moment concurrency isintroduced.

stdout Output (AC-2 requires printing every step)

Loop takes an io.Writer in its constructor (DI-consistent, and gives tests a capture point without requiring one). cmd/worker passes os.Stdout; tests pass io.Discard.

step %d: llm responded (stop_reason=%s)
step %d: text: %q # if present
step %d: tool_use: %s(%s) # once per tool_use block
step %d: invoking tool %s with args %s
step %d: tool %s result: %s
step %d: tool %s FAILED: %v # unknown tool or unmarshal error
run complete after %d step(s): %s
run DID NOT complete: exceeded max_steps=%d without reaching a terminal state

cmd/worker exits 0 on Terminal, 1 on MaxStepsExceeded.

Testability

internal/agent/loop_test.go uses a stubLLMClient fed a scripted list of \*anthropic.Message responses. Important SDK gotcha: anthropic.Message's .AsAny()/union helpers read from an unexported field populated only by UnmarshalJSON — hand-built struct literalswon't work with type-switching. Fixtures must be built via json.Unmarshal of raw JSON strings shaped like real API responses (a mustMessage(t, rawJSON) test helper).

Table-driven cases for TestLoopRun:

- terminal on first response (no tool_use) → Terminal=true, Steps=1
- multi-step ends terminal (6 scripted fixtures matching the mock arc above) → Terminal=true, Steps=6
- max_steps exceeded (never a non-tool_use response) → MaxStepsExceeded=true, stub called exactly maxSteps times
- unknown tool name → no panic, loop continues, ends terminal
- malformed args (wrong JSON type) → no panic, loop continues, ends terminal

internal/tools/registry_test.go: lookup existing/unknown tool, Definitions() length and sort order. Plus one small table-driven test per mock tool (e.g. run_tests: call 1 → "failed", call 2 → "passed").

New Dependency

github.com/anthropics/anthropic-sdk-go@v1.73.0 (confirmed current official module path against the live proxy) — the only newdependency. Reason for the PR description: "Official Go SDK for the Anthropic Messages API — needed for the real LLM tool-selection call; its typed message/tool/content-block types are reused directly as conversation history, avoiding a redundant translation layer."

ANTHROPIC_API_KEY read via plain os.Getenv, log.Fatal if empty — matches Phase 0's fail-fast-at-startup convention, no new env-loading library.

Defaults (easy to change later, flagging as recommendations not fixed requirements): model "claude-sonnet-5" (a 4-tool selection task over deterministic mocks doesn't need more, and it's what gets re-run repeatedly during development), const maxSteps = 10 hardcoded in cmd/worker/main.go (workload config, not a secret — this is the value that maps onto the future RunStarted.max_steps field, name it accordingly now).

Implementation Sequencing

1.  internal/tools (interface, registry, 4 mocks + tests) — no dependency on agent, fastest to get green.
2.  internal/agent (LLMClient interface/adapter, Task/Outcome, Loop/Run, loop_test.go with JSON fixtures).
3.  cmd/worker/main.go (wiring, hardcoded task, env var check).
4.  Update CLAUDE.md: confirm go run ./cmd/worker command works as listed, flip the Phase 1 status row.

Verification

1.  go build ./... and go vet ./... clean.
2.  go test ./... — all table-driven cases above pass, no network calls made (stub-only).
3.  With ANTHROPIC_API_KEY set: go run ./cmd/worker — eyeball the printed step trace, confirm it shows the clone → run_tests(fail) → apply_fix → run_tests(pass) → open_pr → terminal arc, exits 0. This is the concrete evidence for AC-2 ("runs a fake task tocompletion against a mocked tool, printing each step, with no durability layer involved") — run it for real, don't just trust the tests.
4.  Manually lower maxSteps to something like 2 for one throwaway run to see the MaxStepsExceeded path fire for real, then restore it to 10.
5.  After merge: per docs/workflow.md, write docs/decisions/phase-1.md yourself — the note it flags as worth capturing is how the terminal-condition/step-counting design was structured, since Phase 2 wraps this exact loop in durability.
