1. If terminal condition completely depends on the LLM, the AI agent can be stuck in an infinite loop because the LLM may be trapped in a bug and will always try something new to fix it

- Solution: Create a new, independent condition to break out of the loop called max_steps, so if agent gets passed this constant, the loop terminates

2. Step counter increments every LLM round-trip, not every tool call so that in the future, the LLMResponded event can key tool_args by step

3. Wanted to test LLM in this phase to make sure it was working at the basic level, so that we know it's not an error in later phases

4. Made Loop depend on LLMClient interface instead of the Antropic SDK client directly so that we could mock it in LLMClient

- LLMClient can either send to Anthropic SDK client or mock it, so abstract it away. We mock it because the main thing we are testing are the tool calls without wasting resources on LLM calling

5. A single LLM response can contain multiple tool_use blocks in its content array, so we batch responses and send this back to the LLM rather than sending after every tool call. This is actually required by Anthropic, where if a response contains 3 tool_use blocks, your next message back to the API must contain all matching 3 tool_use blocks in a single user turn.

6. if LLM returns stop_reason saying it wants to use a tool, but the content is empty, this should return error

7. If there are multiple runs, only one registry is ever made, so calls will not reset to 0 and future tests will immediately pass

- Solution: Comment made (not fixed) that a fresh Registry should be built for every task. This is fine for now as phase 1 will not have multiple agent runs, this is a future phase's porblem, but good to catch and document right now. Would be overengineering to fix that problem right now.

8. Added Truncated flag when an LLM's output hits the max token limit (stop_reason=max_tokens) so caller can tell difference between finished and unfinished response
