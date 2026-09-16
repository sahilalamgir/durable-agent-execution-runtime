---
name: spec-writer
description: Turns a rough coding project or feature description into a structured technical spec with a Problem Statement, Functional Requirements, API Contracts, Constraints, Edge Cases & Error Handling, and Acceptance Criteria. Use this whenever the user describes a software project, feature, service, or system they're about to build and wants it formalized — including requests to "spec this out," "write a spec/PRD/design doc," "turn this into requirements," or "help me plan this feature before I code it." Trigger even if they don't say the word "spec" explicitly — a rough paragraph describing what they want to build, followed by "can you formalize this" or "what would the requirements look like," counts too. Don't use this for specs on non-software projects, or when the user just wants brainstorming/ideation rather than a locked-down spec document.
---

# Spec Writer

Turns a project or feature description into an engineering-ready spec: something a developer (possibly the user themselves, a few weeks from now with no memory of this conversation) could pick up and build from without having to ping anyone for clarification.

## Why this structure

Each section of the spec answers a different question a developer will ask when they sit down to build the thing:

- **Problem statement** — why does this need to exist at all?
- **Functional requirements** — what exactly does it need to do?
- **API contracts** — what are the actual shapes of data going in and out?
- **Constraints** — what boundaries (performance, tech stack, timeline, compliance) am I building inside of?
- **Edge cases & error handling** — what happens when things go wrong?
- **Acceptance criteria** — how do I know when I'm done?

A spec missing any of these tends to produce the same failure mode: someone builds a working happy path, then discovers scope, error handling, or "what does the response actually look like" questions midway through, and has to backtrack. The goal here is to force those questions to the surface before code gets written.

## Process

1. **Read the description closely.** Note what's explicitly stated vs. what you'd have to guess to fill in a section.

2. **Ask before guessing, but only for things that would materially change the spec.** If the user's description doesn't tell you enough to write real API contracts (e.g., you don't know if this is a REST service, a library function, a CLI, or an internal class) or leaves a genuinely load-bearing constraint unstated (e.g., expected scale, whether this needs to be concurrent-safe, what the existing stack is), ask 1-3 short, specific questions. Don't ask about things you can make a reasonable, clearly-labeled engineering assumption about instead — that just slows things down. When you do assume something non-obvious, say so inline in the spec (a short italic note is enough) rather than silently presenting a guess as a given fact.

3. **Write the spec using the template below, in that exact order.** Keep language concrete and testable. Avoid unquantified adjectives like "fast," "scalable," or "robust" — either attach a number/condition to them (e.g., "p95 latency under 200ms") or leave them out.

4. **Save it as a markdown file and present it.** Use a filename like `<project-slug>-spec.md`. This is a working document meant to be committed to a repo, pasted into a ticket, or handed to a teammate — treat it as a deliverable, not a chat reply.

## Spec template

Use this exact structure and heading order:

````markdown
# [Project/Feature Name] — Spec

## Problem Statement

[2-5 sentences. What's broken, missing, or costly right now? Who feels this pain?
What happens if this doesn't get built? Avoid describing the solution here — this
section is about the "why," not the "what."]

## Functional Requirements

FR-1: [The system must/shall...]
FR-2: ...
[Numbered, one discrete capability per line. Each one should be independently
verifiable — if you can't picture writing a test for it, it's too vague.]

## API Contracts

### `functionOrEndpointName(...)`

**Input:**

```json
{ "field": "type — description" }
```
````

**Output:**

```json
{ "field": "type — description" }
```

**Notes:** [error responses, status codes, auth requirements, pagination, etc.]

[Repeat per endpoint/function/method. If this isn't a networked API, use function
signatures, class interfaces, or CLI arguments instead — same level of specificity:
exact input types, exact output types, exact shape.]

## Constraints

- **Technical:** [language/framework/infra requirements or restrictions]
- **Performance & scale:** [expected load, latency budgets, data volume]
- **Timeline/resourcing:** [if relevant]
- **Security/compliance:** [if relevant]
  [Omit sub-bullets that don't apply rather than forcing content into them.]

## Edge Cases & Error Handling

EC-1: [Scenario] → [Expected behavior, error code/message, fallback, or retry policy]
EC-2: ...
[Think about: empty/null inputs, boundary values, concurrent access, partial
failures, network/dependency failures, malformed input, auth/permission failures,
and what happens on retry.]

## Acceptance Criteria

- [ ] AC-1: [A specific, checkable condition — "given X, when Y, then Z" works well]
- [ ] AC-2: ...
      [This is the checklist someone runs through to decide whether the work is done.
      Every functional requirement and edge case above should be traceable to at least
      one acceptance criterion.]

```

## Section-by-section guidance

**Problem statement:** Resist the urge to jump straight to the solution. If the user's original description was solution-first ("build a Redis cache in front of the user service"), work backward to name the actual problem ("user profile lookups are hitting Postgres on every request and p95 latency is spiking under load") — ask if it's not stated.

**Functional requirements:** Number them (FR-1, FR-2, ...) so they can be referenced later in code review or ticket comments. Split compound requirements into separate lines. Order roughly by priority or by user flow, whichever makes the list easier to read.

**API contracts:** This is the section most likely to be underspecified in a rough description — push for real detail here. Give concrete types (not just "a user object" — spell out the fields and their types). Include at least one realistic example payload per contract, not just a schema. If the project has no external API (e.g., a batch script, a data pipeline), define the contract as function signatures or the shape of intermediate data instead — don't skip the section.

**Constraints:** These bound the solution space. Include only what's real — invented constraints are worse than no constraints, since they'll misdirect an implementer.

**Edge cases & error handling:** Number these too (EC-1, EC-2, ...). For each, state the triggering condition and the exact expected system behavior — not just "handle gracefully." If the description mentions external dependencies (a third-party API, a database, another service), always include at least one edge case for that dependency being slow, down, or returning malformed data.

**Acceptance criteria:** Write these as checkboxes so they can be pasted straight into a GitHub issue or PR description and ticked off. Every functional requirement and every edge case should map to at least one acceptance criterion — if something in FR or EC has no corresponding AC, either add one or reconsider whether that requirement was real.

## Example

**User description (input):**
> I want to build a rate limiter for my API. Should support per-user limits, return a 429 when exceeded, and needs to work across multiple server instances.

**What a good spec does with this:**
- Problem statement names the actual risk (abuse/cost from unbounded requests, or a downstream dependency that will fall over) rather than just restating "we need a rate limiter."
- Functional requirements break "per-user limits" into specifics: is the limit configurable per user or global? What's the default? Fixed window, sliding window, or token bucket? (Ask if not specified — this changes the whole implementation.)
- API contracts define the actual interface: is this a middleware function, a decorator, a standalone service with its own endpoint? What does the 429 response body look like?
- Constraints capture "works across multiple instances" as a real constraint: this requires a shared state store (Redis, etc.), which has its own latency and failure-mode implications — surface that.
- Edge cases cover what happens when the shared store itself is unreachable (fail open vs. fail closed — this is a real product decision, don't default silently).
- Acceptance criteria are checkable: "AC-3: When a user exceeds their limit, a subsequent request within the window returns HTTP 429 with a `Retry-After` header."

## Output format

Default to a markdown file (`<project-slug>-spec.md`) saved and presented as a downloadable file, since this is meant to be reused outside the chat. If the user is clearly just thinking out loud and wants a quick look before committing to anything ("give me a rough sense of what the spec would look like"), it's fine to write it inline in the chat instead — use judgment based on how the user is framing the request.
```
