# Durable Agent Execution Runtime — Development Workflow

Companion to `durable-agent-runtime-spec.md`. That file is the _what_. This file is the _how_ — the loop you run at every phase, and what "done" concretely looks like for each of the 10 phases.

---

## 0. One-time setup (do this before touching Phase 0 code)

**Create `CLAUDE.md` at the project root.** Don't run `/init` and accept it — you already have a full system spec, which is a better source than `/init`'s codebase-scanning guess. Write it by hand, short, covering:

- Project context (one paragraph, taken straight from your spec's Problem Statement)
- Architecture: where things live (`/cmd/worker`, `/cmd/controlplane`, `/internal/events`, `/internal/idempotency`, etc. — decide this in Phase 1 once code exists, then write it down)
- Code style: Go conventions, error handling style, how you name events
- Preferred libraries: `segmentio/kafka-go`, `prometheus/client_golang`, no new deps without a reason
- Commands: `docker-compose up`, how to run tests, how to run a single worker
- Critical rules: e.g. "never write to Postgres directly from a worker — only the projector writes to `events`" (this is exactly the kind of rule from your dual-write decision that belongs here, in caps-lock-worthy IMPORTANT form, because violating it silently breaks correctness)
- Keep it under 200 lines. Anything phase-specific doesn't belong here — it belongs in that phase's spec.

**Create one skill:** `.claude/skills/go-conventions/SKILL.md` — encodes your event-naming scheme, error-wrapping style, and JSON schema comment format. This is the thing from your original brief; build it now while conventions are still simple, and let it grow as you correct the same mistake twice (rule: "correct once, then codify" — the second time Claude Code gets your error-handling style wrong, that correction goes into this skill, not just into the chat).

**Create two subagents now, one later:**

- `.claude/agents/go-implementer.md` — tools: Read, Write, Edit, Bash, Glob, Grep. Model: Sonnet. Prompt: implement exactly what the approved plan says, nothing more, flag ambiguity instead of guessing.
- `.claude/agents/adversarial-reviewer.md` — tools: Read, Grep, Glob only (no write — it should never "helpfully" fix what it finds). Model: **Opus**, not Sonnet — this agent's entire job is catching subtle concurrency/idempotency bugs, which is exactly the kind of task where the extra reasoning depth earns its cost. Prompt: "You are reviewing Go code for race conditions, idempotency violations, and incorrect assumptions about delivery ordering. Assume the author missed something. For every side-effecting operation, verify there is a fencing check before it fires, not just eventually. Flag anything you cannot personally trace through by hand." Give it fresh context every time — never let it see the implementer's reasoning, only the diff.
- `.claude/agents/infra-helper.md` — create this when you hit Phase 6, not now. Tools: Read, Write, Edit, Bash. Model: Sonnet. Scope: Dockerfiles, K8s YAML, KEDA config only — keep it out of application logic entirely so it doesn't start "helpfully" touching Go code.

**Create the folder structure:**

```
.claude/specs/           → phase specs (phase2-event-journal.md, phase3-idempotency.md, etc.)
.claude/agents/          → the subagents above
.claude/skills/          → go-conventions
docs/decisions/          → YOUR decisions log, one file per phase, in your own words
```

`docs/decisions/` is deliberately separate from Claude's auto-generated `MEMORY.md`. MEMORY.md is Claude's operational memory of your codebase ("this project uses INR not USD" — implementation trivia). `docs/decisions/phaseN.md` is _your_ memory, written for a future interview: what the tradeoff was, what you chose, why, and what broke when you got it wrong the first time. Nobody else will write this for you, and it's the actual artifact that turns "I built a thing" into "I can explain a thing."

**Optional custom slash command:** `.claude/commands/verify-ac.md` — takes a phase number as `$ARGUMENTS`, reads that phase's acceptance criteria from the system spec, and walks through each one asking you to confirm pass/fail with evidence (a log line, a metric, a screenshot) rather than just saying "looks good." This turns your self-verification requirement into something repeatable instead of something you have to remember to do rigorously every time.

---

## The loop (repeat this at every phase)

1. **Sync and branch.** `git checkout main && git pull`, then `git checkout -b phaseN-<name>`.
2. **Spec, if this phase needs one (2, 3, 4, 6, 8).** Use the `spec-writer` skill again, scoped tight to just this phase, with the system spec as background context you paste in or reference. Save it to `.claude/specs/phaseN-<name>.md`. Review it yourself before moving on — this is the step that's genuinely yours, not Claude Code's; the point of a spec you write/review by hand is that you can't skip understanding the phase and still produce it.
3. **Concept tutoring pass — explicitly no code yet.** Open a Claude Code session and ask it to explain, in plain terms with your project's specifics as the example, whatever this phase introduces that you haven't used before (see the table below). Ask it to walk through at least one concrete failure scenario for the new concept, not just the happy-path definition. Do not let it write code in this step — if it starts generating a file, stop it. You're building the mental model that lets you review step 7 for real, not rubber-stamp it.
4. **Plan mode.** `/plan` (or shift+tab), pointed at the phase spec. For phases 2, 3, 4, and 6 specifically, use **high or max effort** — these are the phases where the plan quality genuinely changes the outcome, not just the code style. Consider `/ultraplan` for Phase 3 specifically, since idempotency-under-crash is the single hardest piece of the whole project and a bad plan there is expensive to unwind later.
5. **Push back on the plan before approving it.** At minimum ask: what happens if this fails halfway, what's the alternative approach you didn't pick and why, what's the concurrency-unsafe version of this that looks correct at first glance. If you can't personally restate the plan's core mechanism back in your own words, you're not ready to approve it — go back to step 3.
6. **Implement.** Hand the approved plan to `go-implementer`. For phases with genuinely independent pieces (e.g., Phase 6's Dockerfiles + K8s YAML + KEDA config), you can parallelize with `infra-helper` running alongside `go-implementer`, since they touch disjoint files.
7. **Manual diff review.** Read every line touching concurrency, fencing, or replay logic yourself, in full, before it merges — this is the one step your original principles explicitly forbid delegating. Boilerplate (Kafka client setup, generated protobuf code, YAML structure) you can skim.
8. **Adversarial review.** Run `adversarial-reviewer` against the diff, fresh context, no access to the implementation conversation. Treat every flag seriously even if it seems pedantic — a concurrency bug that "probably won't happen" is exactly the bug your chaos test in Phase 8 exists to find the hard way instead.
9. **Acceptance test, run by you, against real infra.** Not "Claude says the tests pass" — you personally run the phase's acceptance criteria from the system spec (or `/verify-ac N` if you built the slash command) and confirm each one with real evidence: a log line, a Grafana panel, a counter value. This is where phases 2, 3, and 8 in particular need you watching real output, not a summary.
10. **Update your docs.** Add anything you corrected twice to the `go-conventions` skill. Add any new critical rule to `CLAUDE.md`. Write `docs/decisions/phaseN.md` yourself — what the hard decision was, what you picked, what you'd tell an interviewer about the alternative you rejected.
11. **Ship it.** `git add -A && git commit`, push, open and merge the PR, delete the branch, `git checkout main && git pull`.

---

## When to ask for concept tutoring: the phase → concepts map

Ask for tutoring on these _before_ step 4 (plan mode) of the relevant phase, since plan mode quality depends on you actually understanding what you're approving:

| Phase | Concepts to have explained before coding                                                                                                                                                                                                                  |
| ----- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 0     | Docker networking basics (you know Docker; you likely don't know inter-container DNS via `docker-compose` service names)                                                                                                                                  |
| 1     | Minimal — this is your home turf (agent loop, tool-calling)                                                                                                                                                                                               |
| 2     | Event sourcing vs. traditional CRUD state; what "replay" actually reconstructs and why it's deterministic only if you're careful; Kafka topics/partitions/producers/consumers from first principles; why Postgres alone can't be the source of truth here |
| 3     | Distributed idempotency vs. simple retries; Redis `SET NX` semantics and why it's atomic; race conditions in check-then-act patterns; fail-open vs. fail-closed as a general concept (not just for Redis)                                                 |
| 4     | Kafka consumer groups and partition assignment; what a rebalance actually is and when it triggers; at-least-once vs. exactly-once delivery (and why "exactly-once" is mostly a marketing term); lease-based ownership and split-brain                     |
| 5     | gRPC vs. REST tradeoffs; what a `.proto` contract buys you; why binary serialization matters at scale (it won't matter at your scale — ask this explicitly so you can say so honestly in an interview instead of overclaiming)                            |
| 6     | Kubernetes fundamentals: pods, deployments, services, from zero; what KEDA actually watches and why lag beats CPU for a queue-driven system; what a `kind` cluster is standing in for                                                                     |
| 7     | Prometheus's pull-based scrape model vs. push-based metrics; counter vs. histogram vs. gauge — when to use which of the three you're building                                                                                                             |
| 8     | Chaos engineering methodology: steady-state hypothesis, blast radius, what actually counts as a valid experiment vs. a stunt                                                                                                                              |
| 9     | Minimal — this is mostly gluing familiar pieces together                                                                                                                                                                                                  |

---

## Phase-by-phase detail

### Phase 0 — Environment (2–3 days, no spec doc needed)

**Do:** Write `docker-compose.yml` for Kafka, Redis, Postgres. Write a throwaway Go script that produces/consumes one Kafka message and sets/gets one Redis key, purely to prove connectivity.
**Looks like at the end:** `docker-compose up` brings up all three with one command; a 30-line Go script proves you can talk to each of them. Nothing durable yet — this phase is plumbing.
**Acceptance:** AC-1 from the system spec.
**Decisions log entry:** Usually none — this phase is too mechanical to need one. Skip it if there's nothing worth writing.

### Phase 1 — Dumbest possible agent loop (2–3 days, no spec doc needed)

**Do:** Single Go process: call an LLM, get a tool decision, call a mocked tool, repeat until a terminal condition. No Kafka, no Redis, no Postgres involved yet.
**Looks like at the end:** One `main.go` you can run that completes a fake task end-to-end, printing each step to stdout. This is your baseline — you'll compare every later phase's added complexity against "it used to just be this."
**Acceptance:** AC-2.
**Decisions log entry:** Worth a short note on how you structured the loop's terminal-condition check, since Phase 2 will wrap this exact loop in durability.

### Phase 2 — Event journal (4–5 days, **needs a phase spec**)

**Do:** Write the phase spec first — it should nail down the exact event envelope and payload shapes (you already have a draft in the system spec; the phase spec should also decide your Go struct definitions, JSON marshaling approach, and the projector service's exact consume-then-insert loop). Wrap the Phase 1 loop so every step publishes an event to Kafka's `run-events` topic, with a separate projector consumer inserting into Postgres.
**Looks like at the end:** You can kill the process mid-run, restart it, and a `replay(run_id)` function reconstructs the exact correct state purely from stored events — no in-memory state survives, and you've proven it by actually killing the process, not by reasoning that it should work.
**Acceptance:** AC-3.
**Decisions log entry:** This is a mandatory one. Write down: why Kafka-first-then-project instead of Postgres-first (the dual-write reasoning from your system spec), and one concrete example you traced by hand of replay producing correct state after a mid-run kill.

### Phase 3 — Idempotent tool execution (3–4 days, **needs a phase spec** — do this one especially carefully)

**Do:** Phase spec must nail down the exact claim-before-invoke sequence (see EC-1 discussion), the Redis key schema, TTLs, and — critically — what happens when a recovering worker finds a stale `CLAIMED` key with no resolution (your reconciliation-check-per-tool decision belongs here). This is the phase where "I read the plan" isn't enough — trace the three-state (empty → claimed → resolved) sequence by hand against a concrete concurrent scenario before you approve the plan.
**Looks like at the end:** Manually replaying an already-completed step a second time proves, via a log line or counter, that the tool did not re-execute. You can also manually simulate a crash between claim and resolve, and demonstrate your reconciliation path fires instead of blindly re-invoking.
**Acceptance:** AC-4, EC-1, EC-2.
**Decisions log entry:** Mandatory, and probably your longest one. This is the "actual point of the project" phase — write it as if explaining to an interviewer why check-then-act alone doesn't work and what closes the gap.

### Phase 4 — Kafka-based worker pool (4–5 days, **needs a phase spec**)

**Do:** Phase spec should decide: exact consumer group configuration, how a worker acquires/renews the Redis lease per run, and precisely what happens during a rebalance (per EC-3 and EC-12 — this needs to be the same code path as crash recovery, decide that explicitly in the spec, not discover it while coding). Add the approval-gate release/resume flow here too.
**Looks like at the end:** 3 worker processes, 10 runs submitted, work visibly distributed across all three with no run double-processed — verified by logs showing which worker handled which run, not assumed. A run parked in `AWAITING_APPROVAL` releases its worker fully (no thread/connection held) and resumes on any available worker after approval.
**Acceptance:** AC-5, AC-6, EC-3, EC-12.
**Decisions log entry:** Mandatory — the lease vs. fencing-key distinction (EC-12) is a genuinely subtle point worth being able to explain: why a lease alone isn't sufficient and the fencing check is what actually prevents split-brain execution.

### Phase 5 — gRPC control plane (2–3 days, no spec doc needed — the system spec's API Contracts section already fully defines this)

**Do:** Write the `.proto` from the system spec's contract, generate Go code, implement server-side in the control plane, client-side in a CLI.
**Looks like at the end:** A CLI that calls `SubmitRun`, `GetRunStatus`, `CancelRun`, `ApproveRun` purely over gRPC and gets correct responses, including a proper `NOT_FOUND` for an unknown run.
**Acceptance:** AC-7, EC-8.
**Decisions log entry:** Optional/short — mostly boilerplate, unless you hit something non-obvious in codegen.

### Phase 6 — Kubernetes + KEDA (4–6 days, **needs a phase spec**, hardest phase operationally)

**Do:** Phase spec should decide exact KEDA scaling thresholds (what lag triggers a scale-up, what the cooldown is before scale-down), resource requests/limits per pod, and how the chaos test in Phase 8 will actually target pods (random selection strategy). Create `infra-helper` now if you haven't. Containerize everything, deploy to `kind`.
**Looks like at the end:** Submitting a burst of runs visibly spins up more worker pods (watch it with `kubectl get pods -w`), and pod count drops once the queue drains.
**Acceptance:** AC-8, AC-9, EC-10.
**Decisions log entry:** Worth a note on why lag-based scaling beats CPU-based here, backed by what you actually observed, not just the theory.

### Phase 7 — Observability (2 days, no spec doc needed)

**Do:** Expose the four metrics from the system spec on `/metrics`, wire up Prometheus scraping, build a Grafana dashboard with throughput, latency percentiles, and the duplicate-call counter as its own panel.
**Looks like at the end:** A dashboard you can watch live while load runs — this becomes the visual you screenshot for your resume/portfolio.
**Acceptance:** AC-10.
**Decisions log entry:** Optional.

### Phase 8 — The chaos test (2–3 days, **needs a phase spec** — your methodology needs to be locked down before you write the script, not discovered afterward)

**Do:** Phase spec should decide: what N (concurrent runs) you're actually targeting and why, what pod-kill interval/randomness strategy counts as a fair test, and exactly what "zero duplicated side effects" means operationally (how you'll detect a duplicate after the fact — a log grep? a counter? the external system's own state?). This is where sloppy methodology quietly invalidates your headline number.
**Looks like at the end:** A script you can run that submits load, kills workers on a timer, and at the end reports: every run terminal, zero duplicates (with the actual evidence, not just "it said zero"), and a real resume-time distribution — recorded, not estimated.
**Acceptance:** AC-11, EC-11.
**Decisions log entry:** Mandatory — this produces your actual resume metric. Write down the real numbers and the exact conditions they were measured under, so you can defend them if asked "what does p99 recovery time under chaos actually mean here."

### Phase 9 — Demo agent (2–3 days, can overlap earlier phases, no separate spec doc — but read the idempotency note below before building the `open_pr` tool)

**Do:** Wire in the real repo-maintenance agent against a real LLM API. Critically: build the GitHub-side reconciliation check into the `open_pr` tool itself (query "does a PR from this branch already exist" before creating one) — this is the Option-1 reconciliation strategy from the Phase 3 discussion, applied concretely, and it belongs in this phase's implementation even though there's no separate spec for it.
**Looks like at the end:** End-to-end: clone → test → fix → PR, and killing its worker mid-task resumes without a duplicate PR — verified by actually killing it mid-task and checking GitHub, not by inspection.
**Acceptance:** AC-12, EC-1 (closed via reconciliation).
**Decisions log entry:** Short note on how the reconciliation check works and why it was necessary even with the Redis fencing key in place.

---

## Final note on pacing

Don't let the concept-tutoring step turn into a stall. The rule from your own stated principles is a good one to hold yourself to literally: if you can trace the mechanism by hand with a concrete concurrent example, you're ready to approve the plan. If you can't, that's the signal to ask one more specific question — not to re-read a general explanation a second time.
