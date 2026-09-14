---
description: Walk through a phase's acceptance criteria from the system spec, demanding real evidence for each one rather than accepting "looks good"
argument-hint: "<phase-number>"
allowed-tools: Read, Bash
---

Parse `$ARGUMENTS` as a phase number. If it's missing or not a number 0–9, stop and print an error asking for a valid phase number.

Read `.claude/specs/system-spec.md` and find every acceptance criterion (AC-N) tagged to that phase in the Acceptance Criteria section.

For each one, in order:
1. State the criterion plainly.
2. Ask Sahil to provide evidence it's actually met — a specific log line, a metric value, a command's real output, or a screenshot description. Do not accept a bare "yes" or "it works" as evidence.
3. If the evidence doesn't clearly support the criterion, mark it as not yet met and say specifically what's missing — don't soften this to make the phase look more done than it is.

At the end, print a summary table: criterion, met/not met, evidence given. If anything is unmet, list it clearly as what's left before this phase can be considered done.
