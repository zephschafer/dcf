You are helping the user define a new ddt feature. Your job is to ask enough questions to produce a precise, useful feature description — not to write from the first sentence. Requirements gathering comes before writing.

---

## Step 1: Read existing features

Read `features/FEATURES.md` to see what features already exist. If the user's request substantially overlaps with an existing feature, surface it before proceeding — they may want to extend an existing file rather than create a new one.

Also read `features/README.md` so you understand the format and intent.

---

## Step 2: Requirements gathering (interactive — do not skip)

Based on what the user has told you, identify what you still need to know before you can write a precise feature description. Ask 3–5 targeted questions and **wait for responses before proceeding to Step 3**.

Do not ask all questions at once if the user already answered some. Adapt to what they've said. The goal is to fill in these unknowns:

- **Who is the primary user?** Developer setting up pipelines? Data analyst querying the warehouse? Operator managing infrastructure?
- **What problem does this solve?** What is currently broken, missing, or too painful to do? What is the workaround today?
- **What does success look like?** How would you demonstrate this works — with a CLI run, a query, a config change?
- **What is explicitly out of scope?** What would you NOT want this feature to do or touch?
- **Are there related test scenarios?** Are there existing scenarios in `testing/scenarios/` that touch this area? Would new scenarios be needed?

Keep the conversation focused. If you can answer a question by reading the codebase (Step 3), do that instead of asking.

---

## Step 3: Probe the codebase (when the feature touches existing code)

Before drafting, read the relevant source files to understand constraints and existing patterns. Key locations:

- `ddt/config/models.py` — YAML schema (Pipeline, Source, Auth, Build, Column types)
- `ddt/engine/runner.py` — pipeline execution loop
- `ddt/engine/fetcher.py` — HTTP and Python source fetch logic
- `ddt/engine/iterator.py` — iterate axis expansion
- `ddt/engine/projector.py` — schema projection
- `ddt/writer/iceberg.py` — write strategies (incremental, append, full_refresh, GCS)
- `ddt/warehouse_reader.py` — query path (local and GCS)
- `ddt/mcp_server.py` — MCP tool surface
- `ddt/cli.py` — CLI commands
- `ddt/gcp/` — GCP provisioning

Note any patterns the feature would reuse, and any constraints (e.g. "this would require a new field in models.py").

---

## Step 4: Draft the feature description

Choose a slug: short, lowercase, hyphenated. Examples: `gcp-data-lake`, `incremental-retry`, `rate-limit-resilience`.

Write `features/<slug>.md` with this structure:

```markdown
# Feature: <Name>

**Status:** Draft
**ID:** <slug>
**Created:** YYYY-MM-DD
**Updated:** YYYY-MM-DD

## Summary
[One paragraph. What this feature does and why it matters.
Write for both humans (product communication) and LLMs (development context).
Explain the value, not the implementation.]

## Problem
[What gap or pain does this address? What is impossible or too painful without this feature?
What do users do today as a workaround, and why is that insufficient?]

## User Story
As a [role], I want to [action], so that [outcome].

## Requirements

### Must Have
- [Specific, testable requirement — behavior, not implementation detail]
- ...

### Nice to Have
- [Lower-priority requirement — acceptable to defer to a follow-on]
- ...

## Acceptance Criteria
- [ ] [Testable condition — specific enough to verify with a CLI run, query result, or unit test]
- [ ] ...

## Out of Scope
- [Explicit non-goal — prevents scope creep and sets clear boundaries]
- ...

## Related Scenarios
- [`testing/scenarios/<name>.md`](../testing/scenarios/<name>.md) — [what aspect it covers]
<!-- Add TODO items for scenarios that don't yet exist but would be needed -->

## Design Notes
[Implementation constraints, relevant code paths, open questions.
Primarily for LLMs and engineers. Include file paths and function names where known.
Example: "Would require a new `retry` field on `Build` in `ddt/config/models.py`".]
```

---

## Step 5: Update FEATURES.md

Add a row to the Features table in `features/FEATURES.md`:

```markdown
| <slug> | <Name> | Draft | <one-line summary> | <scenario name(s) or —> |
```

Update the header counts (Total, Draft).

---

## Step 6: Cross-link with test scenarios

Check whether any files in `testing/scenarios/` cover this feature area. If so, add them to the "Related Scenarios" section of the feature file.

If new scenarios would be needed to fully test this feature, add a `TODO:` note in the Design Notes:

```markdown
<!-- TODO: create testing/scenarios/<slug>.md to cover acceptance criteria X, Y -->
```

---

## Step 7: Present to the user

Tell the user:
- The feature file path (`features/<slug>.md`)
- Which test scenarios were linked (or need to be created)
- Whether any related features already exist that they should review

Ask: is there anything to revise in the feature description? Iterate until they are satisfied.

---

## Step 8: Offer scenario creation

Once the feature description is finalized, ask:

> Would you like to create a test scenario for this feature now? This would produce `testing/scenarios/<slug>.md` with the acceptance criteria mapped to success criteria checkboxes, and cross-link it back to the feature file.

If **yes**: proceed through the scenario creation workflow inline. You have all the context needed — feature slug, acceptance criteria, design notes, and any credentials mentioned. Ask only the questions that are still unanswered:

- What is the target API or component to test against?
- Should this be one scenario (happy path) or multiple (also covering error cases)?
- Are there any pre-identified failure modes to call out?

Then write `testing/scenarios/<slug>.md` following the same structure as files in `testing/scenarios/` (see `python-connector.md` for a complex example). Update the feature file's "Related Scenarios" section to link to it. If the scenario requires a credential not in `testing/test_config.yml.example`, flag it for the user.

If **no**: tell the user they can run `/new-scenario <slug>` later to create the scenario. The feature file's Design Notes already has any `TODO:` items marking gaps.

---

## Rules

**Do not write the feature description until you have enough information from Step 2.** A feature description written from the first sentence without requirements gathering will be vague and require heavy revision.

**Requirements, not solutions.** The feature description describes *what* the feature does for the user, not *how* it is implemented. Design Notes may discuss implementation, but Requirements and Acceptance Criteria must stay behavior-focused.

**Acceptance criteria must be testable.** Each criterion should be specific enough that someone could run a command or write a unit test to verify it. Vague criteria ("the feature works correctly") are not acceptable.

**One feature per file.** If the user's request spans multiple independent capabilities, suggest splitting them into separate features and confirm with the user before writing.
