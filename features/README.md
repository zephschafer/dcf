# ddt Features

This directory contains feature descriptions for ddt. Each file defines one feature: what it does, why it exists, and what "done" looks like.

## Who reads these

**Humans** — feature files serve as product documentation. The Summary and Problem sections explain the feature in plain language. The User Story and Acceptance Criteria make the scope concrete.

**LLMs** — feature files are structured context for Claude when building, reviewing, or extending a feature. The Design Notes section includes code references (file paths, function names) so Claude can orient without reading the full codebase.

## How to create a new feature

Run `/new-feature` in Claude Code. The skill will ask you questions to gather requirements, then write the feature file and update the index.

## File structure

```
features/
├── README.md        — this file
├── FEATURES.md      — index of all features with status and links
└── <slug>.md        — one file per feature (e.g. gcp-data-lake.md)
```

Each feature file contains:
- **Summary** — one paragraph, what the feature does and why it matters
- **Problem** — the gap or pain this addresses
- **User Story** — as a [role], I want [action], so that [outcome]
- **Requirements** — must-have and nice-to-have, each testable
- **Acceptance Criteria** — the checklist that defines done
- **Out of Scope** — explicit non-goals
- **Related Scenarios** — links to `testing/scenarios/` files that test this feature
- **Design Notes** — constraints, open questions, relevant code paths

## How features link to testing

Feature files reference test scenarios in their "Related Scenarios" section:

```markdown
## Related Scenarios
- [`testing/scenarios/gcp-data-lake.md`](../testing/scenarios/gcp-data-lake.md) — end-to-end GCS write and query path
```

Scenario files can reference feature files in their Notes section. This creates traceability:

```
Feature description → Test scenario → Test findings → Code fixes
```

## Status meanings

| Status | Meaning |
|--------|---------|
| **Draft** | Requirements being gathered; not yet approved for development |
| **Active** | In development or recently shipped; acceptance criteria not yet all passing |
| **Complete** | Implemented, tested, and all acceptance criteria verified |
