# Feature Coverage Registry

This directory contains the machine-readable feature registry used by the test
coverage tooling.

## Purpose

- `docs/` remains the source of feature semantics and design rationale.
- `tests/docs/coverage/` defines the stable feature IDs used by tests and
  coverage evaluation.
- Runtime coverage calculation should read these YAML files instead of parsing
  Markdown headings directly.
- This registry is not API schema coverage. Do not add feature IDs only to track
  manager REST field names or DTO shape stability.
- Interaction coverage is declared separately from single-feature coverage and
  is derived from existing test `covers` annotations.

## Layout

- `features/parser.yaml`
- `features/planner.yaml`
- `features/processor.yaml`
- `features/pipeline.yaml`
- `features/decoder.yaml`
- `features/source.yaml`
- `features/sink.yaml`
- `features/stream.yaml`

Each file owns a single top-level feature domain.

Interaction registry files live in:

- `interactions/planner.yaml`
- `interactions/runtime.yaml`
- `interactions/sink.yaml`

Interaction files are also split by domain, but interaction domains may describe
cross-domain behavior and are not limited to the single-feature domain list.

## Naming Rules

Feature IDs use dot-separated lowercase segments:

- Required format: `<domain>.<capability>.<subject>[.<variant>]`
- The first segment must be one of:
  - `parser`
  - `planner`
  - `processor`
  - `pipeline`
  - `decoder`
  - `source`
  - `sink`
  - `stream`
- IDs describe stable observable behavior, not implementation details.
- IDs should use noun-style phrases instead of function names or rule numbers.
- Single-feature IDs must not encode multi-feature combinations.

Examples:

- `parser.select.where_clause`
- `planner.logical.predicate_pushdown`
- `processor.barrier.alignment`
- `source.memory.collection_input`

## Field Semantics

Each feature entry uses the following minimal schema:

- `id`: Stable feature identifier.
- `title`: Human-readable title.
- `summary`: Short statement of the behavior being covered.
- `doc_refs`: Design or behavior documents that justify the feature.
- `status`: Current lifecycle state. Use `active` unless the feature is retired.

Each interaction entry uses the following minimal schema:

- `id`: Stable interaction identifier.
- `title`: Human-readable title.
- `summary`: Short statement of the combined behavior being covered.
- `features`: The single-feature IDs that must be covered by one test unit or
  testcase for this interaction to count as covered.
- `status`: Current lifecycle state.
  - `active`: The interaction must be covered by one same-record coverage
    annotation and participates in coverage reporting.
  - `cross_module`: The interaction remains in the registry as an intentional
    cross-feature target because its evidence is expected to stay split across
    planner and pipeline/runtime or similar specialized suites. It does not
    count as uncovered same-record coverage debt.
  - `retired`: The interaction is no longer part of the active coverage target
    set.

Interaction coverage uses superset matching. A test record whose `covers` list
contains all interaction `features` covers that interaction, even if the test
record also covers additional features.

## Maintenance Rules

- Add or update a feature here when a new documented capability becomes part of
  the supported test coverage space.
- Add or update an interaction when an important documented behavior depends on
  multiple features being validated by the same test unit or testcase.
- Use `cross_module` for interactions that should stay visible in the registry
  when planner-side and runtime-side evidence is intentionally split across
  different suites.
- Keep pure manager/control-plane REST DTO contracts in API docs and manager API
  tests, unless the endpoint itself is the documented surface for a runtime
  feature.
- Keep `doc_refs` aligned with the current design documents.
- Prefer adding new IDs over renaming existing ones. If a rename is required,
  handle it as an explicit migration in follow-up tooling instead of silently
  changing the old ID.
- Keep feature granularity at the "independently assertable behavior" level.
  Do not encode case variants such as `null`, `alias`, or `empty` directly in
  the feature ID during the single-feature coverage phase.
