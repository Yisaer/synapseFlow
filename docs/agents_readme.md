# SynapseFlow Agent Runtime Playbook (Developer-Facing)

This document is developer-facing operational guidance: what rules and workflow should be embedded into the agent implementation. The runtime agent (CLI process) should not depend on reading repository docs; it should obtain all facts via runtime APIs/tools.

Design/background details live in `docs/agents.md`.

## Grounding Rules

Agents must not invent:

- stream names
- column names
- column types
- function names

All of the above must be grounded via runtime catalog introspection and capability catalogs.

## Stream & Schema Workflow

When a user request references a stream (e.g. “user stream”) or columns (e.g. “column `a`”):

1) `GET /streams` to resolve/confirm the stream name (and handle ambiguity).
2) `GET /streams/describe/:name` to fetch the authoritative schema.
3) Verify:
   - required columns exist
   - column types support requested operations (or that an explicit cast is supported)

If any of these checks fail:

- ask a clarifying question, or
- propose a fix with suggestions (e.g. closest column names), but do not guess.

## SQL Generation Workflow (Recommended)

1) Build an intent sketch (project/filter/aggregate/sort/limit…).
2) Render SQL using only supported syntax/features/functions.
3) Run `validate_sql(sql)` and iterate until it passes.
4) Run `explain_sql(sql)` and verify the plan matches intent (node kinds and ordering).
5) Output:
   - SQL
   - short explanation of key clauses/functions
   - an explain summary (pretty string or key highlights)

Stop and clarify when:

- the stream is missing or ambiguous
- required columns are missing
- the requested feature is unsupported
- type rules make the request invalid

## Do / Don’t

Do:

- Treat `GET /streams/describe/:name` as the source of truth for schema.
- Prefer stable SQL patterns and the supported syntax subset.
- Use `validate_sql` errors as structured feedback to self-correct.

Don’t:

- Assume nested field access syntax exists just because the schema contains `struct`/`list`.
- “Make up” a function or a stream/column name that isn’t in the catalogs.
- Provide unverified SQL when `validate_sql` or `explain_sql` is available.
