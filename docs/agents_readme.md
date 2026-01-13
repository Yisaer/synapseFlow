# veloFlux Agent Runtime Playbook (Developer-Facing)

This document is developer-facing operational guidance: what rules and workflow should be embedded into the agent implementation. The runtime agent (CLI process) should not depend on reading repository docs; it should obtain all facts via runtime APIs/tools.

Design/background details live in `docs/agents.md`.

## Grounding Rules

Agents must not invent:

- stream names
- column names
- column types
- function names

All of the above must be grounded via runtime catalog introspection and capability catalogs.

## Function Workflow

Before generating SQL (and when fixing validation errors):

1) Fetch `GET /functions`.
2) For a specific function, fetch `GET /functions/describe/:name` (or resolve by alias).
3) Use `FunctionDef` to constrain generation:
   - `kind`: scalar vs aggregate vs stateful
   - `signature`: argument count and type expectations
   - `allowed_contexts`: which query contexts are allowed (e.g. `SELECT`, `WHERE`)
   - `requirements`: additional semantic requirements (e.g. aggregation context, deterministic order)

Agents must not guess function existence, arity, or meaning.

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

## Syntax Workflow

Before generating SQL (and when fixing validation errors):

1) Fetch `GET /capabilities/syntax`.
2) Treat the returned `constructs` tree as the source of truth for supported syntax.
3) Treat constructs not present in the catalog as unsupported by default.
4) Use agent-friendly fields to guide generation:
   - `purpose` / `semantics` to decide when to use a construct
   - `placement` to decide where it can appear (e.g. windows declared in `GROUP BY`)
   - `syntax` / `examples` as templates
   - `constraints` / `workarounds` to stay within the supported subset

## SQL Generation Workflow (Recommended)

1) Build an intent sketch (project/filter/aggregate/sort/limit…).
2) Fetch syntax metadata via `GET /capabilities/syntax`.
3) Fetch function metadata via `GET /functions` (and `GET /functions/describe/:name` when needed).
4) Render SQL using only supported syntax constructs and functions.
4) Run `validate_sql(sql)` and iterate until it passes.
5) Run `explain_sql(sql)` and verify the plan matches intent (node kinds and ordering).
6) Output:
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
- Treat `GET /functions` as the source of truth for function availability and constraints.
- Prefer stable SQL patterns and the supported syntax subset.
- Use `validate_sql` errors as structured feedback to self-correct.

Don’t:

- Assume nested field access syntax exists just because the schema contains `struct`/`list`.
- “Make up” a function or a stream/column name that isn’t in the catalogs.
- Provide unverified SQL when `validate_sql` or `explain_sql` is available.
