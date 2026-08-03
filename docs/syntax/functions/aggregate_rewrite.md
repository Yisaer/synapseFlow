# Aggregate Rewrite

## Background

veloFlux rewrites aggregate calls before logical planning. The parser extracts aggregate functions
from SQL-visible expressions, assigns internal placeholder columns such as `col_1`, and preserves a
mapping from each placeholder back to the original aggregate expression.

This rewrite is the contract between parser output and aggregation planning:

- the parser normalizes repeated aggregate references across clauses
- logical planning validates aggregate queries against placeholders instead of raw function calls
- physical planning compiles the stored aggregate expressions into executable aggregate calls

The related stateful rewrite happens earlier in the parse pipeline. See
[`stateful_functions.md`](../language/stateful_functions.md) for the stateful-specific model.

## Goals

- Define which clauses participate in aggregate extraction.
- Document how placeholder allocation and reuse work.
- Explain how rewritten select-field names remain user-facing.
- Capture the planner assumptions that depend on `SelectStmt.aggregate_mappings`.

## Non-Goals

- Listing every built-in aggregate signature.
- Documenting window semantics in general.
- Defining future optimizer rewrites beyond the current parser placeholder model.

## Input SQL Shape

Aggregate rewrite applies after raw `SELECT` parsing and after stateful-function rewrite.
The input is a `SelectStmt` that already contains:

- `select_fields`
- optional `having`
- `order_by`
- `group_by_exprs`
- optional `window`
- any `stateful_mappings` extracted earlier

The parser intentionally runs stateful rewrite first so expressions such as
`last_row(lag(a))` become aggregate-over-placeholder instead of aggregate-over-stateful-AST.

## Rewrite Scope

The aggregate transformer rewrites aggregate calls in exactly three places:

- `SELECT` expressions
- `HAVING`
- `ORDER BY`

It does not rewrite `WHERE`. Row-level filtering remains outside aggregate semantics.

For each participating expression tree, the transformer:

1. walks the AST and detects aggregate calls using the aggregate registry
2. assigns or reuses placeholder names
3. replaces aggregate subexpressions with identifier references to those placeholders
4. stores new placeholder-to-original-expression pairs in `SelectStmt.aggregate_mappings`

## Placeholder Allocation

Parser placeholders use the reserved `col_<n>` shape, allocated by a shared
`ColPlaceholderAllocator`.

Important properties:

- the same allocator instance is shared by stateful rewrite and aggregate rewrite
- placeholder names are therefore unique across both passes
- downstream expression conversion treats `col_<n>` as an internal derived column, not as a source
  schema column

This makes placeholders safe to use in later validation and planning without colliding with normal
user columns.

## Deduplication Rules

Repeated identical aggregate expressions reuse one placeholder. Deduplication is based on the debug
string of the original aggregate AST (`format!("{:?}", expr)`), not on a separately normalized SQL
string.

Implications of the current design:

- repeated `sum(a)` in different clauses usually shares one placeholder
- deduplication is structural and parser-shape-dependent
- `SelectStmt.aggregate_mappings` is a `HashMap`, so consumers must treat it as a set of named
  aggregate definitions rather than as an ordered list

Example:

```sql
SELECT sum(a) + 1
FROM t
GROUP BY b
HAVING sum(c) > 0
ORDER BY sum(a)
```

is normalized into the equivalent placeholder form:

```sql
SELECT col_1 + 1
FROM t
GROUP BY b
HAVING col_2 > 0
ORDER BY col_1
```

with mappings conceptually equivalent to:

- `col_1 -> sum(a)`
- `col_2 -> sum(c)`

## Clause Behavior

### `SELECT`

Aggregate calls inside projection expressions are replaced in place. The overall expression shape is
preserved, so `sum(a) + 1` becomes `col_1 + 1` instead of collapsing to a plain aggregate field.

### `HAVING`

`HAVING` is rewritten after projection fields. Aggregates referenced only in `HAVING` still receive
placeholder mappings and become part of the aggregate stage even if they are not projected.

`last_agg_hit_count()` is not an aggregate function and is not extracted into
`SelectStmt.aggregate_mappings`. It may appear in `HAVING` as a pipeline state function. Aggregate
calls in the same `HAVING` expression are still rewritten normally:

```sql
HAVING sum(a) > 10 AND last_agg_hit_count() < 3
```

becomes:

```sql
HAVING col_1 > 10 AND last_agg_hit_count() < 3
```

### `ORDER BY`

`ORDER BY` participates in the same deduplication set as `SELECT` and `HAVING`. An aggregate that
already appeared earlier should reuse the existing placeholder rather than allocate a new one.

## Field Name Preservation

When a projection expression is rewritten and the user did not provide an explicit alias, the parser
copies the original `field_name` into `field.alias`.

This preserves the user-facing projected name after placeholder replacement. Without this step,
`SELECT sum(a) + 1` would expose a rewritten expression name such as `col_1 + 1`, which is an
internal implementation detail.

Explicit user aliases always win and are not replaced by the parser.

## Interaction With Stateful Rewrite

The parser runs stateful rewrite first, then aggregate rewrite, using one shared allocator.

This ordering matters:

- aggregates may consume already-rewritten stateful placeholders
- aggregate placeholders can never collide with stateful placeholders
- aggregate queries can be validated against a single placeholder namespace

Stateful validation explicitly rejects aggregate functions inside a single stateful function's
argument, filter, or partition context. The supported composition is therefore:

- stateful call inside a larger expression, rewritten first
- aggregate call over that rewritten expression, rewritten second

Unsupported composition is rejected before aggregate planning:

- aggregate inside a stateful call context
- window functions inside a stateful call context
- nested unresolved stateful calls inside a single stateful call context

## Planner Expectations

Logical and physical planning rely on the rewrite contract in several ways.

Logical validation assumes:

- `GROUP BY` without aggregates is rejected
- `HAVING` requires a windowed aggregation context
- `HAVING` must reference aggregate placeholders or `last_agg_hit_count()`, and must not reference
  raw non-aggregate columns
- in aggregate queries, `SELECT` and `ORDER BY` expressions must either be aggregate-derived or
  appear in `GROUP BY`
- when aggregation is present, `ORDER BY` must not reference stateful placeholders

Physical aggregation assumes:

- every entry in `aggregate_mappings` is an aggregate function expression
- placeholder names are the output columns of aggregate calls
- aggregate arguments are still SQL expressions that can be compiled into scalar expressions

Current negative boundaries in physical compilation are part of the contract:

- qualified wildcard aggregate arguments are not supported
- named aggregate arguments are not supported
- `distinct` is preserved in metadata, but runtime support still depends on the concrete aggregate
  implementation

## Failure Semantics

Failures should happen before runtime execution whenever possible.

Common failure classes:

- unsupported aggregate placement is rejected by logical validation
- malformed stateful-plus-aggregate nesting is rejected during stateful validation
- unsupported aggregate argument shapes fail during physical compilation

The parser rewrite itself is not a semantic approval step. It only normalizes aggregate references
for later validation.

## Testing Guidance

Cover these dimensions when adding parser or planner tests:

- repeated identical aggregate expressions across `SELECT`, `HAVING`, and `ORDER BY` reuse one
  placeholder
- different aggregate expressions receive different placeholders
- unaliased rewritten projection expressions still expose the original field name
- explicit aliases remain unchanged after rewrite
- `HAVING`-only aggregates still appear in `aggregate_mappings`
- aggregate queries reject raw non-grouped columns in `SELECT`
- aggregate queries reject raw non-grouped columns in `ORDER BY`
- `HAVING` without aggregate placeholders is rejected unless it references `last_agg_hit_count()`
- stateful placeholders in `ORDER BY` are rejected when aggregation is present
- qualified wildcard and named aggregate arguments fail during physical compilation

## Future Work

- Replace debug-string deduplication with a more explicit normalized-expression key if rewrite
  stability becomes a concern.
- Document `DISTINCT` support per aggregate more explicitly once processor behavior is unified.
