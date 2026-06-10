# Function Semantics And Taxonomy

## Background

veloFlux exposes SQL-visible functions through one semantic model, but the implementation is split
across three execution categories:

- scalar functions evaluated per row without retained state
- aggregate functions evaluated over groups or windows
- stateful functions evaluated per row with retained per-partition state

The user-facing catalog is unified through `FunctionDef`, while execution ownership stays split
across dedicated runtime registries.

This document defines the semantic model and the testing dimensions that matter for compatibility.
It is not a complete function-by-function reference catalog.

## Goals

- Define the taxonomy shared by parser, planner, runtime, and introspection.
- Explain which metadata fields are semantically important.
- Clarify context restrictions and capability flags that should drive validation and testing.
- Document registry ownership boundaries so parser recognition and runtime execution stay aligned.

## Non-Goals

- Exhaustively listing every built-in function.
- Repeating the detailed stateful behavior already covered in
  [`stateful_functions.md`](../language/stateful_functions.md).
- Describing user-facing REST payloads for capability APIs.

## Function Taxonomy

The canonical categories are represented by `FunctionKind`:

- `Scalar`: stateless per-row computation such as `concat(a, b)` or `regexp_substring(...)`
- `Aggregate`: grouped or windowed reduction such as `sum(x)` or `median(x)`
- `Stateful`: per-row computation with evolving state such as `lag(x)` or `latest(x)`

The category determines:

- which registry owns runtime execution
- which SQL contexts are valid
- which planner stages must be present
- which testing dimensions matter

## Function Metadata Model

`FunctionDef` is the shared metadata contract for all SQL-visible functions.

The important fields are:

- `kind`: scalar, aggregate, or stateful
- `name`: canonical function name used for sorting and display
- `aliases`: additional SQL-visible spellings
- `signature`: argument names, optionality, variadic behavior, and return type shape
- `description`: short semantic summary
- `allowed_contexts`: where the function is valid in SQL
- `requirements`: additional planner/runtime preconditions
- `constraints`: stable behavioral limits worth surfacing to users and tests
- `examples`: representative SQL usage
- `aggregate`: aggregate-only sub-spec, currently including `supports_incremental`
- `stateful`: stateful-only sub-spec describing retained state semantics

This metadata is intentionally compact. It should describe stable semantics and testable boundaries,
not every internal implementation detail.

## Context Rules

Context validity is explicit metadata, not an implied convention.

Current context values are:

- `select`
- `where`
- `group_by`

The current built-in patterns are:

- scalar functions default to `select`, `where`, and `group_by`
- aggregate functions currently advertise `select`
- stateful functions currently advertise `select` and `where`

Context metadata should be read together with `requirements`:

- aggregate functions normally require `AggregateContext`
- stateful functions normally require `DeterministicOrder`

In practice this means:

- a scalar function can appear in normal row-level expressions, subject to type checks
- an aggregate function still depends on aggregate rewrite and aggregation planning even if the
  outer clause is `SELECT`
- a stateful function is row-level syntactically, but it requires ordered stateful execution

## Scalar Function Semantics

Scalar functions are implemented by `CustomFuncRegistry`. Their metadata is usually created through
helpers that default to broad row-expression contexts and no extra requirements.

Important scalar semantics:

- lookup is case-insensitive
- aliases are registered as direct runtime lookup keys
- metadata and runtime aliases must stay aligned

Examples of alias behavior:

- `regexp_substring` exposes alias `regexp_substr`
- `ceil` exposes alias `ceiling`

Scalar tests should therefore cover:

- canonical-name lookup
- alias lookup
- case-insensitive lookup
- argument-count and argument-type failures

## Aggregate Function Semantics

Aggregate functions are implemented by `AggregateFunctionRegistry`, which also implements the
parser-side `AggregateRegistry`.

That ownership split is intentional: parser recognition and runtime availability should come from
the same aggregate registration set.

Important aggregate metadata patterns:

- `allowed_contexts` is usually `select`
- `requirements` includes `AggregateContext`
- `aggregate.supports_incremental` distinguishes aggregates that can participate in streaming-style
  incremental execution from those that require full accumulation

Examples:

- `sum` is incremental
- `median` is non-incremental
- `deduplicate` is non-incremental

Tests should cover both semantic correctness and capability correctness:

- signature validation
- return-type behavior
- null handling
- incremental capability flags matching runtime behavior

## Stateful Function Semantics

Stateful functions are implemented by `StatefulFunctionRegistry`, which also implements the
parser-side `StatefulRegistry`.

Important stateful metadata patterns:

- `allowed_contexts` is usually `select` and `where`
- `requirements` includes `DeterministicOrder`
- `stateful.state_semantics` explains what state is retained per partition

Examples:

- `lag` keeps a bounded lag buffer
- `latest` keeps the latest accepted non-NULL value
- `changed_col` keeps one previous value and emits on change
- `had_changed` keeps one previous value per tracked argument position
- `change_to` keeps one previous value and emits true only on transition to a target
- `change_capture` holds the value captured when a monitored expression changes

The runtime registry rejects duplicate registrations, which is stricter than the current scalar and
aggregate registries. That behavior is useful because stateful semantics are especially sensitive to
accidental shadowing.

Detailed SQL-shape restrictions for stateful calls are documented separately in
[`stateful_functions.md`](../language/stateful_functions.md).

## Type And Signature Semantics

`FunctionSignatureSpec` is the catalog-level shape used across all function kinds.

Semantically important parts:

- argument order matters
- `optional` distinguishes trailing configuration arguments from required inputs
- `variadic` captures functions such as `had_changed(ignore_null, x1, x2, ...)`
- return type may be concrete (`bool`, `float64`) or abstract (`any`, `numeric`, list of `any`)

This metadata is intentionally higher-level than internal Rust types. It is suitable for capability
introspection and test planning, not for exact executor type inference.

Tests should validate both:

- catalog-level signature stability
- runtime/type-check behavior for representative accepted and rejected input types

## Incremental Aggregate Capability

`AggregateFunctionSpec.supports_incremental` is not cosmetic metadata. It influences planning and
test expectations for streaming execution.

Current examples:

- `sum`, `count`, `avg`, `min`, and `max` report incremental capability
- `median`, `deduplicate`, and `ndv` do not

This flag must remain aligned with the actual aggregate implementation's
`supports_incremental()` behavior. Divergence here would mislead both planning and capability
introspection.

## Registry Ownership And Introspection

Runtime ownership is split as follows:

- scalar: `CustomFuncRegistry`
- aggregate: `AggregateFunctionRegistry`
- stateful: `StatefulFunctionRegistry`

Catalog introspection merges all three categories:

- `list_function_defs()` returns the combined built-in set sorted by canonical name
- `describe_function_def()` resolves by canonical name or alias, case-insensitively

The design requirement is straightforward: runtime registry contents and introspection metadata must
describe the same function surface.

## Testing Guidance

When adding or changing functions, cover these dimensions:

- canonical-name lookup, alias lookup, and case-insensitive lookup
- metadata correctness for `kind`, contexts, requirements, constraints, and examples
- signature shape: required, optional, and variadic arguments
- accepted versus rejected input types
- null-handling behavior
- return-type behavior
- aggregate incremental capability alignment
- stateful state semantics for representative happy-path cases
- parser-recognition alignment for aggregate and stateful built-ins

For stateful functions, also verify the documented SQL-shape restrictions in
[`stateful_functions.md`](../language/stateful_functions.md). For aggregate usage across clauses,
verify the rewrite contract in [`aggregate_rewrite.md`](./aggregate_rewrite.md).

## Future Work

- Add richer context metadata if new SQL clauses become function-sensitive.
- Consider exposing more explicit capability flags for aggregate `DISTINCT` support and other
  planner-visible restrictions.
