# List Index Column Pruning

## Background

In synapseFlow, a pipeline is compiled from StreamDialect SQL into a `LogicalPlan`, then into a `PhysicalPlan`, and finally executed by decoders/processors.

We already support **column pruning** at the logical-optimizer stage: unused top-level columns can be removed from each stream’s schema so that the physical `DataSource/Decoder` only sees the necessary columns. This was later extended to nested types:

- `struct` field pruning (e.g. `b->c` should only keep field `c` under `b`)
- `list<struct>` element-field pruning (e.g. `items[0]->c` should only keep field `c` under the list element struct)

However, the explain output and runtime decoding still treat lists as “decode all elements”, even when the query only accesses a small subset of indices, e.g.:

```sql
SELECT a, (b->items)[0]->x FROM stream_4
```

In this query:
- Only list index `0` is accessed.
- Only field `x` of the element struct is accessed.
- But decoding `items` currently requires parsing every element (and potentially every field of each element), which is wasteful.

This document describes the desired behavior and the planned implementation for **list index pruning**.

## Goals

1. **Logical plan pruning understands list indices**:
   - When the query contains constant list index access (e.g. `[0]`, `[3]`), record the accessed indices.
   - When indices are not constant (e.g. `items[i]`), fall back to “all indices”.
2. **Explain output reflects index pruning**:
   - Logical plan explain should show both:
     - which struct fields are required, and
     - which list indices are required (when known).
3. **Physical decoding only decodes required indices**:
   - The physical decoder and subsequent decoder processor should avoid decoding unused list elements.
   - This must be visible in physical plan explain as well.

## Non-goals (for initial iteration)

- Rewriting SQL syntax or changing expression semantics.
  - Ambiguous `b->items[0]` should be avoided by users; prefer `(b->items)[0]`.
- Full pushdown for all expressions.
  - Initial support targets list index access patterns the planner can statically recognize.

## Semantics and Safety Notes

### Do not shrink list length

If we prune list indices at decode time, **we must not shrink the list length**.

Example: query accesses `[0]` and `[3]`.
- Input list length might be `10`.
- If the decoder returned a list of length `2`, then later evaluation of `items[3]` would be incorrect or would raise an out-of-bounds error.

Therefore, when index pruning is applied:
- Preserve the original list length.
- For indices that are not decoded, fill the element with `Null` (or an equivalent “missing” value).

This ensures downstream operators (`Project`, `Filter`, etc.) keep consistent indexing behavior.

### Validation should fail early for non-existent fields

A query like `items[0]->b` should fail during logical planning/optimization if:
- the list element is a struct, and
- field `b` does not exist in the element struct schema.

This validation case is important because explain/pruning should never silently accept invalid nested accesses.

## Shared Stream Caveat (Why Nested Pruning May Not Apply)

The logical pruning rules (`StructFieldPruning`, `ListElementPruning`, and list index `DecodeProjection`) are designed for **regular sources** where each pipeline owns its own `DataSource → Decoder` chain.

For **shared streams** (`SourceBindingKind::Shared`), synapseFlow intentionally keeps a different contract:

- A shared stream runtime decodes once and broadcasts tuples to multiple consumers (pipelines).
- Different consumers may require different nested fields and different list indices.
- If we applied per-pipeline nested pruning/index projection to the shared stream’s single decoder, one consumer’s projection could accidentally break another consumer.

Because of this, shared streams currently:

- keep the **full schema** for nested types in logical pruning (no struct-field / list-element schema shrink), and
- only use a **top-level column** union (`Vec<String>`) across consumers to reduce decoding work safely.

Supporting nested pruning for shared streams would require a different mechanism (e.g. maintaining per-consumer projections and decoding the union of nested requirements), which is out of scope for this iteration.

## Desired Explain Representation

For a schema like:

- `a: int64`
- `b: struct{ c: int64, items: list<struct{ x: int64, y: string }> }`

And SQL:

```sql
SELECT a, (b->items)[0]->x FROM stream_4
```

The logical/physical datasource schema in explain should indicate:
- `a` is used
- `b.items` is used
- list indices `{0}` are used
- element struct field `{x}` is used

An example (format subject to final implementation) could be:

```
schema=[a, b{items[indexes={0}][struct{x}]}]
```

## Proposed Implementation (High Level)

### How `StructFieldPruning` works (Logical Optimizer)

`StructFieldPruning` is a logical-optimizer rule that shrinks **struct-typed columns** to only the fields that are actually referenced by expressions.

Conceptually it does:

1. Traverse SQL expressions and recognize `JsonAccess` chains such as `stream_4.b->items->x`.
2. For each access, record a field-path usage rooted at a top-level column (keyed by `source_name + column_name`).
   - Multiple expressions merge into a single usage tree per column.
3. When building the pruned schema, rewrite each struct column datatype by keeping only referenced fields, and recursively applying the same pruning to nested structs.

Important notes:

- This rule only controls **datatype shrink** (which fields exist in the pruned schema). It does not express list index selection.
- List index selection is carried separately in `DecodeProjection` and is used at decode time.
- For shared streams, this rule is currently disabled (see “Shared Stream Caveat”) to avoid cross-consumer inconsistency.

### Step 1 — Projection model

Introduce a projection model that can represent nested requirements:
- top-level column
- struct field path
- list item projection
- optional list index set (constant indices) or “all indices”

This model should be serializable into the existing `Schema`-based explain output.

### Step 2 — Usage collection

Extend the logical optimizer’s expression traversal to recognize:
- list indexing (`items[0]`, `(b->items)[0]`, nested list indexing, etc.)
- downstream struct field access on indexed elements (`items[0]->x`)

When the index is a constant integer:
- record it in the projection model (e.g. `{0}`)

When the index is not statically known:
- mark the list projection as “all indices”

### Step 3 — Logical explain

Integrate the projection model into logical explain so that:
- `LogicalPlan::DataSource` uses a schema formatting that can show list index sets
- regression tests print the logical explain output and assert that index pruning is visible

### Step 4 — Physical plan propagation

Propagate the projection model into physical planning so that:
- `PhysicalDataSource/PhysicalDecoder` include the pruned schema/projection info
- physical explain includes index pruning details

### Step 5 — Decoder execution

Update JSON decoding (and other decoders that can support it) to:
- decode only required list indices when index sets are present
- preserve list length by emitting `Null` for non-decoded indices

Add a regression test that prints the physical plan explain output and verifies:
- physical decoder schema/projection reflects the pruned indices
- downstream decoder processor applies the same projection consistently
