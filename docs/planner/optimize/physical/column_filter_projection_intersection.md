# ColumnFilterProjectionIntersection

## Overview

`ColumnFilterProjectionIntersection` is a physical-plan optimization rule that crops and re-indexes
by-index projection specs when a transparent `PhysicalColumnFilter` sits between a shared
`Project` and its downstream `RowDiff`/`SinkEncoder` consumers.

Implementation: `src/flow/src/planner/optimizer.rs` (`ColumnFilterProjectionIntersection`).

## Background (Why This Exists)

Per-sink column filtering introduces `PhysicalColumnFilter` nodes between the shared final
`Project` and each sink's processing chain:

```text
shared Project([a,b,c,d])
  ├─ ColumnFilter(sink_1, include=[a,c]) → RowDiff → ... → Encoder
  └─ ColumnFilter(sink_2, include=[b,d]) → RowDiff → ... → Encoder
```

`PhysicalColumnFilter` is **transparent** in the consumer map, so the existing by-index projection
rewrite rules continue to fire on the shared `Project`, making it passthrough and attaching the
**full** projection spec to each `RowDiff`/`Encoder`:

```text
shared Project(passthrough)
  ├─ ColumnFilter(sink_1, include=[a,c])
  │   └─ RowDiff(late_projection: [(s,0,0,a), (s,1,1,b), (s,2,2,c), (s,3,3,d)])
  └─ ColumnFilter(sink_2, include=[b,d])
      └─ RowDiff(late_projection: [(s,0,0,a), (s,1,1,b), (s,2,2,c), (s,3,3,d)])
```

This is a problem:

1. Each `RowDiff`'s `late_projection` includes columns the sink does not need
2. The `output_index` values in the projection spec are based on the **shared Project's** output
   schema (width=4), but `RowDiff`'s actual output width comes from `ColumnFilter` (width=2).
   `output_index=2` on a width-2 output is out of bounds.

This rule corrects both issues before plan execution.

## Goal

For each `PhysicalColumnFilter` node that sits above a `RowDiff` or `SinkEncoder` carrying a
by-index projection spec:

1. **Crop** the projection spec to only the columns the `ColumnFilter` wants
2. **Re-index** the `output_index` of each remaining column to align with the `ColumnFilter`'s
   narrowed output schema
3. **Remove the `ColumnFilter` node from the plan**, re-parenting its consumer directly to the
   column filter's child

## Inputs (What It Recognizes)

This rule scans the physical plan graph and identifies `PhysicalColumnFilter` nodes where:

- the `ColumnFilter` has `include_columns` or `exclude_columns` configured
- the nearest ancestor carrying a by-index projection is either:
  - `PhysicalRowDiff` with `late_projection: Some(...)`, or
  - `PhysicalSinkEncoder` or `PhysicalIncSinkEncoder` with `by_index_projection: Some(...)`
- the ancestor is reachable by walking up through transparent nodes (`EmptySuppress`, `Batch`, etc.)

## Preconditions (When It Is Safe)

This rewrite is applied **after** all existing by-index projection rewrite rules have run:

```text
Execute order:
  1. ByIndexProjectionAcrossMixedConsumersRewrite
  2. PartialByIndexRowDiffAndEncoderRewrite
  3. ByIndexProjectionIntoRowDiffRewrite
  4. ByIndexProjectionIntoEncoderRewrite
  5. ColumnFilterProjectionIntersection    ← THIS RULE
  6. InsertBarrierForFanIn
```

By this point, the shared `Project` has been converted to passthrough, and projection specs are
attached to all eligible `RowDiff` and `Encoder` nodes.

The rule requires:

- `ColumnFilter` has a non-empty include or exclude list
- The target node (`RowDiff` or `Encoder`) carries a by-index projection spec
- The `ColumnFilter`'s output schema is available (from `output_schema()`)

## Outputs (What It Produces)

### Cropping

For each column in the target's projection spec:

- Look up the original column name from the shared output schema
- Check whether the column matches the `ColumnFilter`'s include/exclude rule
- Keep matching columns, discard others

### Re-indexing

For each surviving column, assign a new `output_index` that corresponds to the column's position
in the `ColumnFilter`'s output schema:

| Old output_index (shared width=4) | Column | Filter match? | New output_index (filtered width=2) |
|-----------------------------------|--------|---------------|-------------------------------------|
| 0 | a | ✅ | 0 |
| 1 | b | ❌ | — (discarded) |
| 2 | c | ✅ | 1 |
| 3 | d | ❌ | — (discarded) |

### Passthrough Marking

When all columns in the `ColumnFilter`'s include set are covered by the upstream projection spec
(i.e., no remaining columns need runtime filtering), **remove the `ColumnFilter` node entirely**.
The consumer (e.g., `RowDiff`, `Batch`, or `Encoder`) is re-parented to the `ColumnFilter`'s child.

The projection spec on the consumer already handles the full column delivery. There is no need for
a passthrough alias in the plan.

### When Full Passthrough Is Not Possible

If some columns in the `ColumnFilter`'s include set are not covered by the upstream projection
spec (e.g., they come from computed expressions rather than `ColumnRef::ByIndex`), the
`ColumnFilter` is kept active for those columns. Only the by-index-compatible subset is shifted
to the upstream projection.

## Algorithm

```text
for each PhysicalColumnFilter node (post-order walk):
    let filter = node's include/exclude specification
    let target = find nearest ancestor (RowDiff or Encoder) with by-index projection
    
    if target has projection spec && column_filter has output_schema:
        let cropped = filter_projection_spec(projection, filter, shared_output_schema)
        let reindexed = reindex_for_output_schema(cropped, filter_output_schema)
        
        target.projection = reindexed
        
        if reindexed covers all filter columns:
            column_filter.passthrough = true
```

### Walking Up Through Transparent Nodes

The "find nearest ancestor" step walks upward from the `ColumnFilter` through nodes that are
transparent in plan structure:

- `EmptySuppress`: passthrough, skip
- `Batch`: passthrough, skip

It stops at the first node that can carry a by-index projection spec:

- `PhysicalRowDiff` (field: `late_projection`)
- `PhysicalSinkEncoder` (field: `by_index_projection`)
- `PhysicalIncSinkEncoder` (field: `by_index_projection`)

## Example

### Before Optimization

```text
shared Project(passthrough=true, fields=[])
  └─ PhysicalColumnFilter { sink_id: "sink_1", include_columns: ["a", "c"] }
      └─ PhysicalRowDiff {
           late_projection: [(s,0,0,a), (s,1,1,b), (s,2,2,c), (s,3,3,d)]
         }
```

Shared output schema: `[a, b, c, d]` (indices 0, 1, 2, 3)
ColumnFilter output schema: `[a, c]` (indices 0, 1)

### After Optimization

```text
shared Project(passthrough=true, fields=[])
  └─ PhysicalRowDiff {
       late_projection: [(s,0,0,a), (s,2,1,c)]   ← cropped + reindexed
     }
```

The `PhysicalColumnFilter` node is **eliminated**. `RowDiff` now connects directly to the shared
`Project`.

- Column at old output_index 1 (b): discarded (not in include_columns)
- Column at old output_index 3 (d): discarded (not in include_columns)
- Column at old output_index 0 (a): kept, reindexed to new output_index 0
- Column at old output_index 2 (c): kept, reindexed to new output_index 1

## Explain / Tests

After optimization, `EXPLAIN` output should reflect:

- `PhysicalColumnFilter` is no longer present in the plan
- `PhysicalRowDiff.late_projection` shows only the filtered, re-indexed columns
- `PhysicalSinkEncoder.by_index_projection` shows only the filtered columns (for non-delta sinks)

The rewrite is covered by table-driven planner explain tests in:

- `src/flow/tests/planner/physical/plan_explain_table_driven.rs`

## Implementation Notes

- The rule walks the plan bottom-up (post-order), processing `ColumnFilter` nodes as they are
  encountered
- The "find nearest ancestor" traversal is a local upward walk; it does not need the full
  `consumer_map` because `ColumnFilter` is transparent and its ancestors are always in the same
  sink branch
- The shared output schema used for column name lookup is obtained from the shared `Project`'s
  `output_schema()`
- Re-indexing uses the `ColumnFilter`'s own `output_schema()` to determine the new position of
  each surviving column

## Limitations / Follow-ups

- If the `ColumnFilter` introduces computed expressions (beyond simple column selection), the
  rule keeps those as active fields in the `ColumnFilter` while only pushing eligible by-index
  columns into the upstream projection
- The rule does not currently handle the case where a `ColumnFilter` sits between two projection-
  carrying nodes (e.g., both an upstream late-projected RowDiff and a downstream by-index
  Encoder). This could be added if the plan topology ever requires it.
