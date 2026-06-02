# ByIndexProjectionIntoEncoderRewrite

## Overview

`ByIndexProjectionIntoEncoderRewrite` is a physical-plan optimization rule that prepares
**encoder-side delayed materialization** for `Project` nodes that are effectively doing:

- `ScalarExpr::Column(ColumnRef::ByIndex { .. })`
- with or without an alias (the encoder uses the projected output field name)

The goal is to reduce CPU overhead in large `SELECT` lists (e.g. 15k columns) by avoiding
rebuilding messages and repeatedly cloning `Arc`-backed values during projection.

Implementation: `src/flow/src/planner/optimizer.rs` (`ByIndexProjectionIntoEncoderRewrite`).

## Background (Why This Exists)

In the current runtime pipeline, `Project` commonly materializes a new tuple/collection by
constructing projected messages. For projections dominated by `ColumnRef::ByIndex`, this tends to:

- allocate new vectors for projected message maps
- perform a large number of `Arc::clone` / value cloning operations per tuple

When the downstream sink is an encoder that can encode by reading source columns directly by
index, we can delay this “materialization” until encode time, and keep the upstream messages
unchanged.

## Inputs (What It Recognizes)

This rule scans the physical plan graph and identifies `PhysicalPlan::Project(project)` nodes
where:

- `project.fields` is non-empty, and
- every field is `ScalarExpr::Column(ColumnRef::ByIndex { .. })`, and
- the fields are eligible for encoder-side by-index projection (aliases are allowed).

## Preconditions (When It Is Safe)

This rewrite is applied only when **all consumers** of the `Project` node can honor delayed
materialization:

- Every consumer must be `PhysicalPlan::SinkEncoder`.
- The encoder kind must report support via:
  - `EncoderRegistry::supports_by_index_projection(kind)`

Design constraint (multi-sink / shared DAG): if a `Project` is shared by multiple downstream sinks,
the rewrite is **all-or-nothing**. If any sink’s encoder cannot support the capability, the rewrite
is not applied.

## Outputs (What It Produces)

When matched and safe, the rule rewrites the plan in two coordinated ways:

1. **Mark the `Project` as passthrough**:
   - `PhysicalProject.fields` becomes empty (`[]`)
   - `PhysicalProject.passthrough_messages = true`
   - Children remain the same

2. **Attach the delayed projection spec to encoders**:
   - The downstream `PhysicalSinkEncoder` nodes receive
     `by_index_projection: Some(ByIndexProjection)`
   - Each spec item records:
     - `source_name`
     - `column_index`
     - `output_name` (currently identical to the original identifier because aliases are excluded)

This is a planner-stage rewrite. The runtime processors must interpret `passthrough_messages` and
`by_index_projection` correctly to realize the performance win.

## Explain / Tests

After optimization, `EXPLAIN` output should reflect:

- `PhysicalProject` shows `passthrough_messages=true`.
  - When all fields are delayed, `fields=[]`.
  - For mixed projections, `fields=[...]` includes only the non-delayed expressions.
- Encoder nodes show `by_index_projection=...` describing the delayed columns.

The rewrite is covered by table-driven planner explain tests in:

- `src/flow/tests/planner/physical/plan_explain_table_driven.rs`

Test function: `plan_explain_by_index_projection_rewrite_table_driven`.

## Implementation Notes

- The rule builds:
  - a `node_map` (`plan_index -> node`) and
  - a `consumer_map` (`child_plan_index -> [consumers]`)
  by walking the graph starting from the physical plan root.
- The rewrite is applied via a memoized rewrite pass keyed by `plan_index`, so shared nodes are
  rewritten consistently across a DAG.
- Encoder capability is queried through `EncoderRegistry` (not hard-coded by encoder kind).

## Limitations / Follow-ups

- Supports **mixed projections** by delaying only the eligible unaliased `ColumnRef::ByIndex` fields
- Supports **mixed projections** by delaying eligible `ColumnRef::ByIndex` fields into the encoder
  while keeping the remaining expressions in the `Project`.
