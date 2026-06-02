# ByIndexProjectionIntoRowDiffRewrite

## Overview

`ByIndexProjectionIntoRowDiffRewrite` is a physical-plan optimization rule that prepares
**row-diff-side delayed materialization** for sink branches shaped like:

```text
Project -> RowDiff -> ...
```

when the `Project` is effectively doing a pure pass-through projection:

- every projected field is `ScalarExpr::Column(ColumnRef::ByIndex { .. })`
- there is no wildcard
- the downstream consumer that needs the final output row is `RowDiff`, not the encoder

The goal is to remove one redundant tuple/message materialization step on row-diff branches:

```text
current:
  upstream -> Project(materialize final row) -> RowDiff(read + compare + materialize diff row)

after rewrite:
  upstream -> Project(passthrough) -> RowDiff(read final row directly + compare + materialize diff row)
```

This keeps row-diff semantics unchanged while reducing CPU cost in wide-output pipelines.

Implementation:

- `src/flow/src/planner/optimizer.rs` (`ByIndexProjectionIntoRowDiffRewrite`)

## Background (Why This Exists)

`RowDiff` must compare the **final sink-facing output row** against the previous emitted row of the
same sink branch. That means it needs final projected values before the encoder runs.

For a branch like:

```sql
SELECT a1, a2, ..., a100 FROM stream
```

with sink output mode `delta`, the current runtime work is conceptually:

1. `Project` builds a new tuple containing `a1..a100`
2. `RowDiff` reads those `a1..a100` values again
3. `RowDiff` builds the diff tuple and `output_mask`

For pure by-index pass-through projections, step 1 is redundant. `RowDiff` can be the first
consumer that reads the final projected columns and materializes the stable output tuple.

This optimization is complementary to logical source pruning:

- `TopLevelColumnPruning` reduces how many source columns are decoded / carried downstream
- `ByIndexProjectionIntoRowDiffRewrite` removes the redundant `Project` materialization on the
  remaining projected columns

These two optimizations address different layers and can stack.

## Scope Of The First Version

The first version should be intentionally narrow.

Supported:

- `PhysicalProject` whose fields are all `ColumnRef::ByIndex`
- direct downstream `PhysicalRowDiff`
- delayed materialization endpoint is `RowDiff`

Not part of the first version:

- wildcard (`*` / `source.*`)
- mixed projections (`by-index` fields plus computed expressions)
- delaying materialization past `RowDiff` into the encoder
- row-diff branches that share the same `Project` with non-row-diff consumers

The narrow scope keeps the first rule easy to reason about and aligns with the highest-value row
diff case: wide pass-through projections.

## Inputs (What It Recognizes)

This rule scans the physical plan graph and identifies `PhysicalPlan::Project(project)` nodes
where:

- `project.fields` is non-empty
- every field is `ScalarExpr::Column(ColumnRef::ByIndex { .. })`
- no field is `ScalarExpr::Wildcard { .. }`

and whose consumer shape is:

```text
Project
  -> RowDiff
```

For shared DAGs, the same project may feed multiple sink branches. See the preconditions below.

## Preconditions (When It Is Safe)

The rewrite should be applied only when all of the following hold.

### 1. `RowDiff` is the first consumer that needs final projected values

The consumer directly below the `Project` must be `PhysicalPlan::RowDiff`.

This is the key difference from `ByIndexProjectionIntoEncoderRewrite`:

- encoder rewrite delays materialization into the encoder
- row-diff rewrite delays materialization into `RowDiff`

`RowDiff` cannot be skipped because it must compare final projected values before the encoder runs.

### 2. The `Project` is pure by-index pass-through

Every `Project` field must be expressible as:

- `source_name`
- `column_index`
- `output_name`

That is enough to build a delayed projection spec and enough for `RowDiff` to extract the final
current row directly from the upstream tuple.

The first version should reject:

- wildcard fields
- computed expressions
- mixed projections

### 3. Shared DAG rewrite remains all-or-nothing

If a `Project` node is shared by multiple downstream consumers, the rewrite should only apply when
**all consumers** can honor the delayed-materialization contract.

For the first version, that means every consumer must be a `PhysicalRowDiff` that can accept the
same projection spec.

If any consumer is not `RowDiff`, the rewrite must not apply.

### 4. The row-diff node must still expose a stable output schema

The rewrite must not change row-diff semantics:

- final output column names stay the same
- final output column order stays the same
- `RowDiff` still emits stable-schema rows with `NULL` fill for unchanged tracked columns
- `output_mask` semantics remain unchanged

The optimization is only about *where* final projected values are first materialized.

## Outputs (What It Produces)

When matched and safe, the rule rewrites the plan in two coordinated ways.

### 1. Mark the `Project` as passthrough

The `Project` node becomes a pass-through node:

- `PhysicalProject.fields = []`
- `PhysicalProject.passthrough_messages = true`

Its child stays unchanged.

This means the runtime `ProjectProcessor` forwards upstream messages without rebuilding the
projected tuple.

### 2. Attach delayed projection spec to `RowDiff`

`PhysicalRowDiff` receives a delayed projection spec that records the final output layout in
by-index form.

The existing `ByIndexProjection` / `ByIndexProjectionColumn` shape is already close to what is
needed:

- `source_name`
- `column_index`
- `output_name`

So the first version can likely reuse the same projection-spec type rather than inventing a new
shape.

Conceptually, `PhysicalRowDiff` would carry:

- `late_projection: Some(ByIndexProjection)`

or an equivalent row-diff-specific field name.

## Runtime Mapping

At runtime, `RowDiffProcessor` becomes the delayed-materialization endpoint.

Without the rewrite:

1. `ProjectProcessor` rebuilds the projected tuple
2. `RowDiffProcessor` extracts values from that projected tuple
3. `RowDiffProcessor` compares against previous full row
4. `RowDiffProcessor` materializes the diff tuple

With the rewrite:

1. `ProjectProcessor` forwards upstream messages unchanged
2. `RowDiffProcessor` uses the delayed projection spec to read the final current row directly from
   the upstream tuple
3. `RowDiffProcessor` compares against previous full row
4. `RowDiffProcessor` materializes the diff tuple

The stable diff tuple is still built in `RowDiffProcessor`. The optimization does **not** delay
materialization further into the encoder.

## Why This Is Different From Encoder-Side Delayed Materialization

`ByIndexProjectionIntoEncoderRewrite` is safe only when the encoder is the first consumer that
needs the projected values.

That is not true on row-diff branches.

For:

```text
Project -> RowDiff -> Encoder
```

the first consumer that needs final projected values is `RowDiff`, because `RowDiff` must compare
the final current row with the previous row before the encoder runs.

So the correct optimization endpoint is:

- `RowDiff`, not `Encoder`

This is the core reason a separate rule is needed.

## Explain / Tests

After optimization, `EXPLAIN` should reflect:

- `PhysicalProject` shows `passthrough_messages=true`
- `PhysicalProject.fields=[]`
- `PhysicalRowDiff` shows the delayed projection spec, for example:

```text
PhysicalRowDiff: sink_id=test_sink, mode=delta, columns=[a1, a2], by_index_projection=[stream#0->a1; stream#1->a2]
```

The rule should be covered by table-driven planner explain tests in:

- `src/flow/tests/planner/physical/plan_explain_table_driven.rs`

Suggested cases:

1. pure by-index project feeding row diff rewrites to passthrough project
2. row diff branch with batching still rewrites before `SinkEncoder`
3. shared project with one non-row-diff consumer does not rewrite
4. wildcard project does not rewrite
5. mixed projection does not rewrite in the first version

## Suggested Implementation Shape

The implementation can mirror the structure of `ByIndexProjectionIntoEncoderRewrite`:

1. Build:
   - a `node_map` (`plan_index -> node`)
   - a `consumer_map` (`child_plan_index -> [consumers]`)

2. Scan `PhysicalProject` nodes and collect rewrite candidates.

3. For each candidate:
   - build `ByIndexProjection`
   - mark the project for passthrough rewrite
   - attach the projection spec to downstream `PhysicalRowDiff`

4. Rebuild the graph through a memoized rewrite pass keyed by `plan_index`.

This preserves shared-DAG correctness and keeps the rule localized in the physical optimizer.

## Limitations / Follow-ups

Potential follow-up extensions:

- support mixed projections by delaying only the by-index subset and letting `Project` keep the
  computed remainder
- support wildcard by expanding wildcard output order into an explicit delayed projection spec
- share a common delayed-materialization infrastructure between:
  - `ByIndexProjectionIntoEncoderRewrite`
  - `ByIndexProjectionIntoRowDiffRewrite`

The first version should avoid taking these on together. The narrow rule already captures the main
row-diff optimization opportunity while keeping the runtime contract simple.
