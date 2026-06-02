# PartialByIndexRowDiffAndEncoderRewrite

## Overview

`PartialByIndexRowDiffAndEncoderRewrite` is a physical-plan optimization rule for branches shaped
like:

```text
Project -> RowDiff -> Encoder
```

It splits a pure by-index projection between `RowDiff` and the downstream encoder:

- `RowDiff` late-reads only the columns it tracks for delta comparison
- the downstream encoder late-reads the remaining pass-through columns

Implementation: `src/flow/src/planner/optimizer.rs`
(`PartialByIndexRowDiffAndEncoderRewrite`).

## Background

`ByIndexProjectionIntoRowDiffRewrite` attaches the entire delayed projection spec to `RowDiff`.
That is correct when row-diff is the only consumer that needs the projected values.

For row-diff output followed by an encoder, this can still make `RowDiff` read more columns than it
actually tracks. If a delta output only tracks a subset of the final output columns, the remaining
columns are pass-through data for encoding and do not need to be read by `RowDiff`.

This rule narrows the work at each boundary.

## Inputs

The rule scans `PhysicalProject` nodes where:

- every project field is `ScalarExpr::Column(ColumnRef::ByIndex { .. })`
- there are no remaining computed project fields
- all direct consumers are `PhysicalRowDiff`

For each direct row-diff consumer:

- `tracked_column_indexes` must be non-empty
- tracked columns must be a strict subset of the projected columns
- downstream consumers must be encoders or streaming encoders

## Preconditions

The rewrite is applied only when all downstream encoders can honor by-index projection:

- the encoder kind supports by-index projection in the encoder registry
- encoder transform is not enabled
- downstream consumers are only encoder or streaming-encoder nodes

If any row-diff tracks no columns, tracks every column, or has an unsupported downstream consumer,
the rule does not rewrite the project.

## Outputs

When the rule matches:

1. The project is rewritten as passthrough:
   - `PhysicalProject.fields=[]`
   - `PhysicalProject.passthrough_messages=true`

2. Each row-diff receives a delayed projection spec for tracked columns only.

3. Each downstream encoder receives a delayed projection spec for untracked columns only.

This keeps the row-diff comparison path focused on the columns that define the delta while the
encoder still has enough information to materialize the final output row.

## Example

For a projected output:

```text
[speed, rpm, temp]
```

and `output.mode=delta` tracking only:

```text
[speed]
```

the rewrite attaches:

- `RowDiff.by_index_projection = [speed]`
- `Encoder.by_index_projection = [rpm, temp]`

The project itself becomes a passthrough node.

## Rule Ordering

This rule runs after the mixed-consumer rewrite and before the broader
`ByIndexProjectionIntoRowDiffRewrite` / `ByIndexProjectionIntoEncoderRewrite` rules.

That order lets the partial split claim the specialized `Project -> RowDiff -> Encoder` shape
before the broader row-diff rule attaches the entire projection to `RowDiff`.

## Explain / Tests

After optimization, `EXPLAIN` should show:

- `PhysicalProject` with passthrough behavior
- `PhysicalRowDiff` with a by-index projection containing only tracked columns
- downstream `PhysicalSinkEncoder` with a by-index projection containing
  only untracked pass-through columns

The rule should be covered by table-driven planner explain tests in:

- `src/flow/tests/planner/physical/plan_explain_table_driven.rs`

Suggested cases:

- delta output tracks a strict subset of projected by-index columns
- row-diff tracks every column and the partial rewrite does not apply
- row-diff tracks no columns and the partial rewrite does not apply
- downstream encoder transform disables the rewrite
- non-by-index project fields disable the partial split

## Limitations

- The rule only handles pure by-index projects.
- It does not split computed expressions.
- It does not run when the row-diff tracked set is empty or equal to the full projection.
