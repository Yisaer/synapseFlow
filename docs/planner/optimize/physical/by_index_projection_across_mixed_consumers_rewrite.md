# ByIndexProjectionAcrossMixedConsumersRewrite

## Overview

`ByIndexProjectionAcrossMixedConsumersRewrite` is a physical-plan optimization rule that applies
by-index delayed materialization when one shared `Project` feeds both:

- row-diff consumers
- encoder or streaming-encoder consumers

Implementation: `src/flow/src/planner/optimizer.rs`
(`ByIndexProjectionAcrossMixedConsumersRewrite`).

## Background

The standalone by-index projection rewrites are intentionally conservative:

- `ByIndexProjectionIntoEncoderRewrite` requires all direct project consumers to be encoders.
- `ByIndexProjectionIntoRowDiffRewrite` requires all direct project consumers to be row-diff nodes.

Multi-sink pipelines can produce a shared physical DAG where the final `Project` is consumed by
different sink suffixes. A common shape is:

```text
Project
  -> SinkEncoder -> SinkConnector
  -> RowDiff -> SinkEncoder -> SinkConnector
```

Without a mixed-consumer rule, that shared project remains materializing even though every consumer
can honor delayed materialization.

## Inputs

The rule scans `PhysicalProject` nodes and recognizes projections that contain at least one
`ScalarExpr::Column(ColumnRef::ByIndex { .. })` field.

Wildcard projections are not eligible. Non-by-index fields are allowed only as remaining project
fields that continue to be materialized by the project.

The direct consumers must include both:

- at least one `PhysicalRowDiff`
- at least one `PhysicalSinkEncoder`

## Preconditions

The rewrite is applied only when every relevant consumer can preserve the delayed materialization
contract.

Direct encoder consumers must:

- support by-index projection according to the encoder registry
- not have encoder transforms enabled

Direct row-diff consumers must:

- accept the delayed projection spec
- have only downstream encoder or streaming-encoder consumers
- have downstream encoders that support by-index projection
- have downstream encoders without transforms enabled

Any unsupported direct or downstream consumer prevents the rewrite for that project.

## Outputs

When the rule matches:

1. The shared project is rewritten toward passthrough:
   - eligible by-index fields are removed from `PhysicalProject.fields`
   - `PhysicalProject.passthrough_messages` is set
   - non-by-index fields, if any, remain in the project

2. Each direct encoder or streaming encoder receives the full delayed by-index projection spec.

3. Each direct row-diff receives the same delayed by-index projection spec.

4. Each encoder downstream of a rewritten row-diff also receives the same delayed projection spec.

This keeps all branches reading the same final projected column layout without forcing the shared
project to materialize the by-index columns.

## Why RowDiff And Encoder Both Receive The Spec

`RowDiff` needs the projected values to compare current rows against previous rows. The downstream
encoder still needs the same final output layout to encode the row-diff result.

Attaching the projection to both boundaries lets:

- `RowDiff` late-read the projected columns for delta comparison
- the downstream encoder late-read the projected columns for output materialization

## Rule Ordering

This rule runs before the narrower row-diff-only and encoder-only by-index projection rewrites.

That order matters because once a mixed-consumer project is rewritten, the narrower all-consumer
rules should not have to infer the mixed topology again.

## Explain / Tests

After optimization, `EXPLAIN` should show:

- the shared `PhysicalProject` marked as passthrough
- `PhysicalRowDiff` with `by_index_projection=...`
- direct or downstream encoder nodes with `by_index_projection=...`

The rule should be covered by table-driven planner explain tests in:

- `src/flow/tests/planner/physical/plan_explain_table_driven.rs`

Suggested cases:

- one shared project feeding both full-output and delta-output sink branches
- mixed consumers with a non-supporting encoder does not rewrite
- mixed consumers with encoder transform enabled does not rewrite
- mixed consumers with a non-row-diff/non-encoder consumer does not rewrite

## Limitations

- The rule does not support wildcard projections.
- Encoder transforms disable the rewrite because template transforms consume materialized row
  values.
- The rule is intentionally graph-aware so shared DAG nodes are rewritten consistently.
