# ListElementPruning

## Overview

`ListElementPruning` is responsible for **list index selection** during decoding. It records which
list indices are accessed by SQL expressions and stores this as a `DecodeProjection` on the
`LogicalPlan::DataSource`.

This rule does **not** shrink schemas or datatypes. Element struct field pruning is handled by
`StructFieldPruning`.

Implementation: `src/flow/src/planner/logical_optimizer.rs` (`ListElementPruning`) and
`src/flow/src/planner/decode_projection.rs`.

## Inputs (What It Recognizes)

It looks for nested access chains that include list index segments:
- constant index: `items[0]` → records index `0`
- non-constant index: `items[i]` → marks “all indices”

It records the full nested path, so the decode projection can also include nested struct/list shape
information (e.g. “in list element, only field `x` is needed”).

## Outputs (What It Produces)

### Regular sources

- Produces `LogicalPlan::DataSource.decode_projection: Option<DecodeProjection>`.
- Physical planning propagates the same projection into `PhysicalDataSource` / `PhysicalDecoder`.
- Decoder uses it to avoid decoding unused indices; the list length is preserved and non-decoded
  indices become `NULL`.

### Shared sources

- Disabled (keeps full decode) to avoid cross-consumer inconsistency.

## Explain Walkthrough (Continuation)

Using the same schema and SQL as in `top_level_column_pruning.md`:

After `ListElementPruning` (regular source):
- `schema` stays the same as after `StructFieldPruning`
- `decode_projection` causes explain to show list index selection:
  - `schema=[a, b{items[0,3][struct{x}]}]`

## Decoder Semantics (Key Guarantee)

When index selection is applied at decode time:
- the list length must not change
- elements at non-required indices decode to `NULL`

This preserves correct indexing semantics for downstream evaluation of `items[k]`.

