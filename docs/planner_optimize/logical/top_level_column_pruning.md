# TopLevelColumnPruning

## Overview

`TopLevelColumnPruning` removes unused **top-level columns** from each `LogicalPlan::DataSource`
schema for regular sources, so downstream decoding and execution only pay for columns the SQL
actually references.

For shared sources, it does **not** shrink the schema. Instead, it records a per-pipeline
projection view (`shared_required_schema`) to preserve `ColumnRef::ByIndex` semantics.

Implementation: `src/flow/src/planner/logical_optimizer.rs` (`TopLevelColumnPruning`).

## Inputs (What It Recognizes)

This rule scans SQL expressions that appear in:
- `Project`
- `Filter`
- `Aggregation`
- `Window` (including `partition_by`)
- `StatefulFunction`

It recognizes:
- `Identifier` / `CompoundIdentifier` as column references
- `JsonAccess (->)` as nested access, but for this rule only the **base column** matters
- `*` / `source.*` wildcards

## Outputs (What It Produces)

### Regular sources (`SourceBindingKind::Regular`)

- Updates the `SchemaBinding` entry schema by filtering to used top-level columns.
- Applies the pruned schema back into `LogicalPlan::DataSource.schema`.

### Shared sources (`SourceBindingKind::Shared`)

- Keeps `LogicalPlan::DataSource.schema` unchanged (full source schema).
- Produces `LogicalPlan::DataSource.shared_required_schema: Option<Vec<String>>`, which is a
  projection view (column name list) for runtime decoding.

## Degrade / Safety Behavior

If pruning cannot be done safely for a source, pruning is disabled and the full schema is kept.
Common cases:
- `SELECT *` or `source.*`
- ambiguous unqualified column reference (multiple sources contain the same column name)
- qualifier cannot be resolved

If `eventtime_enabled` is set, the eventtime column is always treated as used.

## Explain Walkthrough (One SQL, All Three Outputs)

Assume `stream_4` has schema:
- `a: int64`
- `b: struct{ c: int64, items: list<struct{ x: int64, y: string }> }`

SQL:

```sql
SELECT stream_4.a, stream_4.b->items[0]->x, stream_4.b->items[3]->x FROM stream_4
```

After `TopLevelColumnPruning` (regular source):
- `schema=[a, b{c, items[struct{x, y}]}]` (keeps `a` and `b`, does not shrink nested types)
- `shared_required_schema` is `None` (only used for shared sources)
- `decode_projection` is `None` (produced by `ListElementPruning`)

For shared sources, the same SQL would instead keep the full schema and set:
- `shared_required_schema=["a", "b"]`

