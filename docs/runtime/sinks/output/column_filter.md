# Sink Column Filter

## Background

In multi-sink pipelines, different sink branches may want different subsets of the final output
columns. For example:

- An MQTT sink may only need `speed` and `rpm`
- A file sink may want all columns
- An HTTP sink may want everything except a debug column

Today all sink branches share the same final output schema defined by the SQL `SELECT`.
There is no per-sink column filtering capability.

## Decision

Per-sink column filtering should be a **sink output feature** under `output`, not a SQL-level
feature and not a per-connector property.

Why:

1. The SQL `SELECT` already defines the maximum set of output columns
2. Different sinks in a multi-sink pipeline often serve different consumers with different needs
3. Column filtering is orthogonal to connector type — any sink should be able to use it
4. It's a "which columns" concern that belongs with other sink output configuration (`mode`,
   `delta`, `omit_if_empty`)

## Proposed API Shape

Add `include_columns` and `exclude_columns` as mutually exclusive fields under `output`:

```json
{
  "sinks": [
    {
      "id": "mqtt_speed",
      "type": "mqtt",
      "props": { "broker_url": "...", "topic": "vehicle/speed" },
      "encoder": { "type": "json" },
      "output": {
        "include_columns": ["speed", "rpm"]
      }
    },
    {
      "id": "file_all",
      "type": "file",
      "props": { "path": "/data", "filename_prefix": "vehicle_" },
      "encoder": { "type": "json" },
      "output": {}
    },
    {
      "id": "http_no_debug",
      "type": "http",
      "props": { "url": "https://server/api/telemetry" },
      "encoder": { "type": "json" },
      "output": {
        "exclude_columns": ["debug_info"]
      }
    }
  ]
}
```

Minimal shape:

- `output.include_columns` (optional): whitelist of final output column names to include
- `output.exclude_columns` (optional): blacklist of final output column names to exclude
- At most one of `include_columns` / `exclude_columns` may be set
- Column names are resolved against the final projected output schema of the pipeline

## Semantic Model

### Include Mode

When `include_columns` is set:

- Only the listed columns are emitted to this sink
- Column order matches the include list order
- If a listed column is not in the final output schema, the pipeline creation is rejected

Example: SQL produces `[a, b, c, d]`, include `["c", "a"]` → sink sees `[c, a]`

### Exclude Mode

When `exclude_columns` is set:

- All columns except the listed ones are emitted
- Column order preserves the original output schema order
- If a listed column is not in the final output schema, the pipeline creation is rejected

Example: SQL produces `[a, b, c, d]`, exclude `["b"]` → sink sees `[a, c, d]`

### Empty Result

- `include_columns: []` → rejected at plan-build time ("sink must receive at least one column")
- `exclude_columns` that excludes all columns → rejected at plan-build time

### Column Name Validation

All column names are resolved against the **final projected output schema** of the pipeline,
after aliasing. For example:

- `SELECT speed AS s, rpm AS r` → column names are `s`, `r`
- `include_columns: ["s"]` resolves against `[s, r]` ✅
- `include_columns: ["speed"]` not found, rejected ❌

## Physical Placement

Column filtering sits after the final `Project` and **before** `RowDiff` (when delta mode is
active). It acts as a "gate" that narrows the output schema before any sink-specific processing:

```
... → Project → ColumnFilter → RowDiff? → EmptySuppress? → Batch? → Encoder → SinkConnector
```

This placement has two benefits:

1. `RowDiff` sees only the columns the sink actually cares about, avoiding unnecessary diff
   computation on excluded columns
2. Downstream nodes (`EmptySuppress`, `Batch`, `Encoder`) all operate on the narrowed schema

### With Delta Mode

When `output.mode = delta` and column filtering are both active:

```
shared Project → ColumnFilter → RowDiff → EmptySuppress → Batch → Encoder
```

The filtering happens first. RowDiff's tracked columns (`output.delta.columns`) are resolved
against the ColumnFilter's output schema, not the shared Project's. This means:

- `output.delta.columns` must reference columns that survive the filter
- Delta diff work is scoped to the columns the sink actually emits

## Physical Plan Node: `PhysicalColumnFilter`

A new physical plan node type that performs per-sink column selection.

### Properties

- **Include/exclude columns**: stored as the filter specification
- **Output schema**: narrowed to the selected subset
- **Consumer map behavior**: **transparent** (see below)

### Transparent Consumer Map

`PhysicalColumnFilter` is classified as **transparent** in the consumer map, just like
`EmptySuppress` and `Batch`. It passes upstream consumers through to its children without
introducing `ProjectConsumer::Other`.

This is critical because it means inserting ColumnFilter nodes between the shared `Project` and
`RowDiff`/`Encoders` does not break existing by-index projection rewrite rules. The shared
`Project` continues to see the same consumer types (`RowDiff` / `SinkEncoder`) and can be
optimized to passthrough as before.

### ColumnFilter Elimination

When all columns in the ColumnFilter can be satisfied by the upstream projection, the
`ColumnFilterProjectionIntersection` optimizer rule (see
[`column_filter_projection_intersection.md`](../../planner/optimize/physical/column_filter_projection_intersection.md))
eliminates the ColumnFilter node from the plan by pushing the include/exclude filtering into the
nearest upstream by-index projection spec (on `RowDiff` or `Encoder`).

Before optimization:

```text
shared Project(passthrough)
  └─ ColumnFilter(include=[a,c])
      └─ RowDiff(late_projection: [(s,0,0,a), (s,1,1,b), (s,2,2,c), (s,3,3,d)])
```

After optimization:

```text
shared Project(passthrough)
  └─ RowDiff(late_projection: [(s,0,0,a), (s,2,1,c)])
```

The ColumnFilter node is **removed from the plan**. RowDiff re-parents to the shared Project
directly. The transformation does two things:

1. **Crops** the projection columns to only those that match the include/exclude filter
2. **Re-indexes** the `output_index` of each projection column to align with the ColumnFilter's
   narrowed output schema

## Interaction With Existing Sink Features

### With Row Diff

- ColumnFilter before RowDiff: diff operates on filtered columns ✅
- `output.delta.columns` validated against filtered schema ✅
- ColumnFilter is transparent — by-index rewrite still pushes projection to RowDiff ✅

### With Omit If Empty

`omit_if_empty` runs after RowDiff, so it sees the filtered column set. This is correct — empty
suppression should consider the columns the sink actually emits.

### With Batching

Batching runs after all column filtering and output mode stages. `StreamingEncoderRewrite` fuses
`Batch → SinkEncoder` as before.

### With By-Index Projection Rewrites

See the dedicated optimizer rule document:
[`column_filter_projection_intersection.md`](../../planner/optimize/physical/column_filter_projection_intersection.md)

### With Multi-Sink Pipelines

Column filtering is **per-sink**. Each sink independently specifies which columns to include or
exclude. The shared upstream `Project` remains unchanged and produces all columns.

## Configuration Layer

Column filtering is part of `SinkOutputConfig`:

- `include_columns: Option<Vec<String>>`
- `exclude_columns: Option<Vec<String>>`

It is not part of `CommonSinkProps` (batching) or connector-specific props.

## Planner Validation

At physical plan build time:

1. Validate `include_columns` and `exclude_columns` are not both set
2. Resolve column names against the upstream output schema
3. Reject if any column is not found in the schema
4. Reject if the resulting column set is empty

## Explain

`EXPLAIN` should show the `ColumnFilter` node with its include/exclude configuration and the
narrowed output schema:

```text
PhysicalColumnFilter: sink_id=mqtt_speed, include_columns=[speed, rpm], output=[speed, rpm]
```

After `ColumnFilterProjectionIntersection` optimization, the `ColumnFilter` node is **removed**
from the plan. `EXPLAIN` will show the shared `Project` connected directly to `RowDiff` (or
`Encoder`), with the projection spec reflecting only the filtered columns.

## Limitations / Follow-ups

- Column filtering operates on top-level columns only; nested struct field filtering is not
  supported in the first version
- The filter does not support column renaming or transformation — it is purely a select/drop
  operation
- Column order in `include_columns` mode follows the include list; in `exclude_columns` mode it
  preserves the original schema order
