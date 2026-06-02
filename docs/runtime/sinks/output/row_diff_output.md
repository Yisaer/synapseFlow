# Row Diff Output

## Background

Some downstream systems do not want a full row snapshot for every emitted record. Instead, they
want each emitted row to represent the difference from the previously emitted row.

In veloFlux, there is an additional requirement: some sink branches may publish decoded collections
that are consumed by downstream pipelines again, for example through memory collection topics.
Those paths need a **stable row schema**. A sparse patch object alone is not enough, because sparse
field absence breaks schema-oriented collection handling.

That leads to two requirements that must both be satisfied:

- compare the **final output row**
- preserve a **stable output schema**

This is the reason row diff output should not be modeled as a sparse row shape change.

## Decision

Row diff output should be modeled as a **sink-level output mode**, not as:

- a SQL scalar function
- a SQL stateful function
- an encoder transform

Why:

1. The comparison target should be the final projected row, after windowing, aggregation, filters,
   ordering, and final projection.
2. Different sinks of the same pipeline may need different delivery styles:
   - one sink may want full rows
   - another sink may want row diff rows
3. Encoder transforms are row formatting features, not previous-row comparison features.

The sink-side row diff design therefore has two layers:

- a **dense stable-schema diff row**
- an optional **output mask** carried as runtime metadata

The dense row keeps tuple and collection layout stable. The output mask lets some encoders omit
unchanged columns without losing the distinction between "unchanged" and "changed to `NULL`".

## Proposed API Shape

Add an optional `output` block under each sink:

```json
{
  "sinks": [
    {
      "id": "mqtt_delta",
      "type": "mqtt",
      "props": {
        "broker_url": "tcp://127.0.0.1:1883",
        "topic": "vehicle.delta"
      },
      "encoder": {
        "type": "json",
        "props": {}
      },
      "output": {
        "mode": "delta",
        "delta": {
          "columns": ["speed", "rpm"]
        }
      }
    }
  ]
}
```

Minimal shape:

- `output.mode`
  - `full` (default)
  - `delta`
- `output.delta.columns` (optional)
  - omitted: track all final output columns
  - present: only track the listed final output column names

The column names are resolved against the final projected output schema of the sink branch.

Still not part of the first version:

- entity keys / per-key diff state

The first version still compares against the **previous emitted row of the same sink branch**.

## Why Not SQL

SQL is the wrong layer for this capability because:

- SQL projection should define **which columns exist in the output row**
- row diff output should define **how that final row is delivered**
- SQL-level syntax would force one change-emission style onto every sink branch

That is too restrictive for multi-sink pipelines.

## Semantic Model

### Comparison Scope

For `output.mode=delta`, the sink branch maintains in-memory state for the previously emitted row of
that branch.

The first version is intentionally simple:

- comparison scope is the sink branch
- comparison is against the previous emitted row in sink output order
- there is no entity key partitioning yet

This means the first version is most suitable when the sink branch output is already a single
logical row sequence. A future keyed extension can be layered on top later.

### Comparison Target

The comparison target is the **final projected row** of the sink branch, before batching and before
encoding.

This means row diff naturally works on:

- plain `SELECT`
- post-window output
- post-aggregation output

without reusing pre-projection stateful function machinery.

### Tracked Columns

Tracked columns are resolved from the **final projected output schema** of the sink branch.

- if `output.delta.columns` is omitted, all final output columns are tracked
- if `output.delta.columns` is present, only those final output column names are tracked
- those names refer to final output names after aliasing, not upstream source names

For example:

- `SELECT a AS c, b AS d ...` with `columns=["c"]` tracks `c`
- `SELECT a, a + 1 AS x ...` with `columns=["x"]` tracks `x`

Columns that are **not** tracked still stay in the stable output schema. They are emitted as normal
current-row values on every row and remain selected in `output_mask`.

### Output Schema

Row diff does **not** change the planned output schema.

For every emitted row:

- column names stay the same
- column order stays the same
- the tuple / collection shape stays stable

This is a hard requirement so that collection-oriented sinks and downstream collection consumers can
keep working with a fixed row layout.

### First Row

For the first emitted row of a sink branch:

- emit the full current row values
- mark every column as selected in the output mask

### Later Rows

For each later row:

- compare the current full row with the previous full row for each tracked column
- if a tracked column changed:
  - write the current value into the output row
  - mark the column as selected in the output mask
- if a tracked column did not change:
  - write `NULL` into the output row
  - mark the column as unselected in the output mask
- if a column is not tracked:
  - write the current value into the output row
  - mark the column as selected in the output mask
- update the stored previous full row to the current full row after evaluation

### `NULL` And `output_mask`

The row diff row uses `NULL` as the fill value for unchanged columns, but `NULL` alone is not
enough to preserve full semantics.

Conceptually:

- unchanged column: `value = NULL`, `output_mask = false`
- changed-to-`NULL` column: `value = NULL`, `output_mask = true`
- changed-to-non-`NULL` column: `value = current value`, `output_mask = true`
- untracked column: `value = current value`, `output_mask = true`

This is the key design point:

- `NULL` preserves stable schema and runtime compatibility
- `output_mask` preserves diff intent for encoders that want sparse output

### Empty Diff Rows

Row diff output should still emit **one row per input row**.

If no column changed:

- emit a row whose values are all `NULL`
- mark every column as unselected in the output mask

Row diff should not silently drop the row at this layer. Dropping unchanged rows would change row
count semantics and make sink chaining harder to reason about.

### Conceptual Example

Assume the final projected output schema is:

```text
a, b
```

And the sink branch sees these full rows:

```json
{"a":1,"b":2}
{"a":1,"b":2}
{"a":null,"b":3}
```

The conceptual row diff result is:

1. First row
   - row values: `{"a":1,"b":2}`
   - mask: `a=true, b=true`

2. Second row
   - row values: `{"a":null,"b":null}`
   - mask: `a=false, b=false`

3. Third row
   - row values: `{"a":null,"b":3}`
   - mask: `a=true, b=true`

If a JSON object encoder uses the mask, it may serialize those rows as:

```json
{"a":1,"b":2}
{}
{"a":null,"b":3}
```

If an encoder ignores the mask, it will instead see the dense stable-schema rows:

```json
{"a":1,"b":2}
{"a":null,"b":null}
{"a":null,"b":3}
```

## Runtime Contract For `output_mask`

`output_mask` is **not** a SQL-visible column and is **not** part of the planned output schema.

It is runtime metadata associated with each output row:

- aligned with the final output schema column order
- intended for encoders or collection transports that can use it
- ignored by regular SQL evaluation

Implementation-wise, this metadata is better represented as a compact row-aligned structure such as
a bitset or `Vec<bool>`, not as a user-visible value column.

If a processor rebuilds tuples or collections after row diff, it must preserve the mask as part of
the runtime row contract. Otherwise the row diff semantics degrade to dense `NULL`-filled rows.

## Physical Placement

The row diff stage should sit after the final `Project` and before batching / encoding:

```text
... -> Project -> RowDiff -> Batch? -> Encoder? -> DataSink
```

This placement is important:

- after `Project`: diff sees the final sink-facing row shape
- before `Batch`: diff operates row by row, not batch by batch
- before `Encoder`: diff remains independent of JSON or any other bytes format

## Planner And Runtime Integration

### Configuration Layer

Row diff should remain a sink output feature, not an encoder option and not a batching option.

That means the configuration model should conceptually be:

- `PipelineSink`
  - `common`: batching and other shared sink controls
  - `connector`: connector kind and connector settings
  - `encoder`: encoder settings
  - `output`: output mode settings such as `full` / `delta`

The important point is not the exact Rust struct name, but the ownership boundary:

- do not hide row diff under `encoder`
- do not overload `CommonSinkProps`

### Physical Plan Node

The clean physical-plan shape is to introduce an explicit `RowDiff` node.

Conceptually:

```text
Project -> RowDiff -> Batch? -> Encoder? -> DataSink
```

That keeps the sink-side suffix readable and makes the capability visible in `EXPLAIN`.

The row diff node does not need to change output schema. It only needs enough metadata to evaluate
row-by-row differences, for example:

- sink branch identity
- output mode config
- final output column layout
- resolved tracked column names / indexes

### Physical Builder Changes

The most straightforward builder change is:

1. Start from the sink branch input child, which is currently the final `Project`.
2. If `sink.output.mode == delta`, resolve tracked columns from the child output schema and wrap
   that child in `PhysicalRowDiff`.
3. Feed the row diff output into the existing batch / encoder / sink construction path.

That means the builder sequencing becomes:

```text
input_child
  -> RowDiff? 
  -> Batch?
  -> Encoder?
  -> DataSink
```

This preserves the existing sink builder structure and avoids mixing row diff logic into encoder
construction.

`EXPLAIN` should show the resolved tracked columns on the physical node, for example:

```text
PhysicalRowDiff: sink_id=vehicle_sink, mode=delta, columns=[speed, rpm]
```

### Processor Shape

At runtime, `RowDiffProcessor` should be a normal row-preserving processor:

- input: `StreamData::Collection`
- output: `StreamData::Collection`
- row count in == row count out

The processor should iterate each incoming collection row in order, compare against stored previous
state, and build a new output collection whose rows:

- keep the same schema-aligned column structure
- contain diff values (`NULL` for unchanged)
- carry `output_mask`

The stored state must be the **previous full row snapshot**, not the previous diff row. Otherwise
the next comparison cannot correctly detect changes.

### Runtime Row Metadata

`output_mask` should be modeled as runtime row metadata, not as a SQL-visible value.

The most natural place for that contract is the row carrier itself:

- extend `Tuple` with optional row-output metadata
- keep the metadata opaque to SQL evaluation
- align the mask with final output schema column order

An implementation shape such as the following fits this requirement:

- `Tuple`
  - `output_mask: Option<Arc<RowOutputMask>>`
- `RowOutputMask`
  - column-aligned mask storage such as bitset or `Arc<[bool]>`

The exact type can still change, but the contract should remain:

- mask is optional
- mask is row-scoped
- mask is not a regular output column

### Propagation Rules

After row diff is introduced, downstream sink-side components fall into two groups.

Components that can consume the mask:

- `SinkEncoderProcessor`
- future mask-aware direct sink paths

Components that do not interpret the mask but must preserve it when rebuilding rows:

- `MemoryCollectionMaterializeProcessor`
- any future processor placed after `RowDiff` that reconstructs `Tuple`

Components that simply forward the same collection / rows without rebuilding tuples usually need no
special logic beyond not discarding row metadata.

### Memory Collection Path

The memory collection path is the main reason this design uses dense fixed-schema rows.

For this branch shape:

```text
Project -> RowDiff -> Batch? -> MemoryCollectionMaterialize -> DataSink
```

`MemoryCollectionMaterializeProcessor` currently rebuilds tuples into a normalized stable layout.
If row diff is enabled on that branch, it must also copy `output_mask` onto the rebuilt tuple.

If that propagation step is omitted:

- the collection topic still receives stable diff rows
- but the exact distinction between unchanged and changed-to-`NULL` is lost

So mask propagation on this path is not an optimization detail. It is part of the row diff runtime
contract.

### Encoder Contract

The first encoder contract should be intentionally small:

- encoders may ignore `output_mask`
- mask-aware encoders may use it to omit unchanged fields

For JSON object encoding, the expected behavior is:

- `mask = true`: serialize the field, even if the value is `NULL`
- `mask = false`: omit the field from the object

This lets JSON output express both:

- sparse patch-like payloads for downstream systems that want them
- dense stable-schema rows when the encoder chooses not to use the mask

### Template Contract

`encoder.transform=template` should not block row diff at the planner level, but exact semantics
depend on whether the template layer can observe the mask.

There are two practical phases:

1. Initial phase
   - template only sees row values
   - row diff + template degrades to dense `NULL`-filled rendering

2. Later phase
   - template context is extended with mask-aware input
   - template can selectively render changed fields

This should be documented as an encoder capability issue, not a row diff planning issue.

### Validation Guidance

The planner should validate row diff against the final sink path, not only the SQL plan.

For the first version, reasonable validation rules are:

- allow `output.mode=delta` on bytes/object paths whose encoder can consume or safely ignore the
  mask
- allow `output.mode=delta` on collection paths only when downstream transport preserves row
  metadata
- reject sink paths whose final delivery semantics treat dense `NULL` values as business data but
  cannot consume or preserve `output_mask`

This is the main reason connectors such as `kuksa` should stay out of the first version.

Current implementation note:

- sink-path capability validation is deferred
- the first runtime step focuses on row-diff semantics and metadata propagation, not planner-side
  rejection of incompatible sink paths

## Interaction With Existing Sink Features

### With batching

Batching remains a sink-side flush policy. Row diff should run before batching.

Result:

```text
Project -> RowDiff -> SinkEncoder -> SinkConnector
```

### With JSON encoding

For structured object encoders such as JSON:

- the encoder can use `output_mask` to omit unchanged columns
- a changed-to-`NULL` column must still be emitted when the mask selects it
- if the encoder ignores the mask, it will emit dense `NULL`-filled rows

This keeps row diff semantics available without forcing every encoder to implement sparse output on
day one.

### With `encoder.transform=template`

`encoder.transform=template` remains an encoder-local formatting feature. It is still orthogonal to
row diff at the plan level:

- row diff runs before the encoder
- the template consumes whatever row representation the encoder exposes

However, semantic fidelity depends on whether the encoder transform can see the mask:

- if the transform can read `output_mask`, it can preserve row diff intent
- if it can only read row values, it only sees the dense `NULL`-filled row and cannot distinguish
  "unchanged" from "changed to `NULL`"

So row diff and template are architecturally orthogonal, but exact sparse rendering requires a
mask-aware encoder contract.

### With memory collection topics

Stable-schema row diff is specifically designed to keep collection-oriented sink chaining possible.

That means the following shape should remain conceptually valid:

```text
Pipeline A
  -> RowDiff
  -> Memory sink (collection topic)

Pipeline B
  <- Memory source (collection topic)
```

In that model:

- the row values keep a fixed schema
- the collection transport should preserve `output_mask` as runtime row metadata if downstream
  consumers need exact row diff intent

If the mask is not preserved, downstream consumers still see a stable dense row stream, but they no
longer have the full distinction between unchanged and changed-to-`NULL`.

### With direct collection sinks that interpret `NULL` as data

Some direct collection sinks may treat `NULL` as an actual business value to be written
downstream. If they do not understand or preserve `output_mask`, row diff is not safe for them.

That means the planner should reject row diff on sink paths whose final delivery behavior cannot
either:

- consume `output_mask`, or
- safely preserve it for downstream consumers

This is why connectors such as `kuksa` are poor candidates for the first version.

### With by-index projection into encoder rewrite

For the first version, `ByIndexProjectionIntoEncoderRewrite` should be disabled on sink branches
where `output.mode=delta` is enabled.

Reason:

- row diff needs materialized final values for comparison
- the by-index rewrite intentionally delays value materialization into the encoder

Keeping both in the first iteration would complicate semantics and planner behavior unnecessarily.

### With multi-sink pipelines

Row diff state is per sink branch.

This means:

- one sink can stay in `full` mode
- another sink can use `delta` mode
- their previous-row state must remain independent

## Supported Scope For The First Version

The first version should focus on the following scope:

- sink-level `output.mode=delta`
- optional `output.delta.columns` resolved against the final output schema
- comparison against the previous emitted row of the same sink branch
- fixed-schema dense diff rows
- unchanged columns filled with `NULL`
- runtime `output_mask` metadata for encoders or transports that can use it

Not part of the first version:

- keyed diff state
- nested struct / list field-level diff
- persisted diff state across pipeline restart
- planner-side sink path capability validation
- full sink-path end-to-end test matrix in the same delivery step

## Suggested Delivery Plan

The implementation can be staged in a way that keeps semantics correct while limiting the first
change set.

### Stage 1

- add sink-side `output.mode=delta` configuration
- add optional `output.delta.columns`
- add explicit `PhysicalRowDiff` and `RowDiffProcessor`
- produce dense fixed-schema diff rows with `NULL` fill
- add `output_mask` to runtime row metadata
- preserve the mask on sink paths that rebuild tuples, especially memory collection materialization
- disable `ByIndexProjectionIntoEncoderRewrite` on row diff branches
- keep sink encoder delivery available

At this point, row diff semantics already exist at the runtime row contract level, even if some
encoders still choose to emit dense rows.

### Stage 2

- make JSON encoder mask-aware
- extend mask-aware behavior to other encoder / sink paths that can benefit from sparse output

This stage unlocks sparse JSON output without sacrificing stable collection shape.

### Stage 3

- extend template input contract if needed
- add keyed diff state if the business case requires entity partitioning

This sequence keeps the first implementation small while preserving the architecture needed for the
full design.

## Relation To `CHANGED_COLS`

This design addresses the same business problem as a multi-column change-emission function, but it
does so at the architectural layer that fits veloFlux better:

- sink-side
- per branch
- after final projection
- with stable row schema preserved
