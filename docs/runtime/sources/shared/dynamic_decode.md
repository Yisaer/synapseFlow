# Shared Stream Dynamic Projection Decode (Plan)

## Background

Today a shared stream ingests and decodes the full source schema for every incoming payload (e.g. `a,b,c`) and fans the decoded `Tuple` out to all pipelines.

This is correct but wastes CPU and memory when multiple pipelines consume the same shared stream while each only uses a subset of columns. Example:

- Pipeline A: `SELECT a FROM shared_stream`
- Pipeline B: `SELECT b FROM shared_stream`

Even though only `{a,b}` are needed globally, the shared stream still decodes `{a,b,c}` for every message.

## Core Constraint: `ColumnRef::ByIndex` Must Stay Correct

The execution engine relies heavily on `ColumnRef::ByIndex(source_name, column_index)`, where `column_index` is compiled based on the *full* source schema order.

Therefore, the shared stream output must preserve the full-schema index semantics across all pipelines. We cannot change the meaning of `column_index` per pipeline.

## Chosen Approach (Option B): Position-Preserving Partial Decode

We implement “decode fewer columns” while preserving the full schema index space:

- The shared stream maintains a **full schema** (e.g. `[a,b,c]`) with stable column indices.
- At runtime, it decodes only a **projection** of columns required by all running consumers (the union).
- For columns not decoded, downstream semantics are **`NULL`**.

This ensures any pipeline compiled with `ByIndex` continues to read the correct column by index.

## Source-Of-Truth: SQL-Semantic Required Columns

For shared streams, the authoritative “which columns does this pipeline need?” answer must come from the SQL-semantic plan (logical plan), not from a later runtime/processor scan.

However, because shared streams must keep the full schema to preserve `ByIndex`, this information must be represented as a **projection view** (a list of column names), not as a schema shrink/rewrite.

Concretely:

- `DataSource.schema` remains the full stream schema for shared sources.
- `DataSource.shared_required_schema` (name TBD) stores the per-pipeline required top-level columns for shared sources as `Vec<String>`.
- `EXPLAIN` for shared sources prints `shared_required_schema` (the view/projection) rather than the full schema (which is always the stream full schema).
- Physical planning propagates the list into `PhysicalSharedStream`, and processor building uses it to call `SharedStreamProcessor.set_required_columns(...)`.

This removes the need to recompute shared required columns during `build_processor_pipeline`.

## Scope: Top-Level Columns Only (Initial)

For the initial iteration, only `TopLevelColumnPruning` contributes to `shared_required_schema`.

- `StructFieldPruning` and `ListElementPruning` remain disabled for shared sources.
- Nested pruning for shared streams requires a separate “union nested projection” mechanism and is out of scope here.

## Required Behavior

1. Dynamic union
   - The shared stream decodes only the union of required columns across all running pipelines.
2. Stable indices
   - Output still behaves as if it has the full schema index space.
   - Undecoded columns evaluate to `NULL`.
3. Startup readiness (no race)
   - When a new pipeline starts consuming a shared stream, the shared decoder must apply the union projection *before* the pipeline begins processing data (to avoid initial `NULL`s).
4. Shutdown shrink
   - When a pipeline stops, the shared stream should recompute the union and reduce decoding accordingly.
5. Wildcard forces full decode
   - If any consumer uses `SELECT *` / `source.*`, the required columns are **ALL**, so the shared stream must fully decode.
6. No-drop fan-out
   - Shared-stream delivery to consumers must be backpressured and must not drop messages.
7. Explicit lifecycle only
   - Runtime errors must not stop the shared-stream runtime implicitly.
   - Consumer attach/detach may start or stop the running shared-stream runtime instance without
     deleting the shared stream definition itself.

## Registry State (Applied Only)

We keep only **applied** projection information in shared stream info:

- `decoding_columns` (applied): the column set that the shared stream decoder is *currently* decoding.

We intentionally do not expose “desired/target” projection in the public info; the info must reflect the decoder’s real applied state.

Internally, the shared stream also caches a compiled `DecodeProjection` for the currently applied
`decoding_columns`. This is not part of the public info, but is used to avoid rebuilding projection
state on the decoder hot path.

## Consumer Registration and Union Computation

The registry maintains:

- `consumer_id -> required_columns`

On every add/remove/update, the registry recomputes:

- `union_required_columns = ⋃ required_columns(consumer)`

Then it notifies the shared stream ingest loop to apply the new projection.

Internally, representing columns by **index** is recommended for speed and to align with `ByIndex`.

## Decoder Application and Info Update

The shared stream ingest loop:

1. Receives updated `union_required_columns`
2. Applies projection to the decoder (decode only those columns)
3. Updates shared stream info:
   - `decoding_columns = union_required_columns` (applied)
4. Emits decoded tuples with full-index semantics:
   - for undecoded columns, returns `NULL`
5. Keeps the shared stream definition installed while allowing the running runtime instance to be
   reclaimed when no consumers remain

Hot path optimization:

- The shared stream maintains a cached `Arc<DecodeProjection>` corresponding to the applied
  `decoding_columns`.
- When `decoding_columns` changes, the shared stream rebuilds the projection once and bumps
  `DecodeProjection.version`.
- `DecoderProcessor` reads the cached `Arc<DecodeProjection>` and passes `Some(&DecodeProjection)`
  into `RecordDecoder::decode_with_projection(...)`, avoiding per-message cloning/rebuilding of
  projection structures.
- Decoder implementations that need to cache projection-dependent state can use
  `DecodeProjection.version()` to detect changes and refresh their caches.

Decoder-specific notes:

- JSON decoder can skip building `Value` for unused keys by extracting only the needed keys.
- If a decoder cannot efficiently skip work, it may fall back to full decode, but still should update `decoding_columns` accordingly.

## Pipeline Startup “Readiness” Without Explicit Ack

We avoid explicit ack by using a simple polling loop against the **applied** state:

1. Pipeline obtains `required_columns` for the shared stream from its logical plan metadata (`shared_required_schema`).
2. Pipeline registers `required_columns` in the shared stream registry.
3. Before starting to process incoming tuples, pipeline loops:
   - read shared stream info (`decoding_columns`)
    - wait until `required_columns ⊆ decoding_columns`
   - `sleep` briefly and retry, with a timeout

This guarantees the decoder applied the projection needed by the pipeline.

## Implementation Steps (Suggested Order)

1. Extend logical `DataSource` metadata
   - Add `shared_required_schema: Option<Vec<String>>` (or similar) to `LogicalPlan::DataSource`.
   - Populate it only when the source is `SourceBindingKind::Shared`.
2. Update `TopLevelColumnPruning` to fill `shared_required_schema`
   - Keep current expression traversal and wildcard/ambiguity handling.
   - For shared sources: do not shrink `schema`; instead, record the computed required column list in `shared_required_schema` (use full column list when “ALL” is required).
3. Update explain rendering
   - For shared sources (`LogicalPlan::DataSource` and `PhysicalSharedStream`), print the `shared_required_schema` view (not the full schema).
   - Ensure the output makes it clear this is a projection/view, not a schema rewrite (indices remain full-schema).
4. Propagate to physical planning
   - Carry `shared_required_schema` into `PhysicalSharedStream` (and the `explain_ingest_plan` if needed for consistency).
5. Processor building: remove recomputation
   - Delete `compute_shared_required_columns(...)` (or stop using it).
   - When building `SharedStreamProcessor`, read `required_columns` directly from the `PhysicalSharedStream` metadata and call `set_required_columns`.
6. Regression tests
   - Two pipelines requiring disjoint columns must both work without wrong-index reads.
   - `SELECT *` / `source.*` must force full decode.
   - `EXPLAIN` for shared sources must show the per-pipeline projected column list.
   - Removing the last consumer must stop the running shared-stream runtime instance without
     removing the shared stream definition.
   - A slow consumer must backpressure shared-stream fan-out instead of losing messages.

---

# Projected (Sparse) Message Encoding

## Motivation

Dynamic decode projection already ensures the shared stream decoder only **decodes** the union of
required columns across active consumers. However, the resulting `Message` values container was
still **dense** (full source-schema width), with un-decoded slots filled with `Value::Null`:

- For schemas like CAN / GBF with hundreds of columns, even when only 3 columns are actively
  decoded, every `Message` allocates, initialises, writes, and drops a full-width `Vec<Arc<Value>>`.
- Downstream processors and encoders iterate across this full-width structure, paying per-message
  cost proportional to schema width rather than active column count.

Projected message encoding addresses this by introducing a **sparse physical representation**
while keeping the source-schema logical index space unchanged.

## Design

### `ProjectedLayout`

A shared mapping table computed once per projection generation:

```rust
pub struct ProjectedLayout {
    /// source-schema logical index → compact physical index (None = not decoded)
    pub logical_to_physical: Arc<[Option<usize>]>,
    /// compact physical index → source-schema logical index
    pub physical_to_logical: Arc<[usize]>,
}
```

### `MessageValues`

```rust
pub enum MessageValues {
    /// Default: values[i] = source-schema column i (existing behaviour).
    Dense(Vec<Arc<Value>>),
    /// Sparse: only actively-decoded columns stored.
    Projected {
        values: Vec<Arc<Value>>,
        layout: Arc<ProjectedLayout>,
    },
}
```

### Key invariants

- **`keys` stays full source-schema width.** `Message.keys` always represents the complete source
  schema, regardless of physical storage mode.
- **`entry_by_index(index)` and `value_by_index(index)` accept source-schema logical indices.**
  For projected messages, the lookup translates via `logical_to_physical[index]`.
- **`entries()` enumerates only materialised columns** for projected messages. It does not
  full-expand to schema width.
- **No per-row allocation.** The `ProjectedLayout` is built once when the active decode projection
  changes and shared via `Arc` across all messages within the same generation.
- **Layout lifecycle is per-generation.** Each time the active decode columns change
  (pipeline attach/detach), a new `ProjectedLayout` is computed and attached to a new
  `DecodeProjection`. Messages decoded with the old layout retain a reference to it, so
  in-flight messages in processor queues are never mis-interpreted.

### Example

Source schema `[a, b, c]` (logical indices 0, 1, 2):

| Event | Active decode | layout.logical_to_physical | Message values |
|---|---|---|---|
| A starts (needs a) | `[a]` | `[Some(0), None, None]` | `[a_val]` |
| B starts (needs b) | `[a, b]` | `[Some(0), Some(1), None]` | `[a_val, b_val]` |
| C starts (needs c) | `[a, b, c]` | `[Some(0), Some(1), Some(2)]` | `[a_val, b_val, c_val]` |
| B stops | `[a, c]` | `[Some(0), None, Some(1)]` | `[a_val, c_val]` |

Access semantics for `layout_v4` (`[a, c]`):
- `value_by_index(0)` → physical 0 → `a_val`
- `value_by_index(1)` → `None` (column b not decoded)
- `value_by_index(2)` → physical 1 → `c_val`

### Layout computation

The `ProjectedLayout` is computed inside `SharedStreamInner::set_applied_decoding_columns`, which
is called every time a pipeline attaches or detaches (changing the union required columns). It is
stored on the `DecodeProjection` via `with_projected_layout()`. The decoder reads it from the
projection; if present it produces `MessageValues::Projected`, otherwise it falls back to Dense.

This means the layout is recomputed only when the column set changes — not on every inbound
payload.

## Feature flag

The optimisation is gated by `SharedStreamConfig.use_projected_messages` (default `false`).

When enabled, the shared stream runtime attaches the projected layout to the decode projection.
The decoder then outputs compact `MessageValues::Projected`. When disabled, the existing
full-width Dense behaviour is preserved.

Exposed via REST API:

```json
{
  "name": "my_stream",
  "type": "mqtt",
  "shared": true,
  "use_projected_messages": true,
  ...
}
```

## Compatibility

- **`ColumnRef::ByIndex(source, index)`** continues to work unchanged. The index space is the
  full source schema.
- **Encoders** using `ByIndexProjection` access columns via `value_by_index(logical_index)` and
  are unaffected.
- **Encoders** iterating `Tuple::entries()` see only materialised columns in projected mode —
  this is the intended behaviour (avoids full-width traversal).
- **`collection_layout_normalize`** and **`output_row_accessor`** use `key_index()` to resolve
  source-schema logical indices, working correctly for both Dense and Projected.
- **Non-decoded columns** return `None` from `value_by_index` / `value`, matching the semantics
  of "column was never decoded" rather than "decoded as Null".
