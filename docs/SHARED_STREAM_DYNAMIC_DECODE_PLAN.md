# Shared Stream Dynamic Projection Decode (Plan)

## Background

Today a shared stream ingests and decodes the full source schema for every incoming payload (e.g. `a,b,c`) and broadcasts the decoded `Tuple` to all pipelines.

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

## Registry State (Applied Only)

We keep only **applied** projection information in shared stream info:

- `decoding_columns` (applied): the column set that the shared stream decoder is *currently* decoding.

We intentionally do not expose “desired/target” projection in the public info; the info must reflect the decoder’s real applied state.

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

Decoder-specific notes:

- JSON decoder can skip building `Value` for unused keys by extracting only the needed keys.
- If a decoder cannot efficiently skip work, it may fall back to full decode, but still should update `decoding_columns` accordingly.

## Pipeline Startup “Readiness” Without Explicit Ack

We avoid explicit ack/versioning by using a simple polling loop against the **applied** state:

1. Pipeline computes `required_columns` for the shared stream.
2. Pipeline registers `required_columns` in the shared stream registry.
3. Before starting to process incoming tuples, pipeline loops:
   - read shared stream info (`decoding_columns`)
   - wait until `required_columns ⊆ decoding_columns`
   - `sleep` briefly and retry, with a timeout

This guarantees the decoder applied the projection needed by the pipeline.

## Implementation Steps (Suggested Order)

1. Column requirement extraction
   - Derive per-source required columns for a pipeline from its SQL plan (including projection/filter/aggregation/window/encoder needs).
   - Treat wildcard as ALL.
2. Registry extensions
   - Store `consumer_id -> required_columns` for each shared stream.
   - Compute union on changes.
   - Expose shared stream info field `decoding_columns` (applied).
3. Ingest loop projection
   - Apply union projection to decoder.
   - Update `decoding_columns` after applying.
4. Pipeline startup guard
   - Poll `decoding_columns` until it covers the pipeline’s required set.
5. Tests
   - Two pipelines requiring disjoint columns must both work without wrong-index reads.
   - `SELECT *` must force full decode.

