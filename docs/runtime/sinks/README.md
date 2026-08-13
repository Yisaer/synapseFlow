# Sink And Output Model

## Purpose

This directory documents sink-side behavior in veloFlux:

- which sink connectors are built into the runtime
- how sink encoders are attached
- which sink-level common properties are applied before delivery
- how sink-level retry is handled
- how planner/physical plan stages are ordered near the sink boundary
- where future sink-side output features should live

The goal is to keep connector semantics, encoder semantics, and sink-side plan behavior clearly
separated.

## Layer Model

At a high level, sink-side delivery in veloFlux is split into five layers:

1. **Sink connector**
   - Delivers the final payload to an external system or in-process destination.
   - Examples: `mqtt`, `memory`, `kuksa`, `nop`.

2. **Sink encoder**
   - Converts a `Collection` into bytes when the connector expects bytes payloads.
   - Examples: `json`, `csv`, `none`.

3. **Delivery transforms**
   - Transform encoded delivery bytes before they reach the connector.
   - Examples: gzip/zstd compression and AES-GCM encryption.
   - The encoder creates one message unit that remains the same unit through transforms,
     `SinkProcessor`, and the connector; delivery names describe its lifecycle, not a new metric
     unit.
   - Collection-native sinks produce the same message unit without serializing it. The collection
     remains the payload representation, but it is one message after the sink boundary.

4. **Sink retry**
   - Retains a failed encoded delivery at the `SinkProcessor` layer and replays the full connector
     delivery sequence after backoff.
   - See `retry.md`.

5. **Common sink properties**
   - Planner-managed sink-side behavior shared across connectors.
   - Today this mainly means sink batching.

These layers are modeled independently on purpose:

- one connector may work with multiple encoders
- one encoder may be reusable across multiple connectors
- common sink properties should not be reimplemented inside each connector

## Current Built-In Runtime Sink Connectors

The runtime currently registers these built-in sink connector kinds:

| Connector | Runtime built-in | Accepts bytes | Accepts collection | Notes |
|----------|-------------------|---------------|--------------------|-------|
| `mqtt` | yes | yes | no | Intended for encoded payloads such as JSON bytes. |
| `nop` | yes | yes | no | Useful for tests, benchmarks, and plan validation. |
| `kuksa` | yes | no | yes | Consumes decoded rows directly; pipeline API forces `encoder=none`. |
| `memory` | yes | yes | yes | Topic kind depends on whether an encoder is present. |

Additional notes:

- A `mock` sink connector implementation exists in the codebase, but it is a test utility and is
  not registered as a normal built-in runtime connector.
- A custom connector may support bytes, collections, or both, depending on its own implementation.

## Current Built-In Sink Encoders

The runtime currently registers the following built-in encoder kinds:

| Encoder | Runtime built-in | Output type | Streaming support | Notes |
|--------|-------------------|-------------|-------------------|-------|
| `json` | yes | bytes | yes | Encodes a `Collection` as a JSON array payload and supports encoder-local JSON formatting options. |
| `csv` | yes | bytes | yes | Encodes the fixed output layout as UTF-8 CSV and supports configurable delimiter and per-delivery headers. |
| `none` | planner pseudo-mode | collection passthrough | n/a | No encoder node is built; the connector receives decoded collections directly. |

Current transform support:

- `encoder.transform=template` is only supported for `encoder.type=json`.
- `encoder.type=csv` supports `props.delimiter` and `props.header`, requires `output.mode=full`, and
  does not support encoder transforms.
- The transform is item-level (`row -> transformed JSON item`), not a standalone collection
  transform stage.
- When `encoder.type=none`, any configured transform is ignored by design.
- `encoder.props.omit_null_columns` is a JSON-encoder-local option that controls omission of
  `null` object fields during native JSON object encoding.

See also:

- [CSV Sink Encoder](encoders/csv.md)
- [Encoder Transform](encoders/encoder_transform.md)
- [Delivery Compression](delivery/compress.md)
- [Delivery Encryption](delivery/encrypt.md)
- [JSON Null Field Omission](encoders/json_null_column_omit.md)
- [Omit If Empty](output/omit_if_empty.md)
- [Row Diff Output](output/row_diff_output.md)

## Current Common Sink Properties

Today the planner exposes one common sink property group:

- `batch_count`
- `batch_duration`

These are modeled as sink-level flush controls, not connector-specific controls.

Behavior:

- encoded-byte sinks carry batching settings on `PhysicalSinkEncoder`
- direct collection sink paths use `PhysicalBatch` for the same sink-level batching semantics
- `batch_duration` flushes on a fixed millisecond processing-time grid anchored when the processor
  starts
- duration windows are left-closed/right-open; a row arriving on a duration boundary belongs to the
  next window
- the first duration window can be partial because the grid is independent of the first row arrival
- when both `batch_count` and `batch_duration` are set, reaching `batch_count` flushes before the
  next duration boundary

## Current Connector-Specific Rules

### MQTT

- Intended for bytes payloads.
- In practice this means an encoder should be present.
- If a pipeline uses `encoder.type=none` with `mqtt`, the connector will receive collection
  payloads and reject them at runtime because MQTT sink does not implement collection delivery.

### Nop

- Intended for bytes payloads.
- Useful as a sink-side terminator for tests and benchmarks.
- Like MQTT, it does not implement collection delivery.

### Kuksa

- Requires decoded row access.
- The pipeline API forces `encoder=none`.
- Physical planning builds no encoder node for Kuksa sinks.

See also:

- [Kuksa Sink](connectors/kuksa.md)

### Memory

- Supports both bytes topics and collection topics.
- When an encoder is present, the sink publishes bytes.
- When `encoder.type=none`, the sink publishes collections.
- For collection topics, the planner inserts a dedicated materialization stage to normalize row
  layout before the sink.

See also:

- [Memory Sink](connectors/memory.md)

## Current Plan Order

### Main query pipeline

Before any sink-specific suffix is attached, the current query pipeline shape is:

```text
DataSource
  -> StatefulFunction? 
  -> Window?
  -> Aggregation?
  -> Filter?
  -> Order?
  -> Project
```

The exact middle stages depend on the query, but the important point for sink work is:

- the relational/query pipeline currently ends at the final `Project`
- sink-specific behavior is attached after that point, per sink branch

### Per-sink branch suffix

After the final `Project`, each sink branch is built independently.

#### Encoded bytes path

Without batching:

```text
Project -> SinkEncoder -> SinkConnector
```

With batching:

```text
Project -> SinkEncoder -> SinkConnector
```

#### Direct collection path

For direct collection sinks (`encoder.type=none`), no encoder node is built.

Generic shape:

```text
Project -> Batch? -> DataSink
```

Kuksa path:

```text
Project -> Batch? -> DataSink
```

Memory collection path:

```text
Project -> Batch? -> MemoryCollectionMaterialize -> DataSink
```

### Multi-sink pipelines

In multi-sink pipelines:

- all sink branches share the same upstream relational/query pipeline
- sink-specific suffixes are attached per branch after the shared `Project`
- physical rewrites must respect shared DAG constraints

Each sink branch receives its own final `OutputLayout`. Shared upstream nodes
must expose a compatible tuple layout to every branch; incompatible fan-in
layouts are rejected during planning.

## Current Sink-Side Related Capabilities

These capabilities already exist near the sink boundary:

1. **Sink batching**
   - Planner-managed flush grouping using `Batch`.

2. **JSON encoder transform**
   - `encoder.type=json`
   - `encoder.transform=template`
   - implemented inside the JSON encoder, not as a standalone plan stage.

3. **JSON null-field omission**
   - `encoder.type=json`
   - `encoder.props.omit_null_columns`
   - implemented inside the JSON encoder's native object-formatting path.

4. **Sink encoder lowering**
   - Lowers encoded-byte sinks to `SinkEncoder -> SinkConnector`, with sink-side batch settings handled inside `SinkEncoder`.

5. **Plan-fixed output layout**
   - Gives encoders, row-diff, and memory materialization one ordered output
     contract with fixed tuple value references.

6. **Memory collection materialization**
   - Normalizes collection rows into a stable layout before publishing to memory collection topics.

Proposed / documented sink-side capabilities:

7. **Empty-result suppression**
   - `output.omit_if_empty`
   - modeled as a sink-side output policy, not as an encoder or connector behavior

## Design Guideline For New Sink-Side Features

When adding new sink-side features, prefer to classify them explicitly as one of:

- connector capability
- encoder capability
- common sink property
- sink output mode
- physical optimization

Avoid hiding one category inside another unless the semantics are genuinely encoder-local or
connector-local.

In particular:

- row-level stateful output shaping should not be modeled as SQL scalar/stateful functions
- encoder-local byte formatting should stay inside encoders
- connector transport rules should stay inside connectors

## Related Documents

- [Memory Sink](connectors/memory.md)
- [Kuksa Sink](connectors/kuksa.md)
- [Omit If Empty](output/omit_if_empty.md)
- [Row Diff Output](output/row_diff_output.md)
- [Column Filter](output/column_filter.md)
- [Encoder Transform](encoders/encoder_transform.md)
- [JSON Null Field Omission](encoders/json_null_column_omit.md)
- [Plan-Fixed Output Layout](../../planner/performance/plan_fixed_output_slots.md)
