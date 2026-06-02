# Encoder Transform

This document lives under `docs/runtime/sinks/` because encoder transform is a sink-side output
capability.
It is encoder-local, but it only applies on sink branches and must be understood together with
batching, sink delivery, and other sink-side output features.

## Background

We need sink-side payload reshaping for a common scenario:

- SQL produces a row-shaped result, for example:

```sql
SELECT a, b FROM stream
```

- Sink batching is still configured at the collection level.
- The final MQTT payload should still be a JSON array.
- Each row item inside that array needs to be reshaped before encoding.

Example:

- Input rows:

```json
[{"a":1,"b":2},{"a":1,"b":2}]
```

- Desired payload:

```json
[{"c":1,"d":2},{"c":1,"d":2}]
```

This document records the current design decision for this capability.

## Current Decision

We do not introduce a standalone sink-side `collection -> collection` transform plan for the
current iteration.

Instead, we extend the JSON sink encoder with an optional transform:

- `encoder.type = json`
- `encoder.transform = template`

The transform is **item-level**, not payload-level:

- The transform consumes the current SQL output row.
- The transform produces one transformed JSON item.
- The outer JSON array, separators, batching, and final payload bytes remain the responsibility of
  the JSON encoder.

This keeps the common sink-batch scenario simple and memory-efficient.

## Why This Shape

We considered two broader directions:

- A standalone sink `row transform` plan (`collection -> collection`)
- A collection-level encoder transform (`collection -> bytes`)

For the current common case, neither is the best fit:

- A standalone `collection -> collection` transform needs a schema-oriented transform DSL and
  creates another plan stage before the encoder.
- A collection-level template transform makes `append(row)` semantics unclear for streaming
  encoding, because the template would conceptually consume the whole collection.

By making the transform an item-level capability of the JSON encoder, we get:

- Clear batching semantics: batching still happens at the collection level.
- Clear streaming semantics: each appended row is transformed into one JSON item.
- Lower memory usage: no need to materialize a full transformed collection before encoding.
- A direct path to support the current business scenario.

## Scope

This document only defines the current encoder transform MVP:

- Supported encoder kind: `json`
- Supported transform kind: `template`
- Supported transform contract: current row -> one JSON item

This document does **not** define:

- A generic sink `collection -> collection` transform stage
- A generic collection-level template renderer
- Bytes-to-bytes sink post-processing
- Arbitrary multi-message fan-out

## Configuration Shape

The intended configuration model is:

```json
{
  "encoder": {
    "type": "json",
    "transform": {
      "template": "{\"c\":{{ json(.row.a) }},\"d\":{{ json(.row.b) }} }"
    }
  }
}
```

The exact config field names may evolve, but the semantics should stay:

- `type = json` means the outer payload is still encoded by the JSON encoder.
- `transform` currently means a template-based transform.
- The template input context is the current SQL output row under `.row`.
- On row-diff sink branches, `.row` remains the dense current output row shape; unchanged tracked
  columns appear as `null` unless a future mask-aware template contract is introduced.
- `type = none` means no encoder node is built, so any configured `transform` is ignored / has no effect.

## Template Contract

The row template is interpreted as a renderer for a single JSON item.

Current constraints:

- `.row` must refer to the current SQL output row.
- The rendered result must be one valid JSON item.
- For the first iteration, it is recommended to restrict the output to one valid JSON object.

Example:

```text
{"c":{{ json(.row.a) }},"d":{{ json(.row.b) }} }
```

Given the input row:

```json
{"a":1,"b":2}
```

The row template produces:

```json
{"c":1,"d":2}
```

The JSON encoder then appends that item into the outer collection payload.

The runtime template engine is `upon`, and the current implementation exposes a
single helper:

- `json(value)`: render a value as a JSON literal inside the template output

## Execution Semantics

### Without Sink Batching

If sink batching is disabled:

- The encoder receives one input collection.
- It iterates all rows in that collection.
- For each row:
  - Render one transformed JSON item from the row template.
  - Append that item into the output JSON array.
- The encoder emits one JSON payload.

Conceptually:

```text
collection
  -> for each row: template(row) => json item
  -> encode all items as one json array
  -> bytes
```

### With Sink Batching

If sink batching is enabled:

- Batching still groups rows into one flush unit.
- The transform is still evaluated row by row.
- The JSON encoder still emits one payload per flush unit.

Example:

- `batch.count = 2`
- rows:
  - `{"a":1,"b":2}`
  - `{"a":1,"b":2}`

The emitted payload is:

```json
[{"c":1,"d":2},{"c":1,"d":2}]
```

## Behavior Without Physical Optimizations

Physical shape:

```text
Project -> Batch(optional) -> Encoder(json + row template transform) -> Sink
```

Runtime behavior:

1. `Project` materializes the SQL output rows.
2. `Batch` groups rows if batching is configured.
3. `Encoder` walks the input collection.
4. For each row, the encoder renders one transformed JSON item from the template.
5. The encoder writes all transformed items into one JSON array payload.
6. `Sink` publishes the final bytes.

Properties:

- Semantics are simple and explicit.
- Correctness is straightforward.
- Memory usage is acceptable for the common case because we only build the final payload buffer,
  not an extra transformed collection.

## Behavior With `PhysicalSinkEncoder`

`PhysicalSinkEncoder` remains valid for this design.

Reason:

- The transform is row-based.
- The encoder still emits exactly one payload per flush unit.
- Therefore `append(row)` has a clear meaning:
  - Render the current row through the row template.
  - Encode the transformed item into the current JSON array buffer.

Physical shape:

```text
Project -> SinkEncoder(json + row template transform) -> Sink
```

Runtime behavior:

1. The streaming encoder starts an output buffer for the current flush unit.
2. For each appended row:
   - Build the row render context from the current SQL output row.
   - Render one transformed JSON item.
   - Append that item into the current JSON array buffer.
3. When batch count or duration is reached:
   - Close the JSON array.
   - Emit one payload bytes buffer.
   - Reset streaming state.

This is effectively:

```text
append(row)
  -> template(row) => transformed json item
  -> append item into current json payload buffer
```

Key point:

- The transform is row-based.
- The payload is still collection-based.
- That is why streaming append semantics are well-defined.

## Behavior With `ByIndexProjectionIntoEncoderRewrite`

`ByIndexProjectionIntoEncoderRewrite` is not supported when `encoder.transform=template` is
enabled.

Reason:

- The template is treated as a dynamic row-to-item renderer.
- The planner cannot reliably derive the exact input column dependency set from the template.
- The rendered item shape is defined by the template at runtime, not by a planner-visible schema.
- Therefore the planner cannot safely rewrite the upstream `Project` into passthrough mode for this
  case.

Physical shape remains:

```text
Project -> SinkEncoder(json + row template transform) -> SinkConnector
```

Runtime implication:

1. `Project` still materializes the SQL output row.
2. The encoder transform consumes that materialized row.
3. The template renders one transformed JSON item.
4. The JSON encoder writes that item into the final payload.

This means the delayed-materialization CPU saving from `ByIndexProjectionIntoEncoderRewrite` does
not apply to `encoder.transform=template`.

## Behavior With By-Index Projection Enabled

When `encoder.transform=template` is enabled, by-index projection into the encoder is not
applicable because the template needs the materialized output row.

Physical shape:

```text
Project -> SinkEncoder(json + row template transform) -> Sink
```

Per-row execution becomes:

1. Materialize the projected SQL output row.
2. Render the current row into one transformed JSON item.
3. Append that item into the current JSON payload buffer.

Flush execution becomes:

1. Close the JSON array.
2. Emit one payload.
3. Reset encoder state.

This path still avoids the extra standalone batch-then-encode buffering through streaming append, but it does
not remove project-side row materialization.

## Unsupported Cases In This MVP

The current design does not attempt to support the following under encoder transform:

- A template whose input context is the whole collection
- A template whose output is multiple payloads for one flush unit
- A template whose output is not a valid JSON item for the JSON encoder
- A transform that changes payload framing outside the JSON encoder's ownership

Those cases need a different abstraction and should not be mixed into this MVP.

## Summary

The current encoder transform design is:

- Keep `encoder.type = json`
- Add `encoder.transform` as an optional template renderer
- Interpret the template as `row -> transformed JSON item`
- Keep outer JSON array encoding inside the JSON encoder
- Keep sink batching at the collection level

Under this design:

- No optimization: supported
- `PhysicalSinkEncoder`: supported
- `ByIndexProjectionIntoEncoderRewrite`: not supported when `encoder.transform=template` is enabled

This provides a practical path for the current sink-batch use case without introducing a separate
collection transform plan in the first iteration.
