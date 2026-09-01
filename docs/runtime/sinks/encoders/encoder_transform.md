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
- The final MQTT payload uses the JSON encoder's configured delivery format.
- Each row item needs to be reshaped before delivery framing is applied.

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
- Array/NDJSON framing, separators, batching, and final payload bytes remain the responsibility of
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

- `type = json` means the final payload is still encoded and framed by the JSON encoder.
- `transform` currently means a template-based transform.
- The template input context is the current encoder input row under `.row`.
- On row-diff sink branches, `.row` remains the dense row-diff output shape; unchanged tracked
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

The JSON encoder then appends that item using the configured delivery format. Array format keeps
the rendered item bytes inside the outer array. NDJSON compacts the rendered JSON value and appends
LF so template whitespace cannot create extra record boundaries.

The runtime template engine is `upon`. The encoder profile exposes:

- `json(value)`: render a value as a JSON literal inside the template output
- `prop("key")`: read one process-wide static property

For example:

```text
{"vin":{{ prop("vin") | json }},"value":{{ json(.row.a) }} }
```

The template is compiled while the pipeline is built. `.row` is supplied for
each encoded item, while the property snapshot is fixed for that pipeline
build. A missing property is reported as an encoder error under the current
sink error policy.

## Execution Semantics

### Without Sink Batching

If sink batching is disabled:

- The encoder receives one input collection.
- It iterates all rows in that collection.
- For each row:
  - Render one transformed JSON item from the row template.
  - Append that item using the configured JSON delivery framing.
- The encoder emits one JSON payload.

Conceptually:

```text
collection
  -> for each row: template(row) => json item
  -> frame all items as one array or NDJSON delivery
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
5. The encoder writes all transformed items using the configured array or NDJSON framing.
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
  - Encode the transformed item into the current JSON delivery.

Physical shape:

```text
Project -> SinkEncoder(json + row template transform) -> Sink
```

Runtime behavior:

1. The streaming encoder starts an output buffer for the current flush unit.
2. For each appended row:
   - Build the row render context from the current SQL output row.
   - Render one transformed JSON item.
   - Append that item using the configured framing.
3. When batch count or duration is reached:
   - Finish the current JSON delivery. Array format writes `]`; NDJSON has no closing bytes.
   - Emit one payload bytes buffer.
   - Reset streaming state.

This is effectively:

```text
append(row)
  -> template(row) => transformed json item
  -> append item into current JSON delivery
```

Key point:

- The transform is row-based.
- The payload is still collection-based.
- That is why streaming append semantics are well-defined.

## Behavior With Output Layout

The template receives a dense row assembled from the same planner-derived
`OutputLayout` used by native JSON encoding. Direct columns are read from fixed
message indexes and computed columns from fixed affiliate indexes. A row-diff
output mask does not make the template context sparse.

Physical shape:

```text
Project -> SinkEncoder(json + row template transform) -> Sink
```

Per-row execution becomes:

1. Resolve the dense projected SQL output row through `OutputLayout`.
2. Render the current row into one transformed JSON item.
3. Append that item into the current JSON payload buffer.

Flush execution becomes:

1. Finish the configured JSON delivery framing.
2. Emit one payload.
3. Reset encoder state.

This path still avoids the extra standalone batch-then-encode buffering through
streaming append. Project materializes only computed expressions; the encoder
resolves the final dense template row by fixed references.

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
- Keep array/NDJSON delivery framing inside the JSON encoder
- Keep sink batching at the collection level

Under this design:

- No optimization: supported
- `PhysicalSinkEncoder`: supported
- Plan-fixed output layout: supported

This provides a practical path for the current sink-batch use case without introducing a separate
collection transform plan in the first iteration.
