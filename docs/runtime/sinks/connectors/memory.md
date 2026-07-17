# Memory Sink

The **Memory sink** publishes pipeline outputs into an in-process pub/sub topic. It is intended
for process-local integration and testing scenarios (no network I/O).

## Overview

- Backing transport: in-process `tokio::sync::broadcast` channels (per topic).
- Topics are scoped to the running **FlowInstance** (they are not shared across instances).
- Topics are **strongly typed**: a topic is either `bytes` or `collection` (mixing is rejected).
- Topics must be **predeclared** via the manager API before a pipeline can publish to them.
- Publishing when there are no subscribers is treated as success (delivered count is `0`).

For the shared resource model, declaration semantics, and startup/import-export behavior, see
[`memory_topics.md`](../../instances/memory_topics.md).

## Topic Declaration (Manager)

Declare the topic before creating a pipeline that uses a memory sink:

- `POST /memory/topics`

```json
{
  "topic": "demo_bytes",
  "kind": "bytes",
  "capacity": 1024
}
```

Notes:

- `capacity` defaults to `1024` when omitted.
- Declaring an existing topic with a different `kind`/`capacity` is rejected.
- The persisted topic resource is documented in
  [`memory_topics.md`](../../instances/memory_topics.md).

## Sink Configuration (Manager)

Add a sink of type `memory` to a pipeline:

| Property | Type | Required | Default | Description |
|----------|------|----------|---------|-------------|
| `type` | string | Yes | - | Must be `memory`. |
| `props.topic` | string | Yes | - | Topic name to publish to (must be declared). |
| `encoder.type` | string | No | `json` | Encoder kind. `none` means the sink publishes `Collection` directly. |

## Data Semantics

The sink uses `encoder.type` to decide which topic kind it publishes:

- `encoder.type != "none"`:
  - The pipeline inserts an encoder before the sink.
  - The sink publishes **bytes** to a `bytes` topic.
- `encoder.type == "none"`:
  - The physical plan does not insert an encoder.
  - The sink publishes **Collection** to a `collection` topic.

The manager validates that the topic exists and that its declared kind matches the encoder mode.

## Collection Materialization (encoder.type = "none")

When publishing to a `collection` topic, the physical plan inserts a dedicated node
`PhysicalMemoryCollectionMaterialize` right before the sink. This node reshapes each input row
(`Tuple`) into a stable layout, so downstream pipelines can rely on a consistent column name + order.

Behavior:

- The output row is rewritten to **1 Message + 0 affiliate**:
  - Message `source` is left empty (`""`). Downstream memory sources normalize the source name to
    the stream binding (SQL datasource name).
  - Message column **names and order** follow the pipeline's planned output schema (the `SELECT`
    field order after wildcard expansion and aliasing).
- The sink does **not** evaluate expressions. It copies values through the
  planner-derived `OutputLayout`: direct columns use fixed message indexes and
  derived columns use fixed affiliate indexes.
- A planned `OutputValueRef::Null` is copied as `NULL`. An out-of-range fixed
  reference is a layout contract violation and returns a processing error.
- Duplicate output column names are rejected at planning time for `collection` topics.

Implementation note:

- `PhysicalMemoryCollectionMaterialize` receives the same immutable
  `OutputLayout` as encoded sinks and performs no first-row discovery or name
  lookup fallback.

## Drop / Lag Behavior (Subscribers)

The implementation uses a bounded broadcast buffer (`capacity`). If subscribers cannot keep up,
they will observe lag and messages may be dropped on the subscriber side. The sink itself does not
block on subscribers.

## Example (Publish Bytes)

```json
POST /pipelines
{
  "id": "pipe_mem_bytes",
  "sql": "SELECT * FROM mem_bytes_stream",
  "sinks": [
    {
      "id": "mem_sink",
      "type": "memory",
      "props": { "topic": "demo_bytes" },
      "encoder": { "type": "json", "props": {} }
    }
  ]
}
```

## Example (Publish Collections)

```json
POST /pipelines
{
  "id": "pipe_mem_collection",
  "sql": "SELECT * FROM mem_collection_stream",
  "sinks": [
    {
      "id": "mem_sink",
      "type": "memory",
      "props": { "topic": "demo_collection" },
      "encoder": { "type": "none", "props": {} }
    }
  ]
}
```
