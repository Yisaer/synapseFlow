# Memory Sink

The **Memory sink** publishes pipeline outputs into an in-process pub/sub topic. It is intended
for process-local integration and testing scenarios (no network I/O).

## Overview

- Backing transport: in-process `tokio::sync::broadcast` channels (per topic).
- Topics are **strongly typed**: a topic is either `bytes` or `collection` (mixing is rejected).
- Topics must be **predeclared** via the manager API before a pipeline can publish to them.
- Publishing when there are no subscribers is treated as success (delivered count is `0`).

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

