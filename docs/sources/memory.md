# Memory Source Connector

The **Memory source** subscribes to an in-process pub/sub topic and ingests data into a stream. It
is intended for process-local integration and testing scenarios (no network I/O).

## Overview

- Backing transport: in-process `tokio::sync::broadcast` channels (per topic).
- Topics are **strongly typed**: a topic is either `bytes` or `collection` (mixing is rejected).
- Topics must be **predeclared** via the manager API before a memory stream can use them.
- Message loss is possible when subscribers lag (drops are logged).

## Topic Declaration (Manager)

Before creating a memory stream, declare the topic:

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

## Stream Configuration (Manager)

Create a stream of type `memory`:

| Property | Type | Required | Default | Description |
|----------|------|----------|---------|-------------|
| `props.topic` | string | Yes | - | Topic name to subscribe to (must be declared). |
| `decoder.type` | string | Yes | `json` | Decoder kind. `none` means the source ingests `Collection` directly. |

Additional constraints:

- Shared streams do **not** support `type=memory`.
- `decoder.type == "none"` is only supported for `type=memory`.
- `decoder.type == "none"` cannot be used together with `eventtime` (eventtime needs decoded rows).

## Data Semantics

The memory topic kind and stream decoder determine what the source expects:

- **Topic kind = `bytes`**:
  - The source ingests `bytes` from the topic.
  - The physical plan inserts the configured decoder (for example `json`) before the datasource.
- **Topic kind = `collection`**:
  - The source ingests `Collection` from the topic.
  - The stream must be created with `decoder.type = "none"`.
  - The physical plan does **not** insert a decoder node.

## Drop / Lag Behavior

The implementation uses a bounded broadcast buffer (`capacity`):

- If a subscriber cannot keep up and falls behind the buffer, older messages are dropped.
- The memory source logs an error on lag events and continues.

Publishing when there are no subscribers is treated as success (delivered count is `0`).

## Example (Bytes Topic)

```json
POST /streams
{
  "name": "mem_bytes_stream",
  "type": "memory",
  "schema": {
    "type": "json",
    "props": {
      "columns": [
        { "name": "value", "data_type": "int64" }
      ]
    }
  },
  "decoder": { "type": "json", "props": {} },
  "props": { "topic": "demo_bytes" }
}
```

## Example (Collection Topic)

```json
POST /streams
{
  "name": "mem_collection_stream",
  "type": "memory",
  "schema": {
    "type": "json",
    "props": {
      "columns": [
        { "name": "value", "data_type": "int64" }
      ]
    }
  },
  "decoder": { "type": "none", "props": {} },
  "props": { "topic": "demo_collection" }
}
```

