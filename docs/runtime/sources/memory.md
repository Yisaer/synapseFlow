# Memory Source Connector

The **Memory source** subscribes to an in-process pub/sub topic and ingests data into a stream. It
is intended for process-local integration and testing scenarios (no network I/O).

## Overview

- Backing transport: in-process `tokio::sync::broadcast` channels (per topic).
- Topics are scoped to the running **FlowInstance** (they are not shared across instances).
- Topics are **strongly typed**: a topic is either `bytes` or `collection` (mixing is rejected).
- Topics must be **predeclared** via the manager API before a memory stream can use them.
- Message loss is possible when subscribers lag (drops are logged).

For the shared resource model, declaration semantics, and startup/import-export behavior, see
[`memory_topics.md`](../instances/memory_topics.md).

## Topic Declaration (Manager)

Before creating a memory stream, declare the topic:

- `POST /memory/topics`

```json
{
  "topic": "demo_bytes",
  "revision": 1,
  "kind": "bytes",
  "capacity": 1024
}
```

Notes:

- `capacity` defaults to `1024` when omitted.
- Declaring an existing topic with a different `kind`/`capacity` is rejected.
- The persisted topic resource is documented in
  [`memory_topics.md`](../instances/memory_topics.md).

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
- The referenced memory topic must already be declared before stream creation.
- Stream creation rejects memory topic kind / decoder mismatches before the stream is installed.

## Data Semantics

The memory topic kind and stream decoder determine what the source expects:

- **Topic kind = `bytes`**:
  - The source ingests `bytes` from the topic.
  - The physical plan inserts the configured decoder (for example `json`) before the datasource.
- **Topic kind = `collection`**:
  - The source ingests `Collection` from the topic.
  - The stream must be created with `decoder.type = "none"`; create-time validation rejects a
    non-`none` decoder against a `collection` topic.
  - The physical plan does **not** insert a decoder node.
  - The logical optimizer preserves the stream **full schema** (no schema pruning) to keep
    `ColumnRef::ByIndex(source_name, idx)` semantics stable.
  - The physical plan inserts a **layout normalization** node right after the datasource:
    `PhysicalCollectionLayoutNormalize`.

### Collection Layout Normalization (decoder.type = "none")

For collection topics, the memory pub/sub layer does not enforce a schema contract: an upstream
publisher may send tuples with different message `source` values or different column ordering.
Downstream physical execution relies on `ColumnRef::ByIndex`, which assumes:

- the tuple contains a message whose `source` matches the stream binding name (e.g. `stream`)
- the message values are ordered according to the stream full schema

To guarantee correctness, `PhysicalCollectionLayoutNormalize` rewrites each incoming tuple into a
stable shape:

- Output tuple shape: **`1 message + 0 affiliate`**
- Output message `source`: the stream binding name (the datasource name used in SQL)
- Output keys and ordering: **exactly the stream full schema column order**
- Missing columns: filled with **`NULL`** (a warning is logged)

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
  "revision": 1,
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
  "revision": 1,
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
