# Stream REST API (Manager)

This document describes the **Manager** REST API for managing streams.

For named schema management and the proto schema parser, see `user_docs/api/schema.md`.

Base URL depends on your deployment (examples use `http://127.0.0.1:8080`).

> **Resource IDs.** The stream `name`, a referenced `schema.ref`, a memory
> stream's `props.topic`, and a shared-client `props.connector_key` must match
> `` `[A-Za-z][A-Za-z0-9_]{0,127}` ``: start with an ASCII letter, then ASCII
> letters, digits, or underscores, up to 128 bytes. The stream name doubles as a
> SQL source identifier, so hyphens and dots are not allowed. IDs are
> case-sensitive and never trimmed; invalid IDs return `400 Bad Request`. This
> does **not** apply to external protocol identifiers such as the MQTT
> `props.topic` or `client_id`.

## Endpoints

### Create Stream

`POST /streams`

Creates a stream definition, persists it, and registers it in the runtime catalog.

Request body: `CreateStreamRequest`

```json
{
  "name": "source_stream",
  "revision": 1721797200000,
  "type": "mqtt",
  "schema": {
    "type": "json",
    "props": {
      "columns": [
        {"name": "user_id", "data_type": "int64"},
        {"name": "score", "data_type": "float64"}
      ]
    }
  },
  "props": {
    "broker_url": "tcp://127.0.0.1:1883",
    "topic": "/yisa/data",
    "qos": 0
  },
  "shared": false,
  "decoder": {
    "type": "json",
    "props": {}
  },
  "sampler": {
    "interval": "10s",
    "strategy": { "type": "latest" }
  }
}
```

Notes:

- `schema.type` selects the schema declaration format. Built-in: `json`, `proto`.
- `schema.props` is schema-format specific.
- Alternatively, use `schema.ref` to reference a pre-defined named schema (see `user_docs/api/schema.md`). When `schema.ref` is set, `schema.type` and `schema.props` are ignored.
- `decoder.type` must be registered in the runtime decoder registry (builtin: `json`; the SDV distribution additionally provides `gbf`, `busmirror`, and others).
- `eventtime.type` must be registered in the runtime event-time registry (builtin: `unixtimestamp_s`, `unixtimestamp_ms`).

Response:

- `201 Created` with `StreamInfo`.
- `409 Conflict` if the stream already exists.

Example:

```bash
curl -s -XPOST http://127.0.0.1:8080/streams \
  -H "Content-Type: application/json" \
  -d @stream.json | jq .
```

### List Streams

`GET /streams`

Returns a lightweight list of persisted streams with schema summaries.

Response:

- `200 OK` with `StreamInfo[]`.

Note: current implementation does not populate `shared_stream` in this endpoint.

Example:

```bash
curl -s http://127.0.0.1:8080/streams | jq .
```

### Describe Stream

`GET /streams/describe/:name`

Returns a single stream’s persisted spec and schema.

Response:

- `200 OK` with `DescribeStreamResponse`.
- `404 Not Found` if the stream does not exist.

Example:

```bash
curl -s http://127.0.0.1:8080/streams/describe/source_stream | jq .
```

### Delete Stream

`DELETE /streams/:name`

Deletes a stream from runtime and storage.

Response:

- `200 OK` with a plain text message.
- `404 Not Found` if the stream does not exist.
- `409 Conflict` if any pipeline still references the stream.

Example:

```bash
curl -s -XDELETE http://127.0.0.1:8080/streams/source_stream
```

### Shared Stream Stats

`GET /streams/:name/shared/stats`

Returns processor-level stats for the internal ingest pipeline owned by one shared stream.

This endpoint only applies to streams created with `shared=true`.

It reports stats for the shared ingest processors, for example:

- `shared/<stream>/PhysicalDataSource_*`
- `shared/<stream>/PhysicalDecoder_*`
- `shared/<stream>/PhysicalResultCollect_*`

It does **not** report stats for downstream user pipelines. For pipeline-local stats, continue to
use `GET /pipelines/:id/stats`.

Query parameters:

- Optional `flow_instance_id=<id>` selects which flow instance's shared-stream runtime to inspect.
- In single-instance deployments, omitting `flow_instance_id` defaults to `default`.
- In multi-instance deployments, `flow_instance_id` is required because shared-stream runtimes are
  instance-scoped.

Response:

- `200 OK` with `SharedStreamStatsResponse`
- `404 Not Found` if the stream does not exist
- `400 Bad Request` if the stream exists but is not a shared stream
- `400 Bad Request` if multiple flow instances are declared and `flow_instance_id` is omitted
- `400 Bad Request` if `flow_instance_id` is empty or references an undeclared flow instance

Example:

```bash
curl -s \
  "http://127.0.0.1:8080/streams/source_stream/shared/stats?flow_instance_id=default" | jq .
```

## Request Shapes

### `CreateStreamRequest`

- `name: string` (required, non-empty)
- `revision: number` (required, positive JSON-safe integer)
- `type: string` (required)
  - Supported: `mqtt`, `history`
- `schema: { type: string, props: object }` (required)
  - Alternatively: `schema: { ref: string }` to reference a named schema.
  - When `ref` is set, `type` and `props` are ignored.
- `props: object` (optional, defaults to `{}`)
- `shared: boolean` (optional, defaults to `false`)
- `decoder: { type: string, props: object }` (optional, defaults to `{ "type": "json", "props": {} }`)
- Optional `eventtime: { column: string, type: string }`
- Optional `sampler: { interval: string, strategy: object }` (stream-level downsampling, see below)

### Stream `props` by `type`

`type == "mqtt"`:

- `broker_url: string` (required when `connector_key` is absent)
- `topic: string` (required)
- Optional `qos: number` (default: `0`)
- Optional `client_id: string`
- Optional `connector_key: string`

`type == "history"`:

- `datasource: string` (required)
- `topic: string` (required)
- Optional `start: int64` (timestamp integer; compared against the history Parquet `ts` column as-is)
- Optional `end: int64` (timestamp integer; compared against the history Parquet `ts` column as-is)
- Optional `batch_size: number`
- Optional `send_interval_ms: number`

### Sampler Configuration (`sampler`)

The `sampler` property enables stream-level downsampling. All pipelines consuming from this stream will receive downsampled data.

> **Note**: The sampler operates on raw bytes *before* decoding, enabling efficient rate limiting at the byte level.

- `interval: string` (required) – Duration between emissions (e.g., `"1s"`, `"100ms"`, `"5m"`)
- `strategy: object` (required) – Sampling strategy:
  - `{ "type": "latest" }` – Emits the most recent value received during each interval
  - `{ "type": "packer", "merger": { "type": "<merger_type>", "props": {...} } }` – Accumulates and merges payloads using a registered Merger

**Latest Strategy Example:**
```json
"sampler": {
  "interval": "10s",
  "strategy": { "type": "latest" }
}
```

**Packer Strategy Example:**
```json
"sampler": {
  "interval": "1s",
  "strategy": {
    "type": "packer",
    "props": {
      "merger": {
        "type": "can_merger",
        "props": { "schema": "/etc/can.dbc" }
      }
    }
  }
}
```

> **Note**: The `packer` strategy requires a Merger registered in the runtime. `can_merger` is a placeholder example; check your specific VeloFlux distribution for available mergers. The SDV distribution provides `gbf` and `busmirror` mergers, each requiring a matching decoder type.

### Schema JSON format (`schema.type == "json"`)

`schema.props` must be an object containing:

- `columns: Column[]`

### Schema Proto format (`schema.type == "proto"`)

Derive schema from a `.proto` file. See `docs/api/schema_registry.md` for the full
proto type mapping and `user_docs/api/schema.md` for the REST API.

File-backed protobuf schemas cannot be defined inline on a stream. Install the
schema ZIP with `POST /schemas`, then use `schema.ref`. The named schema props are:

- `proto_path: string` (path to the schema ZIP package)
- `message_type: string` (fully qualified message name)

The ZIP root contains one `.proto` entry and may contain its same-stem companion
directory for imported files.

### `Column`

- `name: string`
- `data_type: string`
- Optional:
  - `fields: Column[]` (only when `data_type == "struct"`)
  - `element: Column` (only when `data_type == "list"`)

Supported type strings:

- `null`, `bool`
- `int8`, `int16`, `int32`, `int64`
- `uint8`, `uint16`, `uint32`, `uint64`
- `float32`, `float64`
- `string`
- `struct`, `list`

## Response Shapes

### `StreamInfo`

- `name: string`
- `revision: number`
- `shared: boolean`
- `schema: { columns: Column[] }`
- Optional `shared_stream: SharedStreamItem`

### `SharedStreamItem`

- `id: string`
- `status: string` (`starting`, `running`, `stopped`, `failed`)
- Optional `status_message: string` (present when `status == "failed"`)
- `connector_id: string`
- `subscribers: number`
- `created_at_secs: number` (Unix seconds)

### `DescribeStreamResponse`

- `stream: string`
- `revision: number`
- `spec: StreamDefinitionSpec`

`PUT /streams/:name` applies only a greater revision. A lower revision returns
`409 Conflict`; an equal revision succeeds only when the normalized definition
is unchanged, otherwise it returns `409 Conflict`.

### `SharedStreamStatsResponse`

- `stream: string`
- `status: string` (`starting`, `running`, `stopped`, `failed`)
- Optional `status_message: string` (present when `status == "failed"`)
- `processors: ProcessorStatsEntry[]`

When the shared stream runtime is not currently running, the response returns an empty
`processors` array and the runtime `status`.

### `ProcessorStatsEntry`

- `processor_id: string`
- `stats: object`
  - Common fields: `records_in`, `records_out`, `error_count`, `last_error`
  - Custom processor metrics are flattened into this object as additional numeric fields

### `StreamDefinitionSpec`

- `type: string` (stream type label, e.g. `mqtt`)
- `schema: { columns: Column[] }`
- `props: object`
- `shared: boolean`
- `decoder: { type: string, props: object }`
- Optional `eventtime: { column: string, type: string }`
- Optional `sampler: { interval: string, strategy: object }`
