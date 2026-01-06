# Stream REST API (Manager)

This document describes the **Manager** REST API for managing streams.

Base URL depends on your deployment (examples use `http://127.0.0.1:8080`).

## Endpoints

### Create Stream

`POST /streams`

Creates a stream definition, persists it, and registers it in the runtime catalog.

Request body: `CreateStreamRequest`

```json
{
  "name": "source_stream",
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
  }
}
```

Notes:

- `schema.type` selects the schema declaration format. Manager currently ships with `json`.
- `schema.props` is schema-format specific (for `json`, it must contain `columns`).
- `decoder.type` must be registered in the runtime decoder registry (builtin: `json`).
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

## Request Shapes

### `CreateStreamRequest`

- `name: string` (required, non-empty)
- `type: string` (required)
  - Supported: `mqtt`, `history`
- `schema: { type: string, props: object }` (required)
- `props: object` (optional, defaults to `{}`)
- `shared: boolean` (optional, defaults to `false`)
- `decoder: { type: string, props: object }` (optional, defaults to `{ "type": "json", "props": {} }`)
- Optional `eventtime: { column: string, type: string }`

### Stream `props` by `type`

`type == "mqtt"`:

- `broker_url: string` (required)
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

### Schema JSON format (`schema.type == "json"`)

`schema.props` must be an object containing:

- `columns: Column[]`

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
- `spec_version: number` (currently `1`)
- `spec: StreamDefinitionSpec`

### `StreamDefinitionSpec`

- `type: string` (stream type label, e.g. `mqtt`)
- `schema: { columns: Column[] }`
- `props: object`
- `shared: boolean`
- `decoder: { type: string, props: object }`
- Optional `eventtime: { column: string, type: string }`
