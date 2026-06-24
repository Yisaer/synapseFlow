# Pipeline REST API (Manager)

This document describes the **Manager** REST API for managing pipelines.

Base URL depends on your deployment (examples use `http://127.0.0.1:8080`).

## Endpoints

### Create Pipeline

`POST /pipelines`

Creates a pipeline, persists it, builds the execution plan, and registers it in runtime.

Request body: `CreatePipelineRequest`

```json
{
  "id": "demo-pipeline",
  "sql": "SELECT user_id, score FROM source_stream WHERE score > 0",
  "sinks": [
    {
      "type": "mqtt",
      "props": { "broker_url": "tcp://127.0.0.1:1883", "topic": "/yisa/data2" },
      "encoder": { "type": "json", "props": {} }
    }
  ],
  "options": {
    "eventtime": { "enabled": false, "late_tolerance_ms": 0 }
  }
}
```

Response:

- `201 Created` with `{ id, status }`.
- `409 Conflict` if the pipeline already exists.
- `409 Conflict` if the pipeline is busy processing another command.

### List Pipelines

`GET /pipelines`

Returns all persisted pipelines with a best-effort status label.

Response:

- `200 OK` with `ListPipelineItem[]`.

### Get Pipeline

`GET /pipelines/:id`

Returns persisted pipeline spec and desired run state.

Response:

- `200 OK` with `GetPipelineResponse`.
- `404 Not Found` if not present in storage.

Note: `status` is derived from the **stored desired state**, not from the runtime snapshot.

### Upsert Pipeline

`PUT /pipelines/:id`

Replaces pipeline spec by id:

- If present, manager deletes the existing pipeline in runtime and storage.
- Manager persists the new spec and registers the new pipeline.
- If the old desired state was `running`, manager attempts to start the new pipeline.

Request body: `UpsertPipelineRequest` (same shape as create but without `id`).

Response:

- `200 OK` with `{ id, status }`.
- `400 Bad Request` for invalid specs or planning failures.

### Start Pipeline

`POST /pipelines/:id/start`

Persists desired state as `running` and starts runtime execution.

Response:

- `200 OK` on success.
- `404 Not Found` if pipeline is not present.
- `409 Conflict` if the pipeline is busy processing another command.

### Stop Pipeline

`POST /pipelines/:id/stop?mode=quick|graceful&timeout_ms=5000`

Persists desired state as `stopped` and stops runtime execution.

Query parameters:

- `mode` (optional, default `quick`): `quick` or `graceful`
- `timeout_ms` (optional, default `5000`)

Response:

- `200 OK` on success.
- `404 Not Found` if pipeline is not present.
- `409 Conflict` if the pipeline is busy processing another command.

### Explain Pipeline

`GET /pipelines/:id/explain`

Returns a human-readable explain output.

Response:

- `200 OK` with `Content-Type: text/plain; charset=utf-8`
- `404 Not Found` if pipeline is not present.

### Collect Pipeline Stats

`GET /pipelines/:id/stats?timeout_ms=5000`

Collects processor-level stats snapshots from the running pipeline.
Internal bookkeeping processors (e.g. `control_source`, `result collect`) are excluded.

Query parameters:

- `timeout_ms` (optional, default `5000`; currently ignored)

Response:

- `200 OK` with `ProcessorStatsEntry[]`
- `404 Not Found` if pipeline is not present
- `409 Conflict` if the pipeline is busy processing another command

### Delete Pipeline

`DELETE /pipelines/:id`

Deletes a pipeline from runtime and storage.

Response:

- `200 OK` on success
- `404 Not Found` if pipeline does not exist
- `409 Conflict` if the pipeline is busy processing another command

## Request Shapes

### `CreatePipelineRequest`

- `id: string` (required, non-empty)
- `sql: string` (required, non-empty)
- `sinks: CreatePipelineSinkRequest[]` (required, at least one)
- `options: PipelineOptionsRequest` (optional)

### `UpsertPipelineRequest`

- `sql: string` (required, non-empty)
- `sinks: CreatePipelineSinkRequest[]` (required, at least one)
- `options: PipelineOptionsRequest` (optional)

### `PipelineOptionsRequest`

- `eventtime: { enabled: boolean, late_tolerance_ms: number }`
  - `late_tolerance_ms` is milliseconds (default `0`)

### `CreatePipelineSinkRequest`

- Optional `id: string` (defaults to `{pipeline_id}_sink_{index}`)
- `type: string` (required)
  - Supported: `mqtt`, `nop`, `kuksa`, `kura`, `memory`, `file`, `video`, `nng_pubsub`, `http`
- `props: object` (optional, defaults to `{}`)
- `common_sink_props: object` (optional)
  - Optional `batch_count: number`
  - Optional `batch_duration: number` (milliseconds)
- `encoder: { type: string, props: object }` (optional; default is `{ "type": "json", "props": {} }`)
  - For `type == "kuksa"` or `type == "kura"`, encoder is ignored and forced to `none`.
  - For `type == "http"`, encoder is required (e.g. `json`, `protobuf`).

### Sink `props` by `type`

`type == "mqtt"`:

- `broker_url: string` (required when `connector_key` is absent)
- `topic: string` (required)
- Optional `qos: number` (default: `0`)
- Optional `retain: boolean` (default: `false`)
- Optional `client_id: string`
- Optional `connector_key: string`

`type == "nop"`:

- Optional `log: boolean` (default: `false`)

`type == "kuksa"`:

- `addr: string` (required)
- `vss_path: string` (required)

`type == "http"`:

Delivers encoded payloads to a remote HTTP endpoint. Each delivery unit is sent as a
single HTTP request. The sink supports configurable retry with exponential backoff
and random jitter for transient failures (network errors, 5xx, 429).

- `url: string` (required) — target URL (e.g. `https://example.com/api/metrics`)
- Optional `method: string` (default: `"POST"`) — HTTP method: `GET`, `POST`, `PUT`, `PATCH`, `DELETE`
- Optional `timeout_secs: number` (default: `30`) — per-request timeout in seconds
- Optional `headers: object` (default: `{}`) — extra headers (e.g. `{ "Authorization": "Bearer token" }`)
- Optional `content_type: string` — explicit `Content-Type` header. When omitted, inferred from the
  encoder kind (`application/json` for JSON, `application/octet-stream` for protobuf)
- Optional `max_body_size: number` (default: `67108864`, i.e. 64 MiB) — maximum single-delivery body
  size in bytes. Exceeding this limit aborts the delivery.
- Optional `retry_max_attempts: number` (default: none, i.e. no retry) — maximum delivery attempts
  including the first one. Example: `3` means up to 2 retries.
- Optional `retry_backoff_ms: number` (default: `1000`) — initial backoff in milliseconds between
  retries, doubles after each failed attempt.
- Optional `retry_max_backoff_ms: number` (default: `30000`) — upper bound on backoff duration.

**Example — basic JSON POST:**

```json
{
  "type": "http",
  "props": {
    "url": "https://example.com/api/metrics",
    "content_type": "application/json"
  },
  "encoder": { "type": "json", "props": {} }
}
```

**Example — with retry and custom headers:**

```json
{
  "type": "http",
  "props": {
    "url": "https://example.com/api/submit",
    "method": "PUT",
    "timeout_secs": 10,
    "headers": { "Authorization": "Bearer xxx" },
    "retry_max_attempts": 3,
    "retry_backoff_ms": 500,
    "retry_max_backoff_ms": 10000
  },
  "encoder": { "type": "json", "props": {} }
}
```

## Response Shapes

### `CreatePipelineResponse`

- `id: string`
- `status: string` (`running` or `stopped`)

### `ListPipelineItem`

- `id: string`
- `status: string` (`running` or `stopped`)

### `GetPipelineResponse`

- `id: string`
- `status: string` (`running` or `stopped`)
- `spec: CreatePipelineRequest`

### `ProcessorStatsEntry`

- `processor_id: string`
- `stats: object`
  - Common fields: `records_in`, `records_out`, `error_count`, `last_error`
  - Custom processor metrics are flattened into this object as additional numeric fields (e.g. `rows_buffered`)
