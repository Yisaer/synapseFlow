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
    "plan_cache": { "enabled": true },
    "eventtime": { "enabled": false, "lateTolerance": 0 }
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
- Manager persists the new spec, rebuilds plan cache, and registers the new pipeline.
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

Query parameters:

- `timeout_ms` (optional, default `5000`)

Response:

- `200 OK` with `ProcessorStatsEntry[]`
- `404 Not Found` if pipeline is not present
- `409 Conflict` if the pipeline is busy processing another command
- `504 Gateway Timeout` on stats collection timeout

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

- `plan_cache: { enabled: boolean }` (default `false`)
- `eventtime: { enabled: boolean, lateTolerance: number }`
  - `lateTolerance` is milliseconds (default `0`)

### `CreatePipelineSinkRequest`

- Optional `id: string` (defaults to `{pipeline_id}_sink_{index}`)
- `type: string` (required)
  - Supported: `mqtt`, `nop`, `kuksa`
- `props: object` (optional, defaults to `{}`)
- `commonSinkProps: object` (optional)
  - Optional `batchCount: number`
  - Optional `batchDuration: number` (milliseconds)
- `encoder: { type: string, props: object }` (optional; default is `{ "type": "json", "props": {} }`)
  - For `type == "kuksa"`, encoder is ignored and forced to `none`.

### Sink `props` by `type`

`type == "mqtt"`:

- `broker_url: string` (required)
- `topic: string` (required)
- Optional `qos: number` (default: `0`)
- Optional `retain: boolean` (default: `false`)
- Optional `client_id: string`
- Optional `connector_key: string`

`type == "nop"`:

- Optional `log: boolean` (default: `false`)

`type == "kuksa"`:

- `addr: string` (required)
- `vssPath: string` (required)

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
- `stats: { records_in: number, records_out: number, error: string | null }`
