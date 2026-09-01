# Pipeline REST API (Manager)

This document describes the **Manager** REST API for managing pipelines.

Base URL depends on your deployment (examples use `http://127.0.0.1:8080`).

> **Resource IDs.** The pipeline `id`, each `sinks[].id`, and `flow_instance_id`
> must match `` `[A-Za-z][A-Za-z0-9_]{0,127}` ``: start with an ASCII letter,
> then ASCII letters, digits, or underscores, up to 128 bytes. IDs are
> case-sensitive and are never trimmed — leading/trailing whitespace is rejected
> rather than silently removed. Invalid IDs return `400 Bad Request`.

## Endpoints

### Create Pipeline

`POST /pipelines`

Creates a pipeline, persists it, builds the execution plan, and registers it in runtime.

Request body: `CreatePipelineRequest`

```json
{
  "id": "demo_pipeline",
  "revision": 1721797200000,
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

- `201 Created` with `{ id, revision, status }`.
- `409 Conflict` if the pipeline already exists.
- `409 Conflict` if the pipeline is busy processing another command.

### List Pipelines

`GET /pipelines`

Returns all persisted pipelines with a best-effort runtime status label.

Response:

- `200 OK` with `ListPipelineItem[]`.

### Get Pipeline

`GET /pipelines/:id`

Returns persisted pipeline spec, current status, and desired run state when it differs from the
current status.

Response:

- `200 OK` with `GetPipelineResponse`.
- `404 Not Found` if not present in storage.

### Upsert Pipeline

`PUT /pipelines/:id`

Replaces pipeline spec by id when the request revision is greater:

- If present, manager deletes the existing pipeline in runtime and storage.
- Manager persists the new spec and registers the new pipeline.
- If the old desired state was `running`, manager attempts to start the new pipeline.
- A lower revision returns `409 Conflict`.
- An equal revision is an idempotent success only when the normalized
  definition is unchanged; otherwise it returns `409 Conflict`.

Request body: `UpsertPipelineRequest` (same shape as create but without `id`).

Response:

- `200 OK` with `{ id, revision, status }`.
- `400 Bad Request` for invalid specs or planning failures.

### Start Pipeline

`POST /pipelines/:id/start`

Persists desired state as `running` and starts runtime execution.
Starting a failed pipeline is an explicit retry; a successful start clears the runtime failure
marker.

Response:

- `200 OK` on success.
- `409 Conflict` if the pipeline has `options.schedule`; scheduled pipeline lifecycle is
  managed by the scheduler.
- `404 Not Found` if pipeline is not present.
- `409 Conflict` if the pipeline is busy processing another command.

### Stop Pipeline

`POST /pipelines/:id/stop?mode=quick|graceful&timeout_ms=5000`

Persists desired state as `stopped` and stops runtime execution. For a scheduled failed pipeline,
manual stop is allowed as a recovery action: it clears the runtime failure marker, writes
`scheduled_stopped`, and lets the scheduler retry on a later patrol.

Query parameters:

- `mode` (optional, default `quick`): `quick` or `graceful`
- `timeout_ms` (optional, default `5000`)

Response:

- `200 OK` on success.
- `409 Conflict` if the pipeline has `options.schedule` and does not have a matching runtime
  failure marker; scheduled pipeline lifecycle is managed by the scheduler.
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
- `400 Bad Request` if the pipeline is failed; the error includes the failed processor and reason
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
- `revision: number` (required, positive JSON-safe integer)
- `sql: string` (required, non-empty)
- `sinks: CreatePipelineSinkRequest[]` (required, at least one)
- `options: PipelineOptionsRequest` (optional)

### `UpsertPipelineRequest`

- `revision: number` (required, positive JSON-safe integer)
- `sql: string` (required, non-empty)
- `sinks: CreatePipelineSinkRequest[]` (required, at least one)
- `options: PipelineOptionsRequest` (optional)

### `PipelineOptionsRequest`

- `eventtime: { enabled: boolean, late_tolerance_ms: number }`
  - `late_tolerance_ms` is milliseconds (default `0`)
- `schedule: PipelineScheduleRequest` (optional)

### `PipelineScheduleRequest`

`schedule` declares automatic pipeline start/stop windows. When present, the scheduler owns the
pipeline lifecycle; manual start/stop endpoints return `409 Conflict`, except that manual stop is
allowed to recover a scheduled failed pipeline with a matching runtime failure marker.

```json
{
  "cron": "*/10 * * * *",
  "duration_secs": 300,
  "datetime_ranges": [
    {
      "begin_timestamp_ms": 1767225600000,
      "end_timestamp_ms": 1767312000000
    }
  ]
}
```

- `cron: string` (optional): Linux-compatible 5-field cron expression evaluated in UTC,
  `minute hour day-of-month month day-of-week`, or one of the supported recurring nicknames
  `@yearly`, `@annually`, `@monthly`, `@weekly`, `@daily`, `@midnight`, and `@hourly`. Day of week
  uses `0-7`, with both `0` and `7` representing Sunday. If day-of-month and day-of-week are both
  restricted, either field may match. `@reboot` is not supported.
- `duration_secs: number` (optional): required when `cron` is present and forbidden without
  `cron`. It defines the run duration after each cron fire and must be greater than `0`.
- `datetime_ranges: PipelineDatetimeRangeRequest[]` (optional): absolute UTC timestamp ranges in
  milliseconds. When cron is absent, at least one range is required and the ranges define the
  complete run windows.

At least one scheduling mode must be present: `cron` with `duration_secs`, or one or more
`datetime_ranges`. The effective run window is:

```text
cron only:              (cron_fire, cron_fire + duration_secs)
datetime ranges only:   any (begin_timestamp_ms, end_timestamp_ms)
cron and ranges:        (cron_fire, cron_fire + duration_secs) intersect any datetime_range
```

If `cron` matches but `datetime_ranges` is non-empty and the current timestamp is outside every
range, the pipeline does not start. If a cron window crosses the end of a matched datetime range,
the pipeline stops at the range end.

On create, upsert, or process restart, a scheduled pipeline first enters `scheduled_stopped`. The
patrol scheduler evaluates the current window before starting it.

### `PipelineDatetimeRangeRequest`

Datetime ranges use `(begin_timestamp_ms, end_timestamp_ms)` semantics. Multiple ranges are
combined with OR semantics.

- `begin_timestamp_ms: number` (required): non-negative Unix timestamp in milliseconds.
- `end_timestamp_ms: number` (required): non-negative Unix timestamp in milliseconds and greater
  than `begin_timestamp_ms`.
- At most 128 normalized ranges are accepted. Ranges are sorted and overlapping ranges are merged
  before persistence. Adjacent ranges remain separate so their shared open boundary stays inactive.

### `CreatePipelineSinkRequest`

- Optional `id: string` (defaults to `{pipeline_id}_sink_{index}`)
- `type: string` (required)
  - Supported: `mqtt`, `nop`, `kuksa`, `kura`, `memory`, `file`, `video`, `nng_pubsub`, `http`
- `props: object` (optional, defaults to `{}`)
- `common_sink_props: object` (optional)
  - Optional `batch_count: number`
  - Optional `batch_duration: number` (milliseconds)
- `encoder: { type: string, props: object }` (optional; default is `{ "type": "json", "props": {} }`)
  - Built-in byte encoders include `json` and `csv`.
  - JSON accepts `props.format`: `array` (default) emits one JSON array per delivery, while
    `ndjson` emits one compact LF-terminated JSON text per output row.
  - CSV accepts `props.delimiter` (one ASCII byte, default `,`) and `props.header` (boolean,
    default `true`), and does not support `output.mode=delta` or encoder transforms.
  - For `type == "kuksa"` or `type == "kura"`, encoder is ignored and forced to `none`.
  - For `type == "http"`, encoder is required (e.g. `json`, `csv`, `protobuf`).

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
- Optional `headers: object` (default: `{}`) — extra **non-secret** headers (e.g. `{ "X-Env": "prod" }`).
  Sensitive auth headers (`Authorization`, `Proxy-Authorization`, `Cookie`) are rejected here — use
  `auth` / `secret_headers` instead so the value never lands in scannable config.
- Optional `auth: object` — structured authentication. Bearer: `{ "type": "bearer", "token": "store:NAME" }`;
  Basic: `{ "type": "basic", "username": "alice", "password": "store:NAME" }`. The secret is a
  `store:NAME` reference into the encrypted secret store (see *Secret references* below).
- Optional `secret_headers: object` — custom auth headers whose values are secrets: header name →
  `store:NAME` reference (e.g. `{ "X-Api-Key": "store:my-api-key" }`).
- Optional `content_type: string` — explicit `Content-Type` header. When omitted, inferred from the
  encoder kind and JSON format (`application/json` for JSON arrays, `application/x-ndjson` for
  NDJSON, `text/csv; charset=utf-8` for CSV, `application/octet-stream` for protobuf). An explicit
  value takes precedence. Multipart mode uses its generated `multipart/form-data` Content-Type
  instead of encoder inference.
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
    "auth": { "type": "bearer", "token": "store:submit-api-token" },
    "retry_max_attempts": 3,
    "retry_backoff_ms": 500,
    "retry_max_backoff_ms": 10000
  },
  "encoder": { "type": "json", "props": {} }
}
```

### Secret references

Sensitive values (HTTP `auth`/`secret_headers`, MQTT `password`, sink encryption `key`) are not
written inline in pipeline/stream config. Instead they reference an entry in the encrypted secret
store as `store:NAME`. Manage entries with the local CLI (values are read from a prompt, stdin, or
`--from-file` — never from the command line):

```bash
veloflux secrets set submit-api-token --from-file ./token.txt   # or: printf %s "$T" | veloflux secrets set submit-api-token
veloflux secrets list
veloflux secrets get submit-api-token   # debug
veloflux secrets rm  submit-api-token
```

**Where the store lives.** The CLI and server both use `<data-dir>/secrets.enc`, where `<data-dir>`
comes from the `--data-dir` flag (default `./tmp`). The CLI does not read the config file, so pass the
**same `--data-dir`** you start the server with, e.g. `veloflux secrets set k --data-dir /var/lib/veloflux`.

The store is encrypted with a root key from the `VELOFLUX_SECRETS_KEY` environment variable (base64
32-byte key); without it a built-in key is used, which keeps secrets out of static scanners but is not
confidential against someone holding the binary. Set the **same** env var for the CLI and the server.

**Inline values and policy.** Any secret field value that does not start with `store:` is treated as
an inline literal. The `VELOFLUX_SECRETS_POLICY` env var controls handling: `warn` (default) accepts
inline literals and logs a warning; `strict` rejects them at config apply. Inline literals are written
verbatim into pipeline config (scannable) — prefer `store:NAME`. Credentials in the wrong place (URL
userinfo, or `Authorization`/`Cookie` in plain `headers`) are always rejected regardless of policy.

## Response Shapes

### `CreatePipelineResponse`

- `id: string`
- `revision: number`
- `status: string` (`running`, `stopped`, `scheduled_running`, `scheduled_stopped`, or `failed`)

### `ListPipelineItem`

- `id: string`
- `revision: number`
- `status: string` (`running`, `stopped`, `scheduled_running`, `scheduled_stopped`, or `failed`)
- `desired_status: string` (optional; present when stored desired state differs from `status`)
- `last_runtime_error: PipelineRuntimeFailure` (optional; present when a matching runtime failure
  marker exists)

### `GetPipelineResponse`

- `id: string`
- `revision: number`
- `status: string` (`running`, `stopped`, `scheduled_running`, `scheduled_stopped`, or `failed`)
- `desired_status: string` (optional; present when stored desired state differs from `status`)
- `last_runtime_error: PipelineRuntimeFailure` (optional; present when a matching runtime failure
  marker exists)
- `spec: CreatePipelineRequest`
- `schedule_status: ScheduleStatus` (optional; present when `spec.options.schedule` is present)

### `PipelineRuntimeFailure`

- `processor_id: string`
- `processor_kind: string`
- `reason: string`
- `failed_at_ms: number`

### `ScheduleStatus`

- `cron: string` (optional; omitted for datetime-range-only schedules)
- `duration_secs: number` (optional; omitted for datetime-range-only schedules)
- `datetime_ranges: PipelineDatetimeRangeRequest[]` (omitted when empty)
- `in_window: boolean`
- `previous_fire_at: string` (optional RFC 3339 UTC timestamp)
- `next_fire_at: string` (optional RFC 3339 UTC timestamp)
- `auto_stop_at: string` (optional RFC 3339 UTC timestamp). For a datetime-range-only schedule this
  is the active range end. When a datetime range clips a cron window, this is the clipped range end.

### `ProcessorStatsEntry`

- `processor_id: string`
- `stats: object`
  - Common fields: `records_in`, `records_out`, `error_count`, `last_error`
  - Custom processor metrics are flattened into this object as additional numeric fields (e.g. `rows_buffered`)
