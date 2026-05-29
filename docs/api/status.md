# GET /status

Returns a JSON snapshot of the current veloflux runtime state.

This endpoint is designed for health checks, dashboards, and operational
introspection. It complements the Prometheus-format `/metrics` endpoint by
providing a structured, machine-readable JSON response.

## Request

```http
GET /status HTTP/1.1
```

No query parameters or request body.

## Response

### 200 OK

```json
{
  "cpu_usage_percent": 12.5,
  "memory_usage_bytes": 104857600,
  "heap_in_use_bytes": 67108864,
  "heap_in_allocator_bytes": 134217728,
  "tokio_tasks_inflight": 42,
  "uptime_seconds": 3600,
  "active_pipeline_count": 5,
  "commit": "abc123def",
  "release_tag": "v0.1.0"
}
```

### Fields

| Field | Type | Description |
|---|---|---|
| `cpu_usage_percent` | `number` (`f64`) | Process CPU usage percentage, sampled via `sysinfo`. Updated every `metrics_poll_interval_secs` (default 5s). |
| `memory_usage_bytes` | `number` (`i64`) | Resident memory (RSS) in bytes, sampled via `sysinfo`. Updated every `metrics_poll_interval_secs`. |
| `heap_in_use_bytes` | `number` (`i64`) | Bytes actively allocated by the global allocator (jemalloc `stats.allocated`). Updated every `metrics_poll_interval_secs`. Reports `0` when the `metrics` feature is disabled or jemalloc is not the active allocator. |
| `heap_in_allocator_bytes` | `number` (`i64`) | Bytes reserved by the allocator from the operating system (jemalloc `stats.resident`). Updated every `metrics_poll_interval_secs`. Reports `0` when the `metrics` feature is disabled or jemalloc is not the active allocator. |
| `tokio_tasks_inflight` | `number` (`i64`) | Number of currently spawned Tokio tasks in the runtime. |
| `uptime_seconds` | `number` (`u64`) | Seconds elapsed since the manager server process started. |
| `active_pipeline_count` | `number` (`usize`) | Number of pipelines currently in the `running` state across all flow instances. |
| `commit` | `string` | Git commit SHA embedded at build time. Returns `"unknown"` when built outside a Git repository. |
| `release_tag` | `string` | Git tag from `git describe --tags --always` embedded at build time. Returns the closest tag (e.g. `v0.1.0`) when HEAD is at a tag, or a describe-style string (e.g. `v0.1.0-3-gabc123`) otherwise. Returns `"unknown"` when built outside a Git repository. |

### Notes

- All metric-backed fields (`cpu_usage_percent`, `memory_usage_bytes`, `heap_in_use_bytes`,
  `heap_in_allocator_bytes`) are *snapshots* updated asynchronously by the background
  metrics collector, not real-time samples. The collection interval is controlled by
  the `metrics_poll_interval_secs` server option (default 5s).
- When the `metrics` feature is disabled at compile time, all metric-backed fields
  report `0` or `0.0`.
- `active_pipeline_count` counts pipelines whose runtime status is `running`.
  Stopped pipelines or pipelines that exist only in storage are not counted.
