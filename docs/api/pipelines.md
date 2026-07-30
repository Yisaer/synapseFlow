# Pipeline API

## Schedule Options

Pipeline schedules are declared inside `options.schedule` when creating or upserting a pipeline.
The scheduler owns the lifecycle of a scheduled pipeline. Manual `POST /pipelines/:id/start` and
`POST /pipelines/:id/stop` calls return `409 Conflict` while a schedule is present.

```json
{
  "options": {
    "schedule": {
      "cron": "*/10 * * * *",
      "duration_secs": 300,
      "datetime_ranges": [
        {
          "begin_timestamp_ms": 1767225600000,
          "end_timestamp_ms": 1767312000000
        }
      ]
    }
  }
}
```

| Field | Type | Description |
| --- | --- | --- |
| `cron` | string | Required 5-field cron expression: `minute hour day-of-month month day-of-week`. |
| `duration_secs` | integer | Required run duration after each cron fire. Must be greater than `0`. |
| `datetime_ranges` | array | Optional UTC timestamp ranges in milliseconds. Missing or empty means no datetime restriction. |

Each datetime range uses `[begin_timestamp_ms, end_timestamp_ms)` semantics:

- `begin_timestamp_ms` must be non-negative.
- `end_timestamp_ms` must be non-negative.
- `begin_timestamp_ms` must be less than `end_timestamp_ms`.
- At most 128 normalized ranges are accepted.

The effective run window is the intersection of the cron window and the datetime range set:

```text
[cron_fire, cron_fire + duration_secs) intersect any datetime_range
```

If the cron window crosses the end of the matched datetime range, the pipeline is stopped at the
range end. `GET /pipelines/:id` returns `schedule_status.auto_stop_at` as the effective current
window end, not just the raw cron duration end.

Scheduled pipeline desired state is stored separately from manual lifecycle state:

- `ScheduledRunning`: the effective schedule window currently expects the pipeline to run.
- `ScheduledStopped`: the effective schedule window currently expects the pipeline to stop.

REST responses expose these states as `scheduled_running` and `scheduled_stopped`.
