# Pipeline API

## MQTT 5 Sink Properties

An MQTT sink may set `props.protocol_version` to `v5` and provide static User Properties as an
ordered array. MQTT 3.1.1 remains the default when the version is omitted. Each value may use a
process property template that is rendered once during pipeline apply.

```json
{
  "id": "mqtt_output",
  "type": "mqtt",
  "props": {
    "broker_url": "tcp://127.0.0.1:1883",
    "topic": "processed/telemetry",
    "protocol_version": "v5",
    "user_properties": [
      { "key": "source", "value": "{{ prop(\"site\") }}" },
      { "key": "tag", "value": "primary" },
      { "key": "tag", "value": "edge" }
    ]
  }
}
```

Each item must contain string `key` and `value` fields. Keys are literal. Values support the static
connector template profile, where `prop()` reads process-wide properties; `.row` and incoming MQTT
5 User Properties are not available. Order and duplicate keys are preserved. With `connector_key`,
omit connector-local `protocol_version`; the referenced shared MQTT client owns the version. User
Properties require that effective shared version to be `v5`.

## Schedule Options

Pipeline schedules are declared inside `options.schedule` when creating or upserting a pipeline.
The scheduler owns the lifecycle of a scheduled pipeline. Manual `POST /pipelines/:id/start` and
`POST /pipelines/:id/stop` calls return `409 Conflict` while a schedule is present, except that
manual stop is allowed when the current revision has a matching runtime failure marker.

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

## Runtime Failure Status

Pipeline list and get responses expose the current runtime status when it differs from the stored
desired state. A pipeline can report:

- `running`
- `stopped`
- `scheduled_running`
- `scheduled_stopped`
- `failed`

`failed` means a processor task returned an error, panicked, or exited unexpectedly while the
pipeline was expected to run. When this happens, the pipeline supervisor aborts the remaining
processor tasks and records a runtime failure marker for the current pipeline revision.

Responses may include these additional fields:

- `desired_status`: present when the stored desired state differs from `status`, such as a failed
  pipeline that is still desired to be `running` or `scheduled_running`
- `last_runtime_error`: present when a matching runtime failure marker exists for the current
  revision

`last_runtime_error` has this shape:

```json
{
  "processor_id": "filter_1",
  "processor_kind": "filter",
  "reason": "processor returned error: boom",
  "failed_at_ms": 1797249600000
}
```

Startup hydration restores a failed pipeline definition but does not auto-start a revision with a
matching runtime failure marker. Scheduled patrol also skips auto-start while the matching marker is
present. A manual start is treated as an explicit retry for unscheduled pipelines and clears the
marker only after a successful runtime start. Manual stop clears the marker and writes desired state
`stopped`; for scheduled failed pipelines, manual stop writes `scheduled_stopped` and returns the
pipeline to scheduler control so the next active patrol can retry. Delete removes the stored pipeline
and the marker.

`GET /pipelines/:id/stats` is only available for a running pipeline. For a failed pipeline, the
endpoint returns `400 Bad Request` with the failed processor and reason instead of returning stale
processor counters.
