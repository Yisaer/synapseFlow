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
The scheduler owns the lifecycle while the schedule is enabled. Manual `stop` disables schedule
patrol and enters `Stopped`; manual `start` re-enables schedule control and enters
`ScheduledStopped`, leaving the next runtime start to patrol.

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
| `cron` | string | Optional Linux-compatible 5-field cron expression or supported recurring nickname. Expressions are evaluated in UTC. See [Pipeline Schedule Cron Syntax](../syntax/pipeline_schedule_cron.md). |
| `duration_secs` | integer | Required when `cron` is present and forbidden without `cron`. Must be greater than `0`. |
| `datetime_ranges` | array | Optional UTC timestamp ranges in milliseconds. When cron is absent, at least one range is required and the ranges define the complete run windows. |

At least one scheduling mode must be present: `cron` with `duration_secs`, or one or more
`datetime_ranges`. Each datetime range uses `(begin_timestamp_ms, end_timestamp_ms)` semantics:

- `begin_timestamp_ms` must be non-negative.
- `end_timestamp_ms` must be non-negative.
- `begin_timestamp_ms` must be less than `end_timestamp_ms`.
- At most 128 normalized ranges are accepted. Multiple ranges are combined with OR semantics.

Cron windows also use open boundaries. The effective run window depends on the configured modes:

```text
cron only:              (cron_fire, cron_fire + duration_secs)
datetime ranges only:   any (begin_timestamp_ms, end_timestamp_ms)
cron and ranges:        (cron_fire, cron_fire + duration_secs) intersect any datetime_range
```

Multiple cron windows are combined with OR semantics. When consecutive windows overlap, the
pipeline remains scheduled to run across the overlap instead of being stopped and restarted at the
next cron fire.

If the cron window crosses the end of the matched datetime range, the pipeline is stopped at the
range end. `GET /pipelines/:id` returns `schedule_status.auto_stop_at` as the effective current
window end, not just the raw cron duration end. Patrol-triggered stops use graceful shutdown by
default so already accepted data can be drained and finalized before the runtime exits.

On create, a scheduled pipeline first enters `ScheduledStopped`. Upsert and process restart retain
manual `Stopped`; otherwise they enter `ScheduledStopped`. The patrol scheduler evaluates the
current window before starting it; a persisted `ScheduledRunning` value is not used to bypass the
current schedule.

Scheduled pipeline desired state is stored separately from manual lifecycle state:

- `ScheduledRunning`: the effective schedule window currently expects the pipeline to run.
- `ScheduledStopped`: the effective schedule window currently expects the pipeline to stop.
- `Stopped`: schedule control is manually disabled; patrol skips the pipeline.

REST responses expose these states as `scheduled_running` and `scheduled_stopped`.

During a graceful patrol stop, the runtime may temporarily report `stopping`. This is an
in-memory transition state and is not persisted as a desired state. Patrol treats `stopping` as
an operation in progress and does not start the runtime again until the stop has completed. A
manual stop persists `Stopped` immediately to disable schedule control, while the runtime may
remain `stopping` until graceful shutdown finishes. A manual start received during this transition
is rejected as busy; retrying it after the stop completes only re-enables schedule control, and the
next patrol decides whether the runtime should start.

## Runtime Failure Status

Pipeline list and get responses expose the current runtime status when it differs from the stored
desired state. A pipeline can report:

- `running`
- `stopping`
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
marker only after a successful runtime start. For scheduled pipelines, manual start only re-enables
schedule control; patrol performs the later runtime start. Manual stop clears the marker, writes
desired state `stopped`, and disables schedule patrol. Delete removes the stored pipeline and the
marker.

`GET /pipelines/:id/stats` is only available for a running pipeline. For a failed pipeline, the
endpoint returns `400 Bad Request` with the failed processor and reason instead of returning stale
processor counters.
