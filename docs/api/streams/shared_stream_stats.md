# Shared Stream Processor Stats

## Background

A shared stream in veloFlux owns a process-local ingest runtime that is independent from any single
user pipeline.

When a pipeline consumes a shared stream, the user-facing physical plan contains a
`PhysicalSharedStream` node, but the actual ingest work is executed by a separate internal shared
pipeline. Its explain tree looks like:

```text
shared/<stream>/PhysicalDataSource_0
  -> shared/<stream>/PhysicalDecoder_1
  -> shared/<stream>/PhysicalResultCollect_2
```

These processors are the true runtime boundary for:

- source ingestion
- payload decoding
- backpressured fan-out into downstream shared-stream consumers

Today, `/pipelines/:id/stats` only reports the stats of the user pipeline itself. That is useful
for observing one pipeline's consumption path, but it does not expose the stats of the shared
stream's internal ingest processors.

This becomes a real observability gap once one shared stream is consumed by many pipelines:

- pipeline stats are duplicated per consumer pipeline
- pipeline stats do not describe the shared ingest runtime as a first-class object
- operators cannot independently inspect shared processors such as
  `shared/<stream>/PhysicalDataSource_*` or `shared/<stream>/PhysicalDecoder_*`

## Problem Statement

We need a dedicated way to inspect the processor stats of a shared stream's internal ingest
pipeline.

The missing observability target is not the per-pipeline `PhysicalSharedStream_*` processor.
Instead, it is the internal shared ingest processors:

- `shared/<stream>/PhysicalDataSource_*`
- `shared/<stream>/PhysicalDecoder_*`
- `shared/<stream>/PhysicalResultCollect_*`

## Why Stream-Level Stats Only Make Sense For Shared Streams

Stream-level stats are only well-defined for shared streams.

For a shared stream:

- the runtime owns a dedicated internal `ProcessorPipeline`
- the ingest pipeline persists independently of any one consumer pipeline
- the stream itself is a first-class runtime resource
- the stream definition is a first-class runtime resource even when the internal ingest pipeline is
  currently stopped
- the internal ingest runtime may be reclaimed when the stream has no active consumers

For a non-shared stream:

- there is no standalone stream runtime
- source/decoder processors are instantiated inside each consuming pipeline
- any stats naturally belong to the pipeline, not to the stream definition

Because of this difference, a generic `GET /streams/:name/stats` endpoint would have ambiguous
semantics for non-shared streams. It would force one of these bad choices:

- aggregate stats across all pipelines that happen to reference the stream
- arbitrarily pick one pipeline instance
- return a partially defined object with different meaning depending on stream kind

The API should avoid that ambiguity.

## Chosen API Shape

Add a dedicated endpoint:

`GET /streams/:name/shared/stats`

This path encodes the scope explicitly:

- it is attached to a stream resource
- it only applies to shared-stream runtime
- it returns the stats of the shared stream's internal ingest pipeline

Because shared-stream runtimes are instance-scoped, the caller also needs to identify which flow
instance is being inspected:

- single-instance deployment: `flow_instance_id` may be omitted and defaults to `default`
- multi-instance deployment: caller must pass `flow_instance_id`

## API Semantics

### Supported Target

The endpoint only supports streams created with `shared=true`.

### Response Meaning

The response reports the stats of the shared stream's internal ingest processors. It does **not**
report:

- the stats of downstream user pipelines
- the stats of `PhysicalSharedStream_*` processors inside user pipelines
- any synthetic aggregation across user pipelines

In multi-instance deployments, the response is scoped to the selected `flow_instance_id`. It is
not an implicit aggregate and it must not silently fall back to `default`.

### Status Codes

- `200 OK`: the stream exists and is a shared stream
- `404 Not Found`: the stream does not exist
- `400 Bad Request`: the stream exists but is not a shared stream
- `400 Bad Request`: `flow_instance_id` is required when multiple flow instances are declared
- `400 Bad Request`: `flow_instance_id` is empty or undeclared
- `400 Bad Request`: the `:name` path segment or `flow_instance_id` is not a valid resource id

The `:name` path segment and the `flow_instance_id` query parameter must match
`` `[A-Za-z][A-Za-z0-9_]{0,127}` `` (start with an ASCII letter, then ASCII
letters, digits, or underscores; case-sensitive; not trimmed).

## Response Shape

Suggested response:

```json
{
  "stream": "shared_demo",
  "flow_instance_id": "default",
  "status": "running",
  "status_message": null,
  "processors": [
    {
      "processor_id": "shared/shared_demo/PhysicalDataSource_0",
      "stats": {
        "records_in": 100,
        "records_out": 100,
        "error_count": 0,
        "last_error": null
      }
    },
    {
      "processor_id": "shared/shared_demo/PhysicalDecoder_1",
      "stats": {
        "records_in": 100,
        "records_out": 100,
        "error_count": 1,
        "last_error": "invalid json payload"
      }
    },
    {
      "processor_id": "shared/shared_demo/PhysicalResultCollect_2",
      "stats": {
        "records_in": 100,
        "records_out": 100,
        "error_count": 0,
        "last_error": null
      }
    }
  ]
}
```

Fields:

- `stream`: shared stream name
- `flow_instance_id`: the flow instance whose shared-stream runtime produced this stats snapshot
- `status`: shared stream runtime status
- `status_message`: optional message when `status == failed`
- `processors`: stats entries from the internal shared ingest pipeline

`processors` reuses the same stats payload shape already used by pipeline stats:

- `processor_id`
- `stats.records_in`
- `stats.records_out`
- `stats.error_count`
- `stats.last_error`
- any existing flattened custom processor metrics

## Runtime Status Semantics

The endpoint should report shared-stream runtime status using the existing shared runtime states:

- `starting`
- `running`
- `stopped`
- `failed`

Recommended behavior:

- `running`: return the current shared ingest processor stats
- `starting`: return `processors=[]`
- `stopped`: return `processors=[]`
- `failed`: return `processors=[]` and set `status_message`

This keeps the endpoint useful even when no internal pipeline is currently active.

The shared stream definition may remain installed while its internal ingest runtime transitions
between `running` and `stopped`. A `stopped` status is therefore valid both after an explicit
stop/delete path and after the last active consumer releases the shared stream runtime.

## Design Constraints

### Do Not Reuse Pipeline Stats Semantics

`/pipelines/:id/stats` should remain scoped to one user pipeline.

It should not be extended to expose the shared stream's internal processors because that would mix
two different runtime ownership boundaries:

- user pipeline runtime
- shared stream ingest runtime

### Do Not Invent Stats For Non-Shared Streams

The new endpoint should not try to synthesize stream-level stats for non-shared streams.

If a caller needs non-shared source visibility, the correct place remains:

- `/pipelines/:id/stats`

### Keep Explain And Stats Aligned

The processor ids returned by the new endpoint should match the ids shown by the shared ingest
explain tree. This makes explain output and runtime stats directly comparable during debugging.

## Implementation Plan

### 1. Flow Layer: Expose Shared Ingest Processor Stats

The shared stream runtime already keeps an internal `ProcessorPipeline` alive while the shared
stream is running.

The plan is to expose a read-only query from shared-stream runtime state:

- locate `SharedStreamInner` by stream name
- read current runtime status
- if an internal `ProcessorPipeline` is present, collect `processor_stats()`
- convert those handles into `ProcessorStatsEntry`

Suggested flow-layer return type:

```text
SharedStreamProcessorStats
- stream: String
- status: SharedStreamStatus
- processors: Vec<ProcessorStatsEntry>
```

### 2. FlowInstance Layer: Provide Manager-Friendly Access

Add a `FlowInstance` method that:

- validates the stream exists
- validates the stream is shared
- delegates to the shared stream registry

This keeps manager handlers from reaching into shared-stream internals directly.

### 3. Manager Layer: Add REST Handler

Add:

- route: `GET /streams/:name/shared/stats`
- handler in `src/manager/src/stream.rs`

Handler flow:

1. load the stream definition
2. return `404` if missing
3. return `400` if `shared=false`
4. resolve `flow_instance_id`
5. query shared-stream runtime stats from the selected flow instance
6. map runtime status into API fields
7. return the response

### 4. Documentation

Document the endpoint under the stream API docs and clearly state:

- it only applies to shared streams
- it returns shared ingest processor stats
- it is not a replacement for pipeline stats

## Testing Plan

Recommended minimum regression coverage:

1. Shared stream is running
   - request `GET /streams/:name/shared/stats`
   - expect `200`
   - expect `status=running`
   - expect non-empty `processors`
   - expect processor ids to include shared ingest processors

2. Stream exists but is not shared
   - request `GET /streams/:name/shared/stats`
   - expect `400`

3. Shared stream exists but is currently stopped
   - request `GET /streams/:name/shared/stats`
   - expect `200`
   - expect `status=stopped`
   - expect `processors=[]`

## Non-Goals

- adding a generic stream stats API for all stream kinds
- aggregating stats across all pipelines that reference a non-shared stream
- merging shared ingest stats into `/pipelines/:id/stats`
- introducing a separate explain endpoint in the same change
