# Pipeline Stats Design

## Background

Pipeline stats are the manager-visible snapshot of per-processor runtime counters for one user
pipeline.

The purpose is operational visibility into the currently running processor graph, not a general
metrics export replacement and not a shared-stream observability API.

## Goals

- Define the scope and meaning of pipeline processor stats.
- Document the stable metric fields and custom-metric extension model.
- Clarify timeout and error behavior for stats collection.
- Explain the relationship between pipeline stats and shared-stream stats.

## Non-Goals

- Prometheus exposition details.
- Endpoint-by-endpoint API tutorial.
- Aggregated observability across multiple pipelines or flow instances.

## Scope Of Pipeline Stats

Pipeline stats are scoped to one pipeline id in one flow instance runtime.

They describe the processors that belong to the user pipeline runtime only. They do not include:

- shared-stream ingest processors
- synthetic aggregation across multiple pipelines
- control-plane state that exists only in storage

The pipeline must currently be running for stats collection to succeed.

## Processor Stats Model

Each response entry is keyed by `processor_id` and contains a snapshot object.

The always-present base fields are:

- `error_count`
- `last_error`

Collection boundaries additionally expose both the collection count and its row count:

- `collections_in` and `records_in` for a collection input boundary
- `collections_out` and `records_out` for a collection output boundary

These fields strictly count rows. Raw payloads, encoded messages, delivery protocol events, and
connector attempts are not records. A field is omitted when the processor does not declare rows as
an applicable unit in that direction. A declared field starts at `0`, including before the first
collection arrives and when an observed collection contains zero rows. Processing one collection
increments the applicable `collections_*` counter by one and its `records_*` counter by
`collection.num_rows()`.

Processors register applicable data-unit counters as flattened custom fields:

- `messages_in`, `messages_out`, `messages_aborted`, `messages_dropped`
- processor-specific collection outcomes such as `collections_forwarded` and
  `collections_suppressed`
- `bytes_in`, `bytes_out`, `bytes_delivered`

Only applicable fields are registered. For custom message, collection, and byte counters, a missing
field means the processor does not support that unit and a present zero means it is applicable but
has not observed an event. Row fields follow the same declaration rule.

The physical plan determines each processor instance's input and output domains before runtime
startup. Metric fields are registered from that fixed contract and never appear in response to the
first observed data variant. Receiving data outside the planned domain is a pipeline wiring error.

Conversion boundaries deliberately expose different units by direction. A decoder declares
message/byte input and collection/row output. An encoder declares collection/row input and
message/byte output; it does not expose `records_out` because rows are not its output domain.
Every sink declares message output. A collection-native sink declares collection/row input and
message output; retaining a collection as the payload representation does not turn the sink output
back into a collection metric boundary.

## Metric Categories

Pipeline stats support two metric categories:

- stable base counters/errors listed above
- processor-specific custom metrics flattened into the same JSON object

Custom metrics are registered with:

- a stable metric id
- a flattened response field name
- a metric kind (`gauge` or `counter`)

The flattened field name must not collide with reserved base names. Unit-specific custom counters
must not be substituted for row counters simply to make adjacent stages appear symmetric.

## Snapshot Semantics

Stats are a point-in-time read of the current in-memory processor counters.

Important implications:

- stats are not persisted across restarts
- stats reset when a runtime pipeline is rebuilt
- the response reflects the currently installed runtime graph, not historical executions

Manager returns the same logical snapshot shape.
collection path differs.

## Timeout And Snapshot Semantics

The current collection query accepts `timeout_ms` and defaults to 5000 milliseconds.

Current behavior:

- in-process collection forwards the timeout to flow runtime collection
- timeout is surfaced as HTTP `504 Gateway Timeout`

Today, the in-process flow runtime only collects stats from already-running pipelines, so timeouts
protect the request path and future-proof the API contract rather than
wrapping a long historical aggregation job.

## Relationship To Shared Stream Stats

Pipeline stats and shared-stream stats intentionally model different ownership boundaries.

Pipeline stats cover:

- processors instantiated for one user pipeline

Shared-stream stats cover:

- processors owned by a shared ingest runtime

Manager therefore keeps them separate. Shared-stream processor visibility belongs to
`docs/api/streams/shared_stream_stats.md` and related runtime shared-stream designs, not to the
pipeline stats response.

## Processor Filtering

Manager filters out some internal processors before returning pipeline stats:

- `control_source`
- `PhysicalResultCollect_*`

The goal is to keep the payload focused on user-meaningful runtime work instead of framework
bookends. Tests should therefore assert stats on semantic processors such as decoder, project,
watermark, sampler, sink, and similar stages.

## Error Reporting

Current error surface:

- `404` when the pipeline id is unknown
- `400` when the pipeline exists but stats cannot be collected, for example because the pipeline is
  not running
- `504` on timeout

Processor-local failures do not turn the whole stats request into an error automatically. They are
reported through each entry's `error_count` and `last_error`.

## Testing Guidance

- Verify basic counter progression for a simple running pipeline.
- Verify `error_count` and `last_error` update when a processor records runtime errors.
- Verify custom flattened metrics appear alongside the base fields without reserved-name
  collisions.
- Verify filtered processors (`control_source`, `PhysicalResultCollect_*`) are absent from the
  returned list.
- Verify collecting stats for a stopped pipeline returns an error rather than stale counters.
- Verify timeout handling maps to `504` consistently.
  pipelines.
- Verify shared-stream ingest metrics are not mixed into normal pipeline stats.

## Future Work

- If pipeline stats later need historical or persisted semantics, that should be introduced as a
  separate observability layer rather than changing the meaning of the current snapshot response.
