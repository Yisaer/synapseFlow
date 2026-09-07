# Stream Configuration Constraints

## Background

Stream definitions are global metadata. A single stream record captures schema, connector-specific
properties, decoder selection, and optional source-side behaviors such as event time and sampling.

The same definition is persisted once and then installed into every declared flow instance. Because
the definition crosses manager, planner, and runtime boundaries, validation is intentionally split:

- manager-side validation rejects malformed or obviously incompatible definitions early
- flow/planner validation resolves runtime registries and source-specific execution assumptions later

This document centralizes those cross-cutting rules so stream-level test planning does not need to
rediscover them from `manager::stream`, the catalog, and planner/runtime code.

## Goals

- Define the stable stream-definition model shared by manager and flow.
- Document which constraints are validated at create time versus pipeline-build time.
- Explain how schema, decoder, shared-stream, event-time, and sampler options interact.
- Provide a test-oriented matrix for source-type-specific constraints.

## Non-Goals

- Endpoint-by-endpoint REST request reference.
- Decoder implementation internals.
- Connector-specific behavior that belongs in dedicated source/sink design docs.

## Stream Definition Model

A stream definition stores:

- stream identity and stream type
- schema declaration
- source-specific connector properties
- decoder configuration
- optional event-time declaration
- optional sampler configuration
- whether the stream should run as a shared ingest runtime

Manager persists the request first, then installs the same definition into every declared flow
instance. If any instance rejects the stream during create, manager rolls back both the already
created runtime copies and the stored record. This keeps stream metadata and runtime installation
aligned at creation time.

## Stream Type Matrix

Current built-in stream types are:

- `mqtt`
- `history`
- `memory`
- `mock`
- `nng_pubsub`
- `video`

The built-in required properties are:

- `mqtt`: `topic`; `broker_url` is also required when `connector_key` is absent; `qos` defaults
  when omitted; `client_id`, `connector_key`, and `protocol_version` are optional;
  `protocol_version` accepts `v3` (MQTT 3.1.1, the default) or `v5` and must be omitted when
  `connector_key` is present
- `history`: `datasource`, `topic`; optional start/end/batch/interval controls
- `memory`: `topic`
- `mock`: no connector props
- `nng_pubsub`: `url`; `topic` defaults to subscribe-all when omitted; `topic_delimiter`
  defaults to `:`
- `file`: `path`; optional `framing` configuration supports `append_batch` and delimiter-based
  framing
- `video`: `url`; optional RTSP/reconnect controls

The stream type decides both the connector property schema and which downstream combinations are
valid. Validation is not purely syntactic: some combinations are rejected because they would not
produce a coherent runtime model.

## Schema Type And Schema Registry

Schema declarations are parsed through a global schema registry owned by manager-side stream
creation.

The current builtin registration is:

- `json`

The registry is extensible, so `schema.type` is part of the design surface instead of being hard
coded into storage. Unknown schema types fail stream creation immediately.

The `json` schema format is structural:

- leaf scalar columns declare `name` and `data_type`
- `struct` columns must declare `fields`
- `list` columns must declare `element`

Schema parsing is strict because the resulting `Schema` becomes the contract used by decoder
projection, event-time lookup, planner explain output, and downstream type validation.

## Decoder Rules

Decoder configuration is stored per stream as `decoder.type` plus free-form `props`.

Manager-side rules:

- `decoder.type = none` is treated as a special case and bypasses decoder registry lookup
- any other decoder kind must already be registered in the default flow instance decoder registry

Semantic rules:

- `decoder.type = none` is only supported for `memory` streams
- shared streams cannot use `decoder.type = none`
- event time cannot be declared when decoder type is `none`

The reason is architectural:

- non-memory connectors produce raw bytes and require a decoder boundary before tuple/collection
  semantics exist
- shared streams own a reusable ingest runtime and therefore require a stable decoded stream shape
- event time is attached during decode, so there is no event-time extraction step when decoding is
  disabled

## Shared Stream Rules

`shared = true` changes the ownership model of the source runtime.

For shared streams:

- the ingest runtime is a first-class runtime resource instead of being embedded into one consumer
  pipeline
- shared-stream stats are instance-scoped, not globally aggregated
- all consumer pipelines on the same instance observe the same decoded stream output
- delivery into consumer pipelines is no-drop and backpressured
- runtime errors are non-terminal and follow normal processor-style error handling
- the stream definition remains installed even when the running ingest runtime is reclaimed
- the running ingest runtime exists only while at least one consumer references it

Current create-time constraints are:

- shared streams cannot use stream type `memory`
- shared streams cannot use stream type `nng_pubsub` in the first implementation
- shared streams cannot use decoder type `none`

This preserves a consistent shared ingest pipeline shape: source -> optional sampler -> decoder ->
backpressured fan-out into consumers.

## Event Time Rules

Stream definitions may declare:

- `eventtime.column`
- `eventtime.type`

Creation-time validation only checks structural compatibility with the decoder boundary. The full
runtime validation is deferred until event-time processing is actually enabled for a pipeline.

Pipeline-build/runtime expectations are:

- if pipeline event-time handling is disabled, the stored stream event-time declaration is inert
- if pipeline event-time handling is enabled, at least one referenced stream must declare event
  time
- the declared event-time column must exist in the effective stream schema
- the declared event-time type key must exist in the event-time parser registry

Current builtin event-time parser keys are:

- `unixtimestamp_s`
- `unixtimestamp_ms`

This split is intentional: stream metadata may be stored before the deployment decides which
event-time parsers are registered in the flow runtime.

## Sampler Rules

Sampler configuration is a source-level runtime behavior, not a per-pipeline rewrite.

Current properties:

- `interval`
- `strategy`

Built-in strategies are:

- `latest`
- `packer`

Planner/runtime behavior:

- the sampler sits between the physical data source and the decoder
- for non-shared streams, the sampler is instantiated inside each consuming pipeline runtime
- for shared streams, the sampler is instantiated inside the shared ingest runtime before decode
- consumers of the same shared stream on the same instance therefore observe the same sampled
  output from that shared ingest path

This makes sampler semantics part of the source contract rather than a query-local optimization.

## Stream-Type-Specific Constraints

Cross-cutting constraints worth preserving in future work:

- `memory` is the only built-in source type that may bypass decode with `decoder.type = none`
- `memory` cannot be promoted into a shared stream
- memory streams require a predeclared memory topic, and the topic kind must match decoder mode
  (`bytes` for normal decoders, `collection` for `decoder.type = none`)
- MQTT streams may optionally bind to a shared connector via `connector_key`, but the stream
  definition still owns the topic-level source identity
- NNG pub/sub streams own a URL, topic prefix, and topic delimiter. They follow the same decode
  boundary as other byte sources and do not support shared-stream promotion in the first
  implementation.
- NNG pub/sub transport is derived from the URL scheme. `tcp://`, `ipc://`, and `inproc://` are
  supported; the stream request must not add a second `transport` or `inproc` field. Embedded
  `inproc` deployment requirements are documented in
  [NNG In-Process Integration](../../integrations/ffi/nng_inproc.md).
- shared MQTT client delivery is no-drop and backpressured when used through `connector_key`
- shared MQTT runtime errors are non-terminal; explicit close/delete is the only terminal path
- shared MQTT metadata may remain installed while the running shared connection instance is
  reclaimed when no source/sink still references it
- history streams carry connector props in the stream definition even though their runtime behavior
  is different from live ingest connectors

## Validation Order

The effective validation order today is:

1. validate stream identity and request shape
2. parse schema via schema registry
3. decode source-specific props by stream type
4. resolve decoder configuration
5. enforce cross-field stream constraints (`shared`, decoder, event time, memory rules)
6. for memory streams, validate that the referenced memory topic already exists and that its kind
   matches decoder mode
7. persist the stream request into storage
8. install the stream into each flow instance
9. later, when pipelines are explained or built, validate event-time registry and pruned-schema
   compatibility

This order explains why some failures are create-time `400` errors while others only appear once a
pipeline references the stream.

## Testing Guidance

- Cover every built-in stream type with its minimal valid property set.
- Verify unknown `schema.type` and unknown non-`none` decoder kinds fail at stream creation.
- Verify `shared + memory`, `shared + decoder.none`, and `decoder.none + non-memory` are rejected.
- Verify `decoder.none + eventtime` is rejected at stream creation.
- Verify event-time registry validation when a pipeline enables event time and the stream declares
  an unknown `eventtime.type`.
- Verify missing `eventtime.column` fails once the pipeline is explained or built with event time
  enabled.
- Verify sampler plans appear before decode in planner explain output for both regular and shared
  ingest paths.
- Verify stream creation rollback when installation succeeds in some instances and fails in a later
  instance.

## Future Work

- A richer schema-type ecosystem will need explicit compatibility rules between manager-side schema
  parsing and decoder-side projection.
- If non-memory zero-decode sources are introduced later, the current `decoder.type = none`
  restriction should be revisited as a design change rather than relaxed ad hoc.
