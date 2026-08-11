# MQTT Source Connector Design

## Background

MQTT streams are byte-oriented ingress connectors. They receive MQTT publish payloads, forward
those bytes into the source pipeline, and rely on later runtime stages for sampling, decoding, and
optional event-time extraction.

The source can run in two materially different modes:

- standalone connection owned by the source connector
- shared-client-backed connection selected by `connector_key`

This document focuses on that runtime behavior and on the interactions with shared streams,
decoding, event time, and sampling.

## Goals

- Define the runtime behavior of MQTT-backed streams.
- Clarify standalone versus shared-client-backed ownership.
- Document where decode, event time, and sampler semantics attach to the ingest path.
- Provide testing dimensions for source/runtime interactions.

## Non-Goals

- MQTT sink behavior.
- Endpoint reference manuals for creating streams.
- Decoder implementation details beyond the source/decoder boundary.

## Configuration Model

MQTT stream definitions currently carry:

- `broker_url`
- `topic`
- `qos`
- optional `client_id`
- optional `connector_key`
- optional `protocol_version` (`v3` or `v5`)
- stream-level decoder config
- optional stream-level event-time declaration
- optional sampler config
- optional `shared=true`

Manager requires `topic` in the stream request shape. `broker_url` is required only when
`connector_key` is absent. The meaning of stream-local fields changes once a shared client is
bound:

- without `connector_key`, the stream owns its own live connection settings
- with `connector_key`, the shared client resource becomes the live connection/subscription owner

`protocol_version` defaults to `v3`, which means MQTT 3.1.1. A standalone source may explicitly
select `v5`. When `connector_key` is present, the stream must omit `protocol_version`; the shared
client resource owns the connection protocol.

## Connection Modes

### Standalone source

Standalone mode is used when `connector_key` is absent.

Behavior:

- build MQTT options from stream-local `broker_url`
- derive a default client id from a generated UUID if the stream did not provide one
- subscribe directly to the configured `topic` using the configured `qos`
- own reconnect/backoff logic locally inside the source connector
- use MQTT 3.1.1 by default or MQTT 5.0 when `protocol_version` is `v5`

### Shared-client-backed source

Shared mode is used when `connector_key` is present.

Behavior:

- acquire the shared client by key
- subscribe to the shared client's backpressured event stream instead of opening another MQTT
  connection
- consume the shared client's payload/error/close events
- use the protocol selected by the shared client resource

In this mode, the stream-local MQTT props are no longer the live subscription owner. The shared
client config drives broker URL, subscription topic, client id, QoS, and packet-size settings.

## Decode Path

The MQTT source connector itself is byte-only. It forwards raw MQTT payload bytes downstream as
connector events.

The current implementation does not expose incoming MQTT 5 User Properties to rows or processors.
MQTT 5 sources receive the PUBLISH payload exactly as MQTT 3.1.1 sources do; source metadata
propagation is deferred.

The later decode boundary is responsible for:

- converting bytes into tuples/collections
- surfacing decode errors in decoder processor stats
- attaching event-time extraction when enabled

This is important for testing: invalid payloads are not source-connector failures by themselves.
They become decoder/runtime failures after the bytes leave the source connector.

## Shared Stream Behavior

When an MQTT stream is declared with `shared=true`, flow builds a shared ingest runtime through the
shared-stream registry. The shared-stream connector factory still creates an `MqttSourceConnector`,
so the same standalone-versus-shared-client binding rules apply.

Practical implications:

- one shared ingest runtime exists per flow instance
- all consumers on that instance observe the output of that shared ingest runtime
- shared-stream stats and lifecycle are separate from normal pipeline stats

Shared-stream delivery is part of the source contract:

- fan-out into shared-stream consumers is backpressured
- shared-stream delivery must not drop messages because one consumer is temporarily slower than
  another
- when the final consumer detaches, the shared-stream runtime may be reclaimed while the stream
  definition remains installed

## Backpressure And Delivery

Shared MQTT source delivery is intentionally different from a lossy pub/sub buffer.

Required semantics:

- shared-client-backed sources must not silently lose payloads due to lagging local subscribers
- slow subscribers propagate backpressure upstream through the shared-client fan-out path
- runtime delivery errors are surfaced as ordinary connector/runtime errors
- runtime delivery errors are non-terminal and must not be converted into implicit
  `EndOfStream`

This aligns shared MQTT source behavior with processor runtime behavior: errors are recorded and
handled, while terminal shutdown comes only from explicit lifecycle actions.

## Event Time Interaction

Event time is not parsed inside the MQTT connector.

Instead:

- the source emits bytes
- the decoder converts bytes into rows
- event-time extraction is attached to the decoder stage when pipeline event-time mode is enabled

This leads to two important constraints:

- MQTT sources still require a decoder before event-time extraction can happen
- stream-level event-time metadata may exist even when a particular pipeline does not enable
  event-time runtime behavior

See [pipeline_eventtime.md](/Users/yisa/Downloads/Github/veloFlux/docs/runtime/time/pipeline_eventtime.md) for the downstream
watermark and late-data model.

## Sampler Interaction

Sampler configuration is also outside the connector itself.

Planner/runtime place the sampler:

- after the physical data source
- before the decoder

For non-shared MQTT streams, that sampler exists inside each consuming pipeline runtime.

For shared MQTT streams, the sampler exists inside the shared ingest runtime, so all consumers on
the same instance observe the same sampled output from that shared ingest path.

## Error Handling And Recovery

Standalone source recovery behavior:

- `ConnAck` resets reconnect backoff and resubscribes
- `Disconnect` triggers reconnect cleanup
- connection errors are forwarded downstream and retried with exponential backoff from 100 ms up to
  5 s
- shutdown sends end-of-stream and disconnects the MQTT client

Shared-client-backed source behavior:

- payloads and connection errors are relayed from the shared client
- acquisition failure for the shared key is surfaced immediately as a connector error
- connection/runtime errors remain non-terminal unless the shared client is explicitly closed
- explicit close/delete of the shared client is the only path that relays end-of-stream to the
  source connector
- when no source/sink still references the shared client, the running shared-client connection
  instance may be reclaimed and recreated on the next acquisition

The source also keeps per-connector ingress/egress counters for payload throughput.

## Shared MQTT Client Interaction

`connector_key` does more than connection reuse. It also changes where source semantics are owned:

- source stream metadata still names the logical source
- shared client metadata owns the live network subscription contract
- pipeline context building must include the referenced shared client config before a
  rebuild the source

Because of this, missing shared-client metadata is a pipeline/context error, not merely a source
runtime warning.

## Testing Guidance

- Verify standalone MQTT streams build a direct connector path and reconnect after disconnect.
- Verify MQTT 3.1.1 remains the default and MQTT 5 sources receive the same payload bytes.
- Verify streams using `connector_key` fail clearly when the shared client is missing from storage
  or runtime.
- Verify shared-stream MQTT ingestion uses the same source connector model as non-shared streams.
- Verify decoder failures are surfaced after the source boundary rather than as source connection
  failures.
- Verify event-time extraction works only when a decoder is present and pipeline event-time mode is
  enabled.
- Verify sampler placement is before decode in planner explain output.
- Verify shared-client-backed streams consume the shared client's subscription settings rather than
  opening an independent standalone connection.
- Verify sampler behavior is per shared-ingest runtime for shared streams and per pipeline runtime
  for non-shared streams.
- Verify shared-client-backed sources do not convert ordinary delivery/runtime errors into
  `EndOfStream`.
- Verify a slow shared-client-backed consumer backpressures delivery instead of losing messages.
- Verify explicit shared-client close/delete is the only path that terminates a shared-client-backed
  source.
- Verify the shared-client runtime is recreated when a later pipeline reacquires the same
  `connector_key` after all prior users released it.

## Future Work

- Stream-local `broker_url` is already optional when `connector_key` is present. Any further
  request-shape simplification would be on top of the existing runtime ownership model, not a
  change to the runtime binding semantics themselves.
