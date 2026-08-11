# MQTT Sink Connector Design

## Background

The MQTT sink publishes final sink-branch payload bytes to an MQTT broker. It is a byte-delivery
connector, so encoder/output behavior upstream of the connector materially affects what is
published.

Like MQTT sources, the sink can either own its own standalone connection or reuse a shared client
selected by `connector_key`.

## Goals

- Define the runtime contract of the MQTT sink connector.
- Clarify standalone versus shared-client-backed sink behavior.
- Explain how encoder and sink-output features interact with MQTT delivery.
- Document the semantics of `retain`, `qos`, and `max_packet_size`.

## Non-Goals

- MQTT source semantics.
- Generic encoder design beyond the MQTT sink boundary.
- API reference documentation for pipeline create/update.

## Configuration Model

MQTT sink definitions currently accept:

- `broker_url`
- `topic`
- `qos`
- optional `retain`
- optional `client_id`
- optional `connector_key`
- optional `max_packet_size`
- optional `protocol_version` (`v3` or `v5`)
- optional ordered `user_properties`
- encoder config
- sink output config (`full` / `delta`, `omit_if_empty`, encoder transform, batching, common sink
  props)

Manager validates `topic` as required. The topic may use static property
templates such as `vehicle/{{ prop("vin") }}/telemetry`; it is rendered once
during pipeline apply and validated as an MQTT publish topic. `broker_url` is required only when `connector_key` is
absent. `retain` defaults to `false`, and `qos` falls back to the manager default when omitted.

`protocol_version` defaults to `v3`, meaning MQTT 3.1.1. Static User Properties require effective
protocol `v5`. Each entry has literal string `key` and `value` fields:

```json
{
  "protocol_version": "v5",
  "user_properties": [
    { "key": "source", "value": "veloflux" },
    { "key": "tag", "value": "primary" },
    { "key": "tag", "value": "edge" }
  ]
}
```

The array preserves declaration order and permits duplicate keys, matching MQTT 5 wire semantics.
Keys and values must satisfy MQTT UTF-8 String limits. Every value is currently treated as a static
literal; row or source-property templates are not implemented.

No other MQTT sink property supports connector templates. See
[connector property templates](../../../syntax/connectors/property_templates.md).

Flow runtime fills a default standalone client id when none is provided:

- `<pipeline_id>-<sink_id>`

## Delivery Path

The MQTT connector sits at the end of the sink branch.

Conceptually, the branch is:

```text
Project
  -> RowDiff?
  -> EmptySuppress?
  -> Batch?
  -> Encoder?
  -> DataSink(MQTT)
```

The connector itself only publishes the final bytes it receives from the sink processor. It does
not know about SQL rows, row diff masks, or collection batching decisions directly.

## Encoder Interaction

MQTT sink is a bytes-oriented connector.

Current implications:

- normal usage expects an encoder node to turn rows/collections into bytes
- encoder transforms, if configured, happen before MQTT delivery
- batching decisions also happen before MQTT delivery

Unlike memory collection or Kuksa sinks, MQTT does not implement collection-native delivery. If a
pipeline reaches the MQTT sink without an encoder and tries to deliver collections directly, the
connector reports a runtime error because only byte payload delivery is implemented.

## Shared MQTT Client Interaction

### Standalone sink

Without `connector_key`, the sink lazily creates its own client during `ready` / first send.

Standalone connection ownership uses:

- sink-local `broker_url`
- sink-local `client_id` or the derived default
- sink-local `max_packet_size`
- sink-local `protocol_version`, defaulting to MQTT 3.1.1

### Shared-client-backed sink

With `connector_key`, the sink lazily acquires the shared client instead of creating a standalone
connection.

In this mode:

- connection ownership comes from the shared client resource
- sink-local `broker_url`, `client_id`, and `max_packet_size` no longer drive the live connection
- publish-time `topic`, `qos`, and `retain` still come from the sink definition
- the shared resource owns `protocol_version`; a connector-local value is rejected

This lets one shared client connection be reused across multiple MQTT sinks while preserving
sink-local publish routing and retain behavior.

## Retain / QoS / Packet Size Semantics

`retain`

- only affects publish behavior
- is always sink-local, including in shared-client mode

`qos`

- source-side shared clients use the shared client's subscription QoS
- sink-side publish QoS remains sink-local and is applied for each publish

`max_packet_size`

- controls MQTT option limits when a standalone or shared client is created
- in standalone sink mode, sink-local `max_packet_size` config owns the connection limit
- in shared-client sink mode, the shared client config owns the connection limit instead

The default packet limit is `64 MiB` when the chosen connection owner does not specify an override.

## Interaction With Batching / Row Diff / Omit-If-Empty

MQTT sink does not interpret these sink-output features itself; it receives their result.

Relevant interactions are:

- batching changes how many rows are encoded into one publish payload
- row diff changes the row values and optional output-mask metadata before encoding
- `omit_if_empty` may suppress the branch so the MQTT connector publishes nothing
- encoder transform reshapes each encoded row item before the final bytes are emitted

This means MQTT sink testing should focus on the published payload bytes and publish count, not on
re-implementing row-diff or batching logic inside the connector.

See:

- [row_diff_output.md](/Users/yisa/Downloads/Github/veloFlux/docs/runtime/sinks/output/row_diff_output.md)
- [omit_if_empty.md](/Users/yisa/Downloads/Github/veloFlux/docs/runtime/sinks/output/omit_if_empty.md)
- [encoder_transform.md](/Users/yisa/Downloads/Github/veloFlux/docs/runtime/sinks/encoders/encoder_transform.md)

## Error Handling

Connection establishment is lazy. The sink initializes its client on `ready` or first send.

Standalone sink behavior:

- background event loop tracks connection state and reconnects with exponential backoff
- publish while disconnected fails with the last known connection error when available
- close disconnects the client and aborts the event loop task

Shared-client-backed sink behavior:

- acquisition failure for the shared key fails readiness immediately
- publish while the shared client is disconnected fails through the shared client's connection
  state
- close only releases the acquired shared handle; the underlying shared client remains alive until
  all holders release it

At the sink-processor layer, repeated readiness failures are logged and data may be dropped until
the connector becomes ready again.

## Testing Guidance

- Verify a normal bytes encoder path publishes the expected payload bytes.
- Verify MQTT sink with `connector_key` reuses the shared connection instead of creating a
  standalone client.
- Verify sink-local publish `topic`, `qos`, and `retain` still apply when using a shared client.
- Verify standalone `max_packet_size` is honored by the sink-owned connection.
- Verify shared-client sink behavior inherits packet-size limits from the shared client resource.
- Verify row diff, omit-if-empty, and encoder transform change the published payload/count only
  through upstream sink-output stages.
- Verify runtime errors are surfaced when MQTT sink is used without a bytes-producing encoder path.
- Verify disconnect/reconnect behavior eventually restores readiness for standalone clients.
- Verify MQTT 5 publishes static User Properties in declaration order, including duplicate keys.
- Verify MQTT 3.1.1 sinks reject non-empty User Properties without changing normal v3 delivery.

## Future Work

- If MQTT sink ever needs collection-native delivery, that should be introduced as an explicit new
  connector contract instead of silently overloading the existing bytes-only behavior.
