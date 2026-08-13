# Prometheus Metrics Design

## Overview

veloFlux uses a single Prometheus definition layer for all exported metrics.

All Prometheus collectors must be declared in one dedicated crate and must not be created inside
business modules such as connectors, processors, runtime services, or telemetry collectors.

This design enforces:

- one source of truth for metric definitions
- stable naming and label conventions
- consistent subsystem boundaries
- predictable Prometheus query patterns

## Definition Ownership

Prometheus metric definitions are owned by the `veloflux_metrics` crate.

Other crates may:

- obtain an existing collector handle from `veloflux_metrics`
- update metric values
- attach label values to an existing collector

Other crates must not:

- call `prometheus::register(...)`
- create `Opts` or `HistogramOpts` directly
- hardcode full Prometheus metric names
- define ad hoc `Lazy<...>` collectors in local modules

## Naming Model

Every metric must use the Prometheus three-part naming model:

- `namespace`
- `subsystem`
- `name`

The namespace is fixed:

- `namespace = veloflux`

The subsystem identifies a stable observability domain:

- `mqtt_source`
- `mqtt_sink`
- `shared_stream`
- `processor`
- `runtime`
- `flow_instance`

The name identifies the metric itself:

- `records_in_total`
- `records_out_total`
- `messages_in_total`
- `messages_out_total`
- `errors_total`
- `handle_duration_seconds`
- `memory_usage_bytes`

The final metric name is generated as:

```text
<namespace>_<subsystem>_<name>
```

Examples:

- `veloflux_mqtt_source_messages_in_total`
- `veloflux_mqtt_sink_messages_out_total`
- `veloflux_shared_stream_messages_out_total`
- `veloflux_processor_errors_total`
- `veloflux_processor_handle_duration_seconds`
- `veloflux_runtime_memory_usage_bytes`
- `veloflux_flow_instance_cpu_usage_percent`

## Naming Rules

The following naming rules apply to every new metric:

- Use `snake_case`.
- Counter names must end with `_total`.
- Histogram names must carry the unit in the name, such as `_seconds` or `_bytes`.
- Gauge names should describe the observed quantity directly and should not append `_gauge`.
- The `name` part must not repeat the subsystem meaning.

Good examples:

- `veloflux_processor_records_in_total`
- `veloflux_runtime_heap_in_use_bytes`

Bad examples:

- `veloflux_processor_processor_records_in_total`
- `veloflux_runtime_memory_gauge`

## Label Conventions

Label keys must use `snake_case` and must stay consistent across subsystems.

Shared label keys:

- `flow_instance`
- `pipeline_id`
- `processor_id`
- `connector`
- `key`
- `metric`
- `kind`

Alias labels must not be introduced when an existing canonical label already exists.

For example:

- use `flow_instance`
- do not use `flow_instance_id`

The `key` label is reserved for manager-owned shared runtime resources that are addressed by a
stable metadata key instead of a connector id or pipeline id.

Examples:

- shared MQTT client key
- future shared runtime resource keys

## Subsystem Boundaries

### `mqtt_source`

This subsystem contains source-side connector message ingress and egress counters. Ingress means a
message received externally; egress means the same message was handed to the flow runtime.

Examples:

- `veloflux_mqtt_source_messages_in_total`
- `veloflux_mqtt_source_messages_out_total`
- `veloflux_nng_pubsub_source_messages_in_total`
- `veloflux_nng_pubsub_source_messages_out_total`

Expected labels:

- `flow_instance`
- `connector`

### `mqtt_sink`

This subsystem contains sink-side connector message ingress and publish-success counters. A
logical message is counted once on ingress and once after confirmed external output; retrying the
same message does not increment ingress again.

Examples:

- `veloflux_mqtt_sink_messages_in_total`
- `veloflux_mqtt_sink_messages_out_total`
- `veloflux_nng_pubsub_sink_messages_in_total`
- `veloflux_nng_pubsub_sink_messages_out_total`

Expected labels:

- `flow_instance`
- `connector`

### `mqtt_shared_client`

This subsystem contains metrics for the shared MQTT client runtime resource addressed by shared
client `key`.

These metrics describe the manager-owned shared connection itself, not any individual source or
sink connector that happens to use that shared connection.

Examples:

- `veloflux_mqtt_shared_client_connected`
- `veloflux_mqtt_shared_client_active_refs`
- `veloflux_mqtt_shared_client_rx_messages_total`
- `veloflux_mqtt_shared_client_tx_messages_total`
- `veloflux_mqtt_shared_client_errors_total`
- `veloflux_mqtt_shared_client_reconnects_total`

Expected labels:

- `flow_instance`
- `key`

For error counters that need stable classification, add:

- `kind`

This subsystem should be used for observability of:

- lazy start and zero-reference reclaim behavior
- shared-client connection health
- shared-client ingress and egress throughput
- non-terminal runtime error trends
- reconnect churn

It must not be used to describe:

- one source connector instance
- one sink connector instance
- process-wide runtime state

Those concerns belong to `mqtt_source`, `mqtt_sink`, and `runtime` respectively.

### `shared_stream`

This subsystem contains metrics for the shared stream runtime resource addressed by shared stream
`key`.

These metrics describe the manager-owned shared ingest runtime itself, not any individual
downstream pipeline processor that consumes the stream.

Examples:

- `veloflux_shared_stream_running`
- `veloflux_shared_stream_active_subscribers`
- `veloflux_shared_stream_starts_total`
- `veloflux_shared_stream_errors_total`
- `veloflux_shared_stream_messages_in_total`
- `veloflux_shared_stream_messages_out_total`

Expected labels:

- `flow_instance`
- `key`

For error counters that need stable classification, add:

- `kind`

This subsystem should be used for observability of:

- lazy start and zero-subscriber reclaim behavior
- active subscriber fan-out state
- shared-stream publish activity at the shared runtime boundary
- non-terminal runtime error trends

It must not be used to describe:

- one downstream consumer pipeline
- one processor inside the shared ingest pipeline
- row-level processor throughput

Those concerns belong to `processor`.

The `messages_*_total` metrics in this subsystem count one successful shared-stream publish event
as one message, regardless of how many subscribers are currently attached.

### `processor`

This subsystem contains metrics for processor execution and processor-level custom stats.

Standard processor metrics:

- `veloflux_processor_records_in_total`
- `veloflux_processor_records_out_total`
- `veloflux_processor_errors_total`
- `veloflux_processor_handle_duration_seconds`
- `veloflux_processor_send_backpressure_waits_total`

`records_in_total` and `records_out_total` strictly count rows directly observed through
collections. A processor declares the corresponding direction together with the
`collections_in` or `collections_out` custom counter. One observed collection increments the
collection counter by one and the row counter by `collection.num_rows()`. The counters start at
zero before the first collection. Processors in the raw-message or encoded-message domain do not
expose these series; use their custom message and byte metrics instead.

The declaration comes from the physical plan and remains fixed for the processor lifetime. Runtime
data does not create a previously undeclared row series. In particular, an encoder exposes
`records_in_total` for its collection input but not `records_out_total` for its message output.

Expected labels:

- `flow_instance`
- `pipeline_id`
- `processor_id`

### Processor Custom Metrics

Processor-specific metrics are exposed through shared metric families instead of dynamically
registering a new Prometheus collector for every processor-local stat.

Shared families:

- `veloflux_processor_custom_gauge`
- `veloflux_processor_custom_counter_total`

Expected labels:

- `flow_instance`
- `pipeline_id`
- `processor_id`
- `metric`

Examples of `metric` label values:

- `messages_in`
- `messages_out`
- `bytes_in`
- `bytes_out`
- `bytes_delivered`
- `rows_buffered`
- `collections_in`
- `collections_forwarded`
- `collections_suppressed`

This keeps the Prometheus definition layer stable while still allowing processors to expose
structured custom stats.

### `runtime`

This subsystem contains process-wide runtime metrics.

Examples:

- `veloflux_runtime_cpu_usage_percent`
- `veloflux_runtime_memory_usage_bytes`
- `veloflux_runtime_tokio_tasks_inflight`
- `veloflux_runtime_heap_in_use_bytes`
- `veloflux_runtime_heap_in_allocator_bytes`

These metrics describe the whole process and therefore must not be labeled as a specific
`flow_instance`.

### `flow_instance`

This subsystem contains metrics that are truly scoped to an individual in-process flow instance.

Example:

- `veloflux_flow_instance_cpu_usage_percent`

Expected labels:

- `flow_instance`

## Runtime and Flow Instance Separation

Process-level runtime metrics and flow-instance metrics must not be mixed.

The distinction is:

- `runtime`: the entire veloFlux process
- `flow_instance`: one flow instance inside the process

This separation is required so that Prometheus queries remain semantically correct.

For example:

- overall process memory belongs to `runtime`
- per-instance CPU sampling belongs to `flow_instance`

## Query Ergonomics

The naming and labeling rules are designed to keep Prometheus queries simple and uniform.

Examples:

```promql
sum by (connector) (rate(veloflux_mqtt_source_messages_in_total[5m]))
```

```promql
sum by (flow_instance, key) (rate(veloflux_mqtt_shared_client_rx_messages_total[5m]))
```

```promql
veloflux_mqtt_shared_client_connected{flow_instance="default", key="shared_demo"}
```

```promql
sum by (flow_instance, pipeline_id, processor_id) (
  rate(veloflux_processor_records_out_total[5m])
)
```

```promql
veloflux_runtime_memory_usage_bytes
```

```promql
veloflux_flow_instance_cpu_usage_percent{flow_instance="default"}
```

## Design Summary

After the redesign, the metrics system follows four hard rules:

1. All Prometheus metric definitions live in `veloflux_metrics`.
2. Every metric uses explicit `namespace`, `subsystem`, and `name`.
3. Labels are canonicalized and reused consistently.
4. Runtime, connector, processor, and flow-instance metrics are separated by stable subsystem
   boundaries.

## Shared MQTT Client Metrics

The shared MQTT client is a first-class runtime resource. It must therefore expose its own metrics
through the `mqtt_shared_client` subsystem instead of reusing `mqtt_source`, `mqtt_sink`, or
`runtime`.

The proposed first-stage metrics are:

### `veloflux_mqtt_shared_client_connected`

- Type: gauge
- Labels: `flow_instance`, `key`
- Meaning: whether the shared MQTT client is currently connected to the broker (`0` or `1`)
- Update points:
  - set to `1` after a successful connection acknowledgement
  - set to `0` after disconnect, poll failure, requests-done transition, or runtime shutdown
- Notes:
  - this metric describes broker connectivity only
  - it must not be overloaded to mean that the shared-client runtime exists

### `veloflux_mqtt_shared_client_active_refs`

- Type: gauge
- Labels: `flow_instance`, `key`
- Meaning: number of active runtime handles currently holding the shared MQTT client
- Update points:
  - increment after successful acquire
  - decrement when the held shared client is released
- Notes:
  - this metric is the main lifecycle driver for lazy start and zero-reference reclaim analysis
  - it must be sourced from manager reference tracking, not inferred from subscriber count

### `veloflux_mqtt_shared_client_rx_messages_total`

- Type: counter
- Labels: `flow_instance`, `key`
- Meaning: total number of MQTT publish messages accepted from the broker by the shared client
- Update points:
  - increment after a broker publish event is received and accepted by the shared client
- Notes:
  - this is the upstream ingress counter for the shared client itself
  - it should count MQTT messages, not decoded rows and not bytes

### `veloflux_mqtt_shared_client_tx_messages_total`

- Type: counter
- Labels: `flow_instance`, `key`
- Meaning: total number of MQTT publish messages successfully sent to the broker through the shared
  client
- Update points:
  - increment only after shared-client publish succeeds
- Notes:
  - this counter allows one shared client key to be observed when it is reused by sink-side
    traffic

### `veloflux_mqtt_shared_client_errors_total`

- Type: counter
- Labels: `flow_instance`, `key`, `kind`
- Meaning: total number of shared-client runtime errors
- Update points:
  - increment whenever the shared-client runtime observes an error that should remain
    non-terminal
- Notes:
  - `kind` must be a small stable enum
  - `kind` values should describe a lifecycle stage, not carry free-form error messages
  - recommended values:
    - `subscribe`
    - `poll`
    - `publish`
    - `disconnect`
    - `requests_done`

### `veloflux_mqtt_shared_client_reconnects_total`

- Type: counter
- Labels: `flow_instance`, `key`
- Meaning: total number of successful reconnect transitions of the shared MQTT client
- Update points:
  - increment when the shared client returns to connected state after previously being disconnected
- Notes:
  - this metric should count reconnect success events rather than every retry loop iteration
  - this keeps reconnect churn observable without inflating the counter with internal retry noise

## Shared MQTT Client Design Constraints

When adding shared MQTT client metrics, the following constraints apply:

- Definitions must be added to `veloflux_metrics`, not to flow business modules.
- Final names must be generated through:
  - `namespace = veloflux`
  - `subsystem = mqtt_shared_client`
  - the specific metric `name`
- Business code may only fetch collector handles and update them.
- The canonical resource label is `key`.
- Do not introduce alias labels such as `connector_key` or `flow_instance_id`.
- Do not label these metrics by `topic`, `broker_url`, `client_id`, or free-form error message.
- Do not merge shared-client metrics into `mqtt_source` or `mqtt_sink`; those subsystems describe
  connector behavior, not shared runtime resources.
