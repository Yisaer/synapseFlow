# Runtime Sources

This directory contains source connector behavior and shared-source runtime topics.

- `on_change.md`: Source-side `on_change` input gating for per-pipeline source bindings.
- `stream_configuration.md`: Cross-source stream definition constraints and validation order.
- `mqtt.md`: MQTT source runtime behavior and connector ownership.
- `nng_pubsub.md`: NNG pub/sub source framing and lifecycle behavior.
- `file.md`: File source stream line framing, built-in schema, and watch behavior.
- `video.md`: Video source tuple schema, decoder behavior, and runtime constraints.
- `table_scan.md`: Bounded table scan design for history-backed tables.
- `shared_mqtt_client.md`: Shared MQTT client lifecycle and `connector_key` semantics.
- `shared/`: Shared-stream runtime behavior and optimizations.
