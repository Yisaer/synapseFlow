# Sink Connectors

This directory contains connector-specific sink behavior.

- `mqtt.md`: MQTT sink delivery behavior, shared-client binding, and output-feature interaction.
- `http.md`: HTTP raw and multipart delivery, error classification, Content-Type handling, and
  delivery lifecycle.
- `memory.md`: In-process memory sink connector behavior.
- `file.md`: Local file sink byte delivery, filename generation, temporary files, and retention.
- `nng_pubsub.md`: NNG pub/sub sink framing and lifecycle behavior.
- `kuksa.md`: Kuksa sink design and typed update semantics.
- `kura.md`: Kura sink design, gRPC SetCurrent execution model, and mapping file contract.
- `video.md`: Video sink tuple contract, planner validation, and GStreamer-backed file output.
