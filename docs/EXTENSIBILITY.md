# Connector/Codec Extensibility

This document describes how we keep the open-source repository lean while still
allowing private/custom connectors and codecs to be built and distributed.

## Goals

- Ship a small OSS core (MQTT + JSON) without leaking private code.
- Allow teams to implement proprietary connectors/codecs that plug into the same APIs.
- Keep the public API surface stable and versioned so private crates can evolve independently.
- Support both build-time (Cargo feature) and optional runtime (plugin) loading strategies.

## Architecture Overview

```text
flow (OSS crate)
├── connector::
│   ├── traits / common types
│   ├── registry (feature gated)
│   └── builtin implementations (MQTT/Nop/Mock)
└── codec::
    ├── traits / common types
    ├── registry (feature gated)
    └── builtin implementations (JSON)

flow-custom (private crate)
├── depends on `flow`
├── enables `custom-connectors` or `custom-codecs` features
└── registers proprietary implementations into the registries
```

Key concepts:

- **Traits** remain in the public crate. All connectors implement `SourceConnector` or
  `SinkConnector`; all codecs implement `CollectionEncoder`/`RecordDecoder`.
- **Registry** is the indirection layer. It owns the mapping
  `connector_id -> factory` and `encoder_id -> factory`.
- **Feature Flags** guard the registry hooks. OSS builds without custom components
  pay almost zero cost (registries include only built-ins). A build that enables
  `custom-connectors` or `custom-codecs` simply adds more entries.

## Build-Time Extension

1. Create a private crate (e.g. `flow_custom_connectors`).
2. Add `flow = { path = "../synapseFlow/src/flow", features = ["custom-connectors"] }`.
3. Implement the desired connector and expose a `register()` function:

```rust
pub fn register_custom_connectors(registry: &mut ConnectorRegistry) {
    registry.register_source("my-source", Arc::new(|cfg| Box::new(MySource::new(cfg))));
    registry.register_sink("my-sink", Arc::new(|cfg| Box::new(MySink::new(cfg))));
}
```

4. In the final binary crate, depend on both `flow` and the private crate,
   and call registration before building pipelines.

This approach keeps everything statically linked and compatible with cross
compilation / embedded deployments.

## Runtime Extension (Optional)

For teams that need hot-plug capability, the registry exposes a `load_from_dir`
helper that scans a directory for `.so/.dylib` plugins. Each plugin exports a
`register_connector_plugin()` symbol that receives a mutable registry.
This path is fully optional and gated behind the `dynamic-connectors` feature.

## Configuration Flow

1. Parser/planner references connectors/encoders by logical ID.
2. Processor builder requests implementations from `ConnectorRegistry` or `EncoderRegistry`.
3. Registry resolves the ID by checking:
   - Built-in implementations.
   - Custom registrations (if feature enabled).
   - Dynamic modules (if feature enabled and configured).
4. If the ID is missing, pipeline creation fails with a descriptive error.

## Testing Strategy

- OSS repo keeps existing tests for built-in components.
- Private repo includes integration tests that register custom components and run
  representative pipelines.
- CI for the private repo uses the same registry hooks, ensuring upstream changes
  that modify traits or registry contracts are caught early.

## Distribution

- Public crate publishes `connector`/`codec` traits and registries on crates.io.
- Private crate is distributed through an internal git or crates.io-alike registry.
- Deployments that need proprietary functionality build the binary with
  `--features custom-connectors,custom-codecs` and include the private crate as
  a dependency.
