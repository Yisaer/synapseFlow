# Sink Delivery Compression

The `PhysicalSinkCompress` node compresses encoded delivery output between the encoder and connector stages in the sink delivery pipeline.

## Pipeline Position

```
PhysicalSinkConnector
└─PhysicalSinkCompress(codec=gzip)
  └─PhysicalSinkEncoder(encoder=json, ...)
    └─...
```

Without compression, `PhysicalSinkCompress` is absent and `PhysicalSinkConnector` directly receives from `PhysicalSinkEncoder`.

## Supported Codecs

| Codec | Level range | Default |
|-------|-------------|---------|
| `gzip` | 0–9 | library default (≈6) |
| `zstd` | negative (fast) to positive (high ratio) | 0 |

## Behavior

- The compressor is created once on processor startup and reused across deliveries.
- Each delivery boundary (`START`/`END`) maps to one compressed stream:
  - `START`: calls `begin_delivery()`, writes gzip/zstd header on the first byte output.
  - Middle chunks: compressed bytes forwarded only when natural drain produces output.
  - `END`: flushes remaining data and appends trailer, then forwards.
  - `ABORT`: discards in-progress state; forwards `ABORT` downstream only if `START` was already emitted.
- The connector receives plain bytes and is unaware of compression.

## Configuration (Rust API)

```rust
SinkDefinition::new("my_sink", SinkType::File, SinkProps::File(...))
    .with_compression(CompressionCodec::gzip())

SinkDefinition::new("my_sink", SinkType::File, SinkProps::File(...))
    .with_compression(CompressionCodec::zstd_with_level(3))
```

## Stats Accounting

- `record_in`: each `EncodedDelivery` event counts as 1.
- `record_out`: only completed deliveries (`END`/`START|END`) count as 1. Matches the connector's accounting.
