# StreamingEncoderRewrite

## Summary

Fuses a `PhysicalBatch → PhysicalSinkEncoder` chain into a single
`PhysicalIncSinkEncoder` when the encoder supports streaming delivery.

## Motivation

The physical plan builder always inserts a `PhysicalBatch` node when batching is
enabled (`batch_count` or `batch_duration`). This creates a two-node chain:

```
PhysicalBatch → PhysicalSinkEncoder(Immediate)
```

For streaming-capable encoders (json) the `BatchProcessor` and
`SinkEncoderProcessor` can be merged into one fused processor, eliminating an
extra data-pass and an async task. Non-streaming encoders (future Parquet) keep
the two-node chain.

## Conditions

The rule fuses the chain when **all** of the following hold:

1. The current node is `PhysicalSinkEncoder`.
2. Its unique child is `PhysicalBatch`.
3. `encoder_registry.supports_streaming(encoder_kind)` returns `true`.

When the encoder does **not** support streaming, the chain stays intact.

## Rewrite

### Before

```
PhysicalSinkEncoder(encoder=json, common=default)
  └─ PhysicalBatch(batch_count=10, batch_duration=...)
      └─ PhysicalProject / ...
```

### After

```
PhysicalIncSinkEncoder(encoder=json, batch_count=10, batch_duration=...)
  └─ PhysicalProject / ...
```

The fused node carries:

- `sink_id` and `encoder` from `PhysicalSinkEncoder`
- `common` (batch params) from `PhysicalBatch`
- `children` from `PhysicalBatch` (skipping the batch node)

## Encoder Capability

`supports_streaming` is registered per encoder kind via
`EncoderRegistry::register_encoder_with_all_caps`.

| Encoder   | supports_streaming |
|-----------|--------------------|
| json      | true               |
| protobuf  | true (expected)    |
| Parquet   | false              |

## Processor Mapping

| Physical node          | Processor                                                     |
|------------------------|---------------------------------------------------------------|
| `PhysicalBatch`        | `BatchProcessor` (standalone)                                 |
| `PhysicalSinkEncoder`  | `SinkEncoderProcessor` in `StreamingBatchMode::Immediate`     |
| `PhysicalIncSinkEncoder` | `SinkEncoderProcessor` in `CountOnly` / `DurationOnly` / `Combined` |

## Interaction With Other Rules

This rule runs early in the physical optimization sequence, before all
`ByIndexProjection*` rules. This ensures `PhysicalIncSinkEncoder` is in place
before by-index projections are attached.
