# StreamingEncoderRewrite

## Summary

Fuses a `PhysicalBatch → PhysicalSinkEncoder` chain into a single
`PhysicalIncSinkEncoder`.

## Motivation

The physical plan builder always inserts a `PhysicalBatch` node when batching is
enabled (`batch_count` or `batch_duration`). This creates a two-node chain:

```
PhysicalBatch → PhysicalSinkEncoder(Immediate)
```

For registered sink encoders, the `BatchProcessor` and `SinkEncoderProcessor`
can be merged into one fused processor, eliminating an extra data-pass and an
async task. The `SinkEncoder` delivery API is streaming by contract: an encoder
that cannot emit incrementally may buffer records in `append` and emit from
`finish_delivery`.

## Conditions

The rule fuses the chain when **all** of the following hold:

1. The current node is `PhysicalSinkEncoder`.
2. Its unique child is `PhysicalBatch`.

`encoder=none` branches do not contain `PhysicalSinkEncoder`, so they keep a
standalone `PhysicalBatch`.

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
