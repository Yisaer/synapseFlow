# Pipeline Sampler Strategy

This document describes the design and implementation of the **Sampler** processor in veloFlux, enabling efficient downsampling of high-frequency data streams.

## Background

In many IoT and automotive use cases, data sources produce events at very high frequencies (e.g., 200Hz-1kHz CAN bus signals). Processing every single event in the pipeline—especially decoding raw bytes into structured tuples—can be prohibitively expensive and unnecessary for downstream consumers that only need updates at a lower rate (e.g., 1Hz or 10Hz).

## Goals

- **Reduce CPU Load**: Downsample data *before* expensive operations like decoding.
- **Configurable Strategy**: Support different downsampling behaviors (e.g., "latest value").
- **Seamless Integration**: Configure sampling at the stream definition level.

## Non-goals

- Complex content-based filtering (handled by `Filter` processor).
- Decoded message rate limiter (use window operators instead).

## Architecture: Bytes-First Processing

To achieve maximum performance, the Sampler is designed as a **bytes-first** processor.

- **Placement**: The `PhysicalSampler` is inserted into the pipeline immediately after the Source and **before** the Decoder (`PhysicalDecoder`).
- **Input**: Operates on `StreamData::Bytes(Vec<u8>)` (raw payloads).
- **Benefit**: Discarded messages are never decoded, saving significant CPU cycles.

The physical plan structure is:
`Source -> Sampler -> Decoder -> [Processors]`

## Configuration

### StreamDefinition

`StreamDefinition` gains an optional `sampler` configuration:

- `sampler.interval`: The sampling window duration (e.g., "100ms", "1s").
- `sampler.strategy`: The sampling strategy to apply.

Supported strategies:
1.  **`latest`**: Keep only the most recent message received within the interval.

Example (JSON):
```json
{
  "name": "can_stream",
  "sampler": {
    "interval": "1s",
    "strategy": "latest"
  }
}
```

## Strategies

### Latest Strategy

The `latest` strategy is a lossy downsampling method ideal for telemetry where intermediate values are less critical than the most current state.

**Algorithm:**
1.  Define a repeating time window of `interval` duration.
2.  Within the window, accept incoming `StreamData::Bytes`.
3.  Overwrite a buffer with the newest incoming payload.
4.  At the end of the window:
    - If a payload exists in buffer, emit it downstream.
    - Clear buffer.
    - Wait for next window.

**Result**: For a 200Hz input and 1s interval, the pipeline processes 1 message per second (the 200th, 400th, etc.), discarding 199 messages *without decoding them*.

### Packer Strategy

The `packer` strategy accumulates multiple raw payloads and merges them using a registered **Merger**. On each interval tick, the merger emits either merged bytes through `trigger()` or a decoded collection through `trigger_decoded()`. The exact merge key, overwrite behavior, and output mode are owned by the selected merger implementation.

**Use Case**: High-density CAN bus data where multiple signals (distinct CAN IDs) arrive rapidly. The Packer merges them into one consolidated frame, reducing downstream processing while preserving all distinct signals.

**Configuration Example**:
```json
{
  "sampler": {
    "interval": "1s",
    "strategy": {
      "type": "packer",
      "props": {
        "merger": {
          "type": "can_merger",
          "props": { "schema": "/path/to/schema.dbc" }
        }
      }
    }
  }
}
```

**Algorithm:**
1.  Accept incoming `StreamData::Bytes`.
2.  Pass bytes to the registered `Merger::merge()` method.
3.  At the end of the interval, call `Merger::trigger_decoded()` when the merger supports fused decode; otherwise call `Merger::trigger()`.
4.  Fused mergers emit a decoded collection. Bytes-output mergers emit bytes for the downstream decoder.

**Requirements:**
- The `merger.type` must be registered in the `MergerRegistry` provided by the binary.
- *Note*: `can_merger` in the example above is hypothetical. Users must ensure the specified merger type is available in their VeloFlux distribution.
- The stream decoder must be compatible with the merged output format.

### SDV `gbf` Packer Semantics

The SDV `gbf` merger is a fused decode-capable merger. It parses the GBF outer packet, accumulates supported inner frames, and emits a decoded collection directly. It does not have a bytes-output fallback.

- `merger.type = "gbf"` requires `decoder.type = "gbf"` in stream configuration.
- The merger and normal decoder share the stream's complete `CompiledGbfSchema`; merger props do not repeat packet layout, format paths, naming, CAN ID mapping, or clamping options.
- The fused merger currently supports GBF schemas whose `format.type` is `can`.

For a CAN-format GBF schema, the fused merger keeps newest-wins semantics within each sampler interval:

- CAN IDs are represented as `u32` inside the decoder and merger. The GBF schema can still choose a narrower field type, such as `u16be`, when the source format uses one.
- Frames with CAN IDs that are absent from the DBC are discarded during merge.
- Non-multiplexed CAN IDs are keyed by CAN ID.
- Multiplexed CAN IDs are keyed by `(can_id, mux_value)`.
- Repeated frames with the same key keep only the newest payload.

The mux value is decoded from the payload using the DBC multiplexer signal definition. Future GBF inner formats must add explicit inner handlers with their own key semantics; they must not fall back to generic GBF byte merging.

## Implementation checklist

- [x] Extend `StreamDefinition` with `sampler.interval` and `sampler.strategy`.
- [x] Implement `PhysicalSampler` node in physical planner.
- [x] Ensure `PhysicalSampler` wraps `PhysicalSource` and is wrapped by `PhysicalDecoder` (bytes-first).
- [x] Implement `SamplerProcessor` with `latest` strategy logic.
- [x] Implement `SamplerProcessor` with `packer` strategy logic.
- [x] Verify shutdown handling (emit buffered value).
- [x] Verify with integration tests (stats: 5-in/1-out for latest, merger tests for packer).
