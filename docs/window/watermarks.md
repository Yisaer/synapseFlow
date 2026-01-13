# Watermarks and Time Progression

veloFlux uses **watermarks** to drive time progression for time-aware operators. Window
processors do not read wall-clock directly; they flush based on incoming watermark messages.

This document focuses on how processing-time and event-time watermark streams are produced and
how they interact with window operators.

See also: `docs/window/tumblingwindow.md` and `docs/window/slidingwindow.md`.

## Concepts

- **Tuple timestamp**: Each tuple carries a `timestamp` (a `SystemTime`) which is used as the
  time coordinate for windowing.
- **Watermark**: A message that signals "time has advanced up to T". Window operators may flush
  state when they observe watermarks that pass window boundaries.

## Watermark Producers

veloFlux uses different watermark processors depending on the time mode:

### Processing-time watermark processors

These processors generate watermarks based on wall-clock scheduling and are specific to time-based
windows:

- `TumblingWatermarkProcessor`: emits periodic processing-time watermarks at a fixed interval.
- `SlidingWatermarkProcessor`: emits periodic processing-time watermarks (tick interval is `1s`)
  to advance time and enable downstream window GC. When `slidingwindow(..., lookahead)` is used,
  it additionally schedules and emits per-tuple **deadline** processing-time watermarks at
  `deadline = tuple.timestamp + lookahead`.

### Event-time watermark processor

`EventtimeWatermarkProcessor` is responsible for the event-time watermark stream. It is independent
from processing-time watermark generation.

## Window Processors Consume Watermarks

`TumblingWindowProcessor` and `SlidingWindowProcessor` are watermark-driven:

- They flush and emit windows based on watermark messages observed upstream.
- Which semantics they implement (processing time vs event time) is determined by which watermark
  processor sits immediately upstream.
- The same operator logic is used for both modes; the contract is "flush based on received
  watermarks".

## Planning (High-level)

Physical planning inserts a watermark processor before time-aware window operators as needed:

- For processing-time execution, it inserts a process-time watermark processor configured for the
  window type.
- For event-time execution, it inserts an event-time watermark processor.

For `slidingwindow(..., lookahead)`, delayed emission requires the upstream watermark stream to
advance to each trigger's deadline (`t + lookahead`). In processing time, `SlidingWatermarkProcessor`
provides these deadline watermarks. In event time, this depends on upstream event-time watermark
behavior and must be treated as part of the operator contract.
