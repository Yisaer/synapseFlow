# RFC: slidingwindow

This RFC records the intended semantics and the current implementation status of
`slidingwindow(time_unit, lookback [, lookahead])`.

See also: `docs/syntax/windows/sliding_window.md` and `docs/syntax/windows/watermarks.md`.

## Motivation

Sliding windows are useful for "recent history" queries (lookback) and for "delayed emission"
use-cases (lookahead), while still using `GROUP BY` as the single windowed-aggregation entry point
in StreamDialect.

## Semantics model

Each incoming tuple is a trigger point with timestamp `t`. `lookback` and `lookahead` are measured
in `time_unit` (`ms` for milliseconds, `ss` for seconds):

- `slidingwindow(time_unit, lookback)`:
  - range: `[t - lookback, t]`
  - emission: immediate
- `slidingwindow(time_unit, lookback, lookahead)`:
  - range: `[t - lookback, t + lookahead]`
  - emission: when a watermark advances to `>= t + lookahead`

## Implementation Status

### Parser

- Supported in `GROUP BY` only.
- At most one window function is allowed per statement.
- `time_unit` accepts `'ms'` (milliseconds) and `'ss'` (seconds).
- Parsed into `SelectStmt.window = Window::Sliding { time_unit, lookback, lookahead }`.
- Non-window `GROUP BY` expressions remain in `SelectStmt.group_by_exprs`.

### Planner

- Logical window spec supports `Sliding { time_unit, lookback, lookahead }`.
- Physical plan includes `PhysicalSlidingWindow`.
- When `lookahead` is present, physical planning inserts an upstream watermark processor so the
  sliding window can observe deadline watermarks.

### Watermarks

- Processing time:
  - `SlidingWatermarkProcessor` emits periodic processing-time watermarks (tick interval is `1s`)
    to advance time and enable downstream GC.
  - When `lookahead` is present, it also generates per-tuple deadline processing-time watermarks at
    `deadline = tuple.timestamp + lookahead`.
  - This watermark stream is independent from event-time watermarking.
- Event time:
  - `EventtimeWatermarkProcessor` is responsible for event-time watermarks.
  - Sliding lookahead relies on the upstream event-time watermark stream reaching each trigger's
    deadline (`t + lookahead`). This is treated as a contract requirement.

### Runtime

- `SlidingWindowProcessor` is watermark-driven and uses `SystemTime`/`Duration` internally, so
  window boundaries are unit-agnostic:
  - `lookahead = None` emits windows immediately on data.
  - `lookahead = Some(L)` emits windows when it observes watermarks `>= t + L`.
- `StreamingSlidingAggregationProcessor` exists, and the optimizer can fuse
  `SlidingWindow -> Aggregation` into `PhysicalStreamingAggregation(window=sliding, ...)` when all
  aggregate calls support incremental updates.

## Open Questions / TODO

- Document the exact contract for event-time lookahead: who guarantees deadline watermark
  progression (`t + lookahead`) and what late/out-of-order behavior is expected.
- Document and test GC/trimming behavior for `slidingwindow` buffering (watermark-driven).
- Expand end-to-end tests for sliding windows across:
  - no-lookahead (immediate) vs lookahead (deadline-driven)
  - processing-time watermark vs event-time watermark pipelines
