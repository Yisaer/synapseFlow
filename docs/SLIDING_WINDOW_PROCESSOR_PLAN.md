# Sliding Window Processor Plan

This document captures the requirements, design, and implementation steps for adding runtime support for `slidingwindow(...)` in the processor layer (step 3). It follows the same principles as the existing tumbling window pipeline: **time progression and flushing are driven by incoming `Watermark` messages**.

This plan assumes:
- `TimeUnit` currently supports only `ss` (seconds).
- `slidingwindow('ss', lookback)` has `lookahead = None`.
- `slidingwindow('ss', lookback, lookahead)` has `lookahead = Some(lookahead)`.
- We **do not** support `StreamingAggregationProcessor` for sliding windows yet; optimizer rewrites for sliding are temporarily disabled (tests kept but skipped).

## Requirements

### Runtime semantics
- Sliding window is triggered by **incoming data** (each received record is a trigger point).
- The window range for a trigger at time `t` is:
  - `slidingwindow('ss', lookback)` → `[t - lookback, t]` emitted immediately on receiving the record.
  - `slidingwindow('ss', lookback, lookahead)` → `[t - lookback, t + lookahead]` emitted when time has advanced to `t + lookahead`.
- Time advancement is driven by `StreamData::Watermark(ts)` (no direct dependency on wall-clock inside window processors).

### Two time modes
- **Processing time (default)**:
  - Input `Collection` tuples have non-decreasing `tuple.timestamp` (monotonic).
  - Window processor can keep ordered queues and perform efficient trimming.
- **Event time (optional)**:
  - Input `Collection` tuples may arrive out-of-order in `tuple.timestamp`.
  - Window processor must use a time-indexed structure for range queries and trimming.

### Watermark for lookahead
- For `slidingwindow(..., lookahead)`:
  - The upstream watermark stage must emit a **deadline watermark per trigger record** at `deadline = trigger_timestamp + lookahead`.
  - This watermark is what enables `SlidingWindowProcessor` to emit `[t - lookback, t + lookahead]`.

### Non-goals (for now)
- No streaming aggregation support for sliding windows:
  - Optimizer should not rewrite `SlidingWindow -> Aggregation` into `PhysicalStreamingAggregation`.
  - Existing rewrite test(s) for sliding may remain but must be marked `#[ignore]` until runtime support exists.

## Physical plan shape
- Desired chain for windowed aggregation:
  - `... -> PhysicalWatermark(Sliding) -> PhysicalSlidingWindow -> PhysicalAggregation -> ...`
- `PhysicalSlidingWindow` always expects its single upstream child to be a `PhysicalWatermark` node (mirroring tumbling windows).

## WatermarkProcessor design

### Add `WatermarkConfig::Sliding`
- Extend watermark config to include sliding metadata:
  - `time_unit`, `lookback`, `lookahead: Option<u64>`, `strategy`.

### Sliding watermark emission (Processing time)
- Keep forwarding of all incoming `StreamData::*` as today.
- Additionally, when `lookahead == Some(L)`:
  - On every incoming `Collection`, for each tuple:
    - Compute `deadline = tuple.timestamp + Duration::from_secs(L)`.
    - Register `deadline` in a min-heap / ordered queue of pending deadlines.
  - A background task (or async loop) checks the earliest pending deadline and emits
    `StreamData::Watermark(deadline)` when wall-clock has reached that deadline.
  - Ensure watermarks are monotonic (do not emit older-than-last watermark).
- When `lookahead == None`:
  - No per-tuple deadline scheduling is required (window emits immediately on data).

### Sliding watermark emission (Event time)
- Not implemented for now: the system does not yet support event-time watermark semantics end-to-end.
- Expected future behavior (to be confirmed when event-time watermarking is introduced):
  - Watermarks are sourced externally (event time) and forwarded downstream.
  - No per-tuple wall-clock scheduling exists in event time.
  - `SlidingWindowProcessor` emits a delayed (lookahead) window only when it receives an external watermark `>= end = t + lookahead` in event time.

## SlidingWindowProcessor design
Implementation mirrors `TumblingWindowProcessor`:
- Two internal states: ProcessingTime and EventTime.
- Both accept input `Collection`, `Watermark`, and `ControlSignal`.
- Flush behavior is driven by `Watermark` (especially for lookahead).

### Processing time state (monotonic timestamps)
State:
- `rows: VecDeque<Tuple>`: buffered tuples in non-decreasing timestamp order.
- `pending: VecDeque<WindowRequest>`: only used when `lookahead.is_some()`.
- `lookback: Duration`, `lookahead: Option<Duration>`.

WindowRequest:
- `{ start: SystemTime, end: SystemTime }` where:
  - `start = t - lookback`
  - `end = t + lookahead` (or `t` if `lookahead` absent, but that path emits immediately and does not enqueue)

On `Collection`:
- Append tuples to `rows`.
- For each tuple with trigger time `t`:
  - If `lookahead.is_none()`:
    - Build window range `[t - lookback, t]`.
    - Extract range from `rows` and immediately emit a `RecordBatch` (one window per trigger).
  - If `lookahead.is_some()`:
    - Enqueue `WindowRequest { start, end }` into `pending`.

On `Watermark(w)`:
- If `lookahead.is_some()`:
  - While `pending.front().end <= w`:
    - Pop request and emit a `RecordBatch` containing tuples within `[start, end]`.
- Trim buffered tuples:
  - Determine minimal needed start:
    - If `pending` non-empty: `min_start = pending.front().start` (pending is in trigger order; timestamps are monotonic).
    - Else: `min_start = w - lookback` (best-effort trimming).
  - Pop `rows.front()` while `rows.front().timestamp < min_start`.

### Event time state (out-of-order timestamps)
State:
- `rows_by_ts: BTreeMap<u64, Vec<Tuple>>` keyed by epoch seconds (or a chosen timestamp bucket).
- `pending_by_end: BTreeMap<u64, Vec<WindowRequest>>` keyed by end time (epoch seconds).

On `Collection`:
- Insert tuples into `rows_by_ts` by their timestamp bucket.
- For each tuple trigger time `t`:
  - If `lookahead.is_none()`:
    - Range query `[t-lookback, t]` across `rows_by_ts` and emit immediately.
  - If `lookahead.is_some()`:
    - Insert request into `pending_by_end[end_bucket]`.

On `Watermark(w)`:
- While smallest end_bucket in `pending_by_end` is `<= watermark_bucket`:
  - Pop requests and range query `[start, end]` from `rows_by_ts` to emit.
- Trim `rows_by_ts` based on minimal required start (similar logic as processing time, but computed from remaining pending or watermark).

## Optimizer behavior (temporary)
- Disable `streaming_aggregation_rewrite` for `PhysicalSlidingWindow`:
  - Do not fuse `SlidingWindow -> Aggregation` into `PhysicalStreamingAggregation`.
- Keep the existing sliding rewrite test but mark it `#[ignore]` until sliding streaming aggregation is supported.

## Implementation Steps
1. **Planner alignment**
   - Ensure physical plan always inserts `PhysicalWatermark(Sliding)` before `PhysicalSlidingWindow`.
2. **Disable sliding streaming rewrite**
   - Update optimizer rewrite logic to skip `PhysicalSlidingWindow`.
   - Keep the test and mark it `#[ignore]`.
3. **WatermarkProcessor: sliding config**
   - Implement `WatermarkConfig::Sliding`.
   - Add `SlidingWatermarkProcessor` (or equivalent) that schedules per-tuple deadline watermarks when `lookahead=Some`.
4. **SlidingWindowProcessor**
   - Add a new processor modeled after `TumblingWindowProcessor`.
   - Implement both ProcessingTime and EventTime state machines (default: processing time).
5. **ProcessorBuilder wiring**
   - Add `PhysicalPlan::SlidingWindow` to builder and wire to `SlidingWindowProcessor`.
6. **Tests**
   - Add unit tests covering:
     - `slidingwindow('ss', 10)` emits on data without waiting for watermark.
     - `slidingwindow('ss', 10, 15)` does not emit until receiving deadline watermark.
     - Processing-time trimming assumptions (monotonic).
     - Event-time mode buffering (out-of-order) (may be minimal at first).
   - Maintain existing rewrite test under `#[ignore]`.
