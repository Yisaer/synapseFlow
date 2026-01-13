# Pipeline Event Time (Eventtime)

This document describes how veloFlux adds **event-time** support to the `flow` module and how watermark-driven window operators (e.g. `StreamingTumblingAggregationProcessor`) are advanced by explicit `StreamData::Watermark` messages.

## Background

Today the pipeline uses **processing time** by default:

- `Tuple.timestamp` is populated from the system clock when decoding/parsing input tuples.
- Time-aware operators (windows, streaming aggregations) are advanced by watermarks produced by the watermark stage.

To support **event time**, `Tuple.timestamp` must be derived from a column in the incoming data (not from the decode-time system clock). In event-time mode, input tuples may arrive **out of order** by timestamp. The watermark stage becomes responsible for:

1. inferring event-time progress from the observed tuple timestamps, and
2. emitting `StreamData::Watermark(ts)` to drive downstream window processors.

## Goals

- Allow a stream to declare which column represents event time and how to parse it.
- Make event-time parsing extensible via a runtime registry (`EventtimeTypeRegistry`).
- Split watermark logic into processing-time and event-time physical plans.
- In event-time mode, allow out-of-order input while ensuring downstream sees:
  - **monotonic watermarks**, and
  - tuples forwarded in **non-decreasing** `Tuple.timestamp` order (like "real time").

## Non-goals (initially)

- Distributed watermark alignment across multiple partitions/sources.
- Sophisticated late-event handling policies beyond a single default behavior.

## Configuration

### StreamDefinition: event time declaration

`StreamDefinition` gains an optional `eventtime` section:

- `eventtime.column`: the column name carrying event time.
- `eventtime.type`: the parsing strategy key (e.g. `unixtimestamp_s`, `unixtimestamp_ms`).

The value of `eventtime.type` is a **name**, not a format description. The actual parsing implementation is provided by the flow runtime (see registry below).

Validation requirements (planner-time preferred):

- `eventtime.column` must exist in the stream schema.
- `eventtime.type` must be registered in the runtime registry.
- The column value must be convertible to a timestamp using the selected parser; parsing failures are reported as `StreamData::Error` and do not terminate the pipeline.

### Pipeline options

The pipeline options gain:

- `eventtime.enabled: bool` (default `false`)
- `eventtime.lateTolerance: Duration` (default `0`)

Semantics:

- When `eventtime.enabled=false`, the pipeline behaves as processing time (today's behavior).
- When `eventtime.enabled=true`, the planner requires a valid `StreamDefinition.eventtime` configuration and builds an event-time watermark plan.

## Runtime extensibility: EventtimeTypeRegistry

`FlowInstance` exposes an `EventtimeTypeRegistry` to manage event-time parsing strategies:

- `register(name, parser)`: registers a new parsing strategy under `name`.
- `resolve(name) -> parser`: retrieves a registered parser (error if unknown).
- `list()`: exposes available type keys for diagnostics.

The flow runtime registers `unixtimestamp` by default.

Built-in types:

- `unixtimestamp_s`: parse Unix timestamp in seconds.
- `unixtimestamp_ms`: parse Unix timestamp in milliseconds.

## Physical planning changes

### Watermark node split

Replace the single `PhysicalWatermark` with two dedicated physical plan nodes:

- `PhysicalProcessTimeWatermark`
  - Uses a system-time ticker (or existing processing-time scheduling) to emit `StreamData::Watermark`.
  - Preserves current behavior.
- `PhysicalEventtimeWatermark`
  - Consumes input tuples (out of order by `Tuple.timestamp`) and emits watermarks inferred from event time.

### Planner selection

- If `eventtime.enabled=false`: insert `PhysicalProcessTimeWatermark` before window/aggregation operators that require time.
- If `eventtime.enabled=true`: insert `PhysicalEventtimeWatermark` before those operators.

Downstream processors (e.g. `StreamingTumblingAggregationProcessor`) advance event time only via received `StreamData::Watermark(ts)` and must not rely on a wall-clock ticker in event-time mode.

## PhysicalEventtimeWatermark design

### Inputs and outputs

- Input: `StreamData::Collection(Box<dyn Collection>)` where `collection.rows()` yields tuples (each tuple carries an event-time `Tuple.timestamp`).
- Output: a stream of `StreamData` items:
  - `StreamData::Collection(...)` (implemented as a `RecordBatch`) forwarding tuples downstream (in non-decreasing `Tuple.timestamp` order)
  - `StreamData::Watermark(SystemTime)` emitted to advance downstream event time
  - `StreamData::Error(...)` for non-fatal parsing/processing errors (pipeline continues)

Key contract:

- After emitting `StreamData::Watermark(W)`, the operator must never emit any tuple with `Tuple.timestamp <= W`.

### lateTolerance semantics

Event-time inputs are assumed **out of order**.

`lateTolerance` defines the tolerated disorder bound. Intuitively:

- The operator waits for late/out-of-order tuples within the tolerance window.
- Once event time has progressed enough, it emits a watermark and releases all buffered tuples that are now safe to order.

Late events definition:

- If a tuple arrives with `tuple.timestamp <= current_watermark`, it is considered **too late** (arrived beyond the tolerated bound).
- Default policy: drop the tuple and record metrics.

### Operator state

Maintain the following state per operator instance:

- `current_watermark: SystemTime` (monotonic, last emitted watermark)
- `max_timestamp_seen: SystemTime` (max event time observed so far)
- `buffer: min-heap` ordered by `(tuple.timestamp, seq)` for stable ordering
- `seq: u64` (monotonic arrival sequence to break ties)

### Processing algorithm (per input Collection)

1. **Ingest tuples (buffering + filtering)**
   - For each incoming tuple in `collection.rows()`:
     - Let `ts = tuple.timestamp`.
     - If `ts <= current_watermark`: treat as late (apply late policy), do not buffer.
     - Else:
       - `max_timestamp_seen = max(max_timestamp_seen, ts)`
       - push `(ts, seq, tuple)` into `buffer`, increment `seq`.

2. **Compute target watermark (data-driven, no ticker)**
   - `candidate = max_timestamp_seen - lateTolerance`
   - `target = max(current_watermark, candidate)` (never move backward)

3. **Emit in-order tuples up to target watermark**
   - If `target > current_watermark`:
     - Pop from `buffer` while `buffer.min_ts <= target`.
     - Collect popped tuples into an output list, preserving pop order.
     - Emit them as `StreamData::Collection(...)` (e.g. a row-based collection such as a `RecordBatch`).
     - This guarantees forwarded tuples are in non-decreasing `Tuple.timestamp` order.

4. **Emit watermark to drive downstream processors**
   - After all tuples with `timestamp <= target` are emitted, emit:
     - `StreamData::Watermark(target)`
   - Set `current_watermark = target`.

This ordering (`tuples` first, then `watermark`) preserves the watermark contract for downstream window operators.

### End-of-stream behavior (flush)

On end-of-stream / shutdown:

- Do not emit a special "MAX" watermark; termination is propagated via `ControlSignal`.
- Flush/close semantics are handled by processors reacting to end signals, not by an artificial final watermark.

### Properties

- **Monotonic watermarks**: `current_watermark` never decreases.
- **Ordered downstream tuples**: forwarded tuples are in non-decreasing `Tuple.timestamp` order.
- **Bounded disorder**: tuples that arrive within `lateTolerance` of the observed maximum can still be ordered and delivered; older tuples become late.

### Operational considerations

- Buffer memory grows with the amount of disorder and the chosen `lateTolerance`.
- Implement metrics:
  - `buffer_size`
  - `late_dropped_count`
  - `watermark_emitted_count`
  - `max_timestamp_seen`

## Implementation checklist

1. Extend `StreamDefinition` with `eventtime.column` and `eventtime.type`.
2. Add `eventtime.enabled` and `eventtime.lateTolerance` to pipeline options.
3. Introduce `EventtimeTypeRegistry` in `FlowInstance`, register `unixtimestamp_s` and `unixtimestamp_ms` by default.
4. Split `PhysicalWatermark` into `PhysicalProcessTimeWatermark` and `PhysicalEventtimeWatermark`.
5. Implement `PhysicalEventtimeWatermark` processor using `StreamData::Watermark` emission semantics described above.
6. Add tests covering:
   - processing-time mode regression (no behavior change),
   - event-time ordering + watermark advancement,
   - late events beyond `lateTolerance`,
   - unknown `eventtime.type` and missing `eventtime.column` validation.

## Message-scoped event time

`eventtime.column` is defined relative to a specific stream message (a `Message` inside a `Tuple`). Since a `Tuple` may contain multiple `Message`s, the tuple event time is determined by:

- selecting the first `Message` (in tuple order) whose stream definition includes an `eventtime` configuration, and
- parsing that message's `eventtime.column` using the selected `eventtime.type`.
