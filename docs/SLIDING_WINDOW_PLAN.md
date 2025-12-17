# Sliding Window Plan

This document captures the requirements, design, and implementation steps for adding `slidingwindow(...)` support across parsing (step 1) and planning (step 2). Runtime execution (step 3, processors) is explicitly out of scope for this document and will be handled after step 1+2 code review.

## Requirements
- Support windowed aggregation syntax in `GROUP BY`:
  - `GROUP BY slidingwindow('ss', 10)`
  - `GROUP BY slidingwindow('ss', 10, 15)`
- Window triggers only when an input record is received.
- `slidingwindow('ss', 10)`:
  - On trigger at time `t`, emit a window containing records received within the last `10` seconds (relative to `t`).
- `slidingwindow('ss', 10, 15)`:
  - On trigger at time `t`, do not emit immediately.
  - Emit once at `t + 15s`, containing records received within `[t - 10s, t + 15s]`.
- Parser behavior:
  - The window function is stored in `SelectStmt.window`.
  - Non-window `GROUP BY` expressions remain in `SelectStmt.group_by_exprs` (e.g. `GROUP BY slidingwindow(...), col_a` keeps `col_a` only).
  - Enforce at most one window function in `GROUP BY` (consistent with existing window handling).
- Time unit management:
  - Time units are represented as an enum; for now only `ss` (seconds) is supported.
- Planning behavior:
  - Logical planning must carry the structured sliding window spec so downstream execution can implement it.
  - Event-time selection is out of scope for now (use processing-time semantics in execution later).

## Semantics Model
- Let `t` be the processing-time timestamp when a record is received and triggers a sliding window.
- Define:
  - `lookback = 10s` (second argument)
  - `lookahead = None` if omitted, else the third argument (e.g. `Some(15s)`)
- The window data range is `[t - lookback, t + lookahead]`.
- The window emission time is:
  - `t` when `lookahead` is omitted
  - `t + lookahead` when provided

## Step 1: Parser Changes

### AST representation
- Extend `src/parser/src/window.rs`:
  - Add `Window::Sliding { time_unit: TimeUnit, lookback: u64, lookahead: Option<u64> }`.
  - Reuse the existing `TimeUnit` enum; keep only `Seconds` for now.
- `SelectStmt.window` already stores `Option<Window>`; no schema changes needed in `SelectStmt`.

### Parsing rules
- Extend window function recognition:
  - Add `slidingwindow` to the supported function list.
  - Parse either 2 or 3 arguments:
    - `slidingwindow(time_unit, lookback)`
    - `slidingwindow(time_unit, lookback, lookahead)`
- Validate:
  - `time_unit` must be a string literal (single- or double-quoted).
  - `lookback` and `lookahead` must be unsigned integer literals.
  - Reject any other arity.
  - Time unit parsing uses `TimeUnit` conversion; error messages should reference the correct function name (avoid tumbling-only wording).

### Group-by splitting behavior
- No behavior change needed: `src/parser/src/dialect.rs` already splits `GROUP BY` expressions into `(SelectStmt.window, SelectStmt.group_by_exprs)` by detecting a supported window function.

### Parser tests
- Add/extend unit tests:
  - `src/parser/src/window.rs`: parsing `slidingwindow('ss', 10)` and `slidingwindow('ss', 10, 15)` into `Window::Sliding`.
  - `src/parser/src/dialect.rs`: ensure `GROUP BY slidingwindow('ss', 10), b` stores the window and keeps `b` as a non-window group key.
  - Negative cases:
    - wrong arg count
    - non-literal arguments
    - unsupported unit

## Step 2: Planner Changes (Logical + Physical Representation)

### Logical window spec
- Extend `src/flow/src/planner/logical/window.rs`:
  - Add `LogicalWindowSpec::Sliding { time_unit: TimeUnit, lookback: u64, lookahead: Option<u64> }`.
- Update `src/flow/src/planner/logical/mod.rs`:
  - Extend `convert_window_spec(...)` to map parser `Window::Sliding` to logical `LogicalWindowSpec::Sliding`.

### Explain output
- Update `src/flow/src/planner/explain.rs` to format `LogicalWindowSpec::Sliding` in logical plan explanations.

### Physical planning (representation only)
Two options exist; pick one before implementation:

**Option A (recommended): add a physical node now**
- Add `PhysicalSlidingWindow` (e.g. in `src/flow/src/planner/physical/physical_window.rs`) and a `PhysicalPlan::SlidingWindow` variant.
- Update `src/flow/src/planner/physical/mod.rs` to include the new variant in `children()`, `get_plan_type()`, and `get_plan_index()`.
- Update `src/flow/src/planner/physical_plan_builder.rs` to convert `LogicalWindowSpec::Sliding` into `PhysicalPlan::SlidingWindow`, inserting a `PhysicalWatermark` node before the window (mirroring tumbling windows).
- This makes the planner fully represent sliding windows end-to-end (parse → logical → physical) even before processors exist.
- Execution will still fail until step 3; in the interim, processor building should return a clear "not supported yet" error for `SlidingWindow`.
- Optimization (deferred): `Aggregation(Window(Sliding))` can later become eligible for `streaming_aggregation_rewrite`, but the rewrite is disabled until sliding streaming aggregation execution is implemented.

**Option B: stop at logical planning for now**
- Do not add a new physical plan node.
- In `src/flow/src/planner/physical_plan_builder.rs`, return an explicit error when encountering `LogicalWindowSpec::Sliding` (e.g. "slidingwindow physical planning not implemented yet").
- This avoids any processor-layer changes, but defers physical representation to step 3.

### Planner tests
- Add unit tests under existing patterns in `src/flow/src/planner/logical/mod.rs` tests:
  - `SELECT * FROM stream GROUP BY slidingwindow('ss', 10)` creates a `LogicalPlan::Window` with `LogicalWindowSpec::Sliding`.
  - `GROUP BY slidingwindow('ss', 10), b` preserves `b` as a group key and still creates the window node.

## Out of Scope (Step 3)
- Implementing actual sliding-window buffering, delayed emission, and trigger behavior in processors.
- Integrating any event-time column selection or watermark semantics specific to sliding windows.
