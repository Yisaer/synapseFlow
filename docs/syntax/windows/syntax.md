# Window Syntax

StreamDialect introduces windowed aggregation as special functions that appear inside `GROUP BY`
only. The parser recognizes these functions and records the single allowed window on
`SelectStmt.window`.

## General Rules

- Window functions are only allowed in `GROUP BY` (not in projections or filters).
- Only one window function is allowed per statement.
- Window function names are case-insensitive.
- Non-window `GROUP BY` expressions are preserved in `SelectStmt.group_by_exprs`.
- Window-level `OVER (PARTITION BY ...)` expressions belong to the window spec and affect window
  formation before aggregation. Regular `GROUP BY` keys are applied after a window has been formed.
  See `docs/syntax/windows/window_partitioning.md`.

## Supported Window Functions

- `tumblingwindow(<time_unit>, <length>)` — fixed, non-overlapping time windows.
- `slidingwindow(<time_unit>, <lookback> [, <lookahead>])` — per-row triggered sliding windows.
- `countwindow(<count>)` — fixed windows measured by number of rows.
- `statewindow(<open_expr>, <emit_expr>) [OVER (PARTITION BY <expr> [, <expr> ...])]` — stateful
  open/emit window.
- `eoswindow()` — bounded table-scan window that closes when the source reaches end-of-stream.

Target window partitioning support extends `OVER (PARTITION BY ...)` to `tumblingwindow`,
`countwindow`, and `slidingwindow`. Current runtime support is limited to `statewindow` until the
parser, planner, and processor rollout is complete.

## Window Metadata Functions

SQL-visible window metadata functions are documented in
`docs/syntax/windows/window_metadata.md`.

- `window_start()` exposes the start boundary metadata for the current logical window emission.
- `window_end()` exposes the end boundary metadata for the current logical window emission.
- Time windows use logical time. In event-time mode, that logical time is the tuple event time.
- Non-time windows use processor lifecycle wall-clock time for the window instance.

## Parameter Rules

For `tumblingwindow`, `slidingwindow`, and `countwindow`:
- Arguments must be literals.
- `time_unit`: string literal (both single- and double-quoted strings are accepted).
- `length`, `lookback`, `lookahead`, `count`: unsigned integer literals.
- Currently only `time_unit = 'ss'` is supported.

For `statewindow`:
- `open_expr` and `emit_expr` are general SQL expressions (typically boolean conditions).
- `OVER` is optional. When present, it supports **only** `PARTITION BY <expr> [, <expr> ...]`.
- `ORDER BY`, window frames, named windows, and other `OVER` features are not supported.

For target general window partitioning:
- `OVER (PARTITION BY <expr> [, <expr> ...])` creates independent window state per partition key.
- The partition key is evaluated before rows are assigned to count, time, sliding, or state windows.
- Partition keys are not projected automatically. Select them explicitly when they are needed in the
  output.
- Regular `GROUP BY` keys still group aggregate results inside each emitted window.

For `eoswindow`:
- No arguments are accepted.
- `OVER` is not supported.
- It is intended for bounded table scans. Source eligibility validation belongs to flow planning and
  is implemented with EOS window planning.
- It can be mixed with regular grouping keys to aggregate the whole scan per key.

## Filter Stages

Window functions may use SQL function `FILTER (WHERE ...)` syntax. The filter stages are:

- `window(...) FILTER (WHERE ...)`: filters rows while collecting the window.
- `HAVING`: filters aggregate results after windowing and aggregation.
- `WHERE`: filters rows in the final filter stage after windowing and aggregation.

## Examples

```sql
-- Time-based tumbling window of 10 seconds
SELECT * FROM stream GROUP BY tumblingwindow('ss', 10);

-- Count-based window over every 500 rows
SELECT avg(price) FROM stream GROUP BY countwindow(500);

-- Sliding window with delayed emission
SELECT * FROM stream GROUP BY slidingwindow('ss', 10, 15);

-- Mixing regular group keys with the single window
SELECT user_id, sum(amount)
FROM payments
GROUP BY user_id, tumblingwindow('ss', 10);

-- Target semantics: partitioned count windows form independently per user_id
SELECT user_id, count(*)
FROM payments
GROUP BY countwindow(500) OVER (PARTITION BY user_id);

-- Target semantics: window partitioning and aggregation grouping are separate stages
SELECT region, count(*)
FROM payments
GROUP BY tumblingwindow('ss', 10) OVER (PARTITION BY user_id), region;

-- Partitioned state window
SELECT *
FROM users
GROUP BY statewindow(a > 0, b = 1) OVER (PARTITION BY user_id);

-- End-of-stream window over a bounded table scan
SELECT device_id, sum(bytes)
FROM history_table
WHERE region = 'west'
GROUP BY device_id, eoswindow() FILTER (WHERE bytes > 0);
```
