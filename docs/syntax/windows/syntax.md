# Window Syntax

StreamDialect introduces windowed aggregation as special functions that appear inside `GROUP BY`
only. The parser recognizes these functions and records the single allowed window on
`SelectStmt.window`.

## General Rules

- Window functions are only allowed in `GROUP BY` (not in projections or filters).
- Only one window function is allowed per statement.
- Window function names are case-insensitive.
- Non-window `GROUP BY` expressions are preserved in `SelectStmt.group_by_exprs`.

## Supported Window Functions

- `tumblingwindow(<time_unit>, <length>)` — fixed, non-overlapping time windows.
- `slidingwindow(<time_unit>, <lookback> [, <lookahead>])` — per-row triggered sliding windows.
- `countwindow(<count>)` — fixed windows measured by number of rows.
- `statewindow(<open_expr>, <emit_expr>) [OVER (PARTITION BY <expr> [, <expr> ...])]` — stateful
  open/emit window.
- `eoswindow()` — bounded table-scan window that closes when the source reaches end-of-stream.

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
