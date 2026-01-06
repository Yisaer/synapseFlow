# Windowing

This document describes the supported window declarations in SynapseFlow SQL.

## Placement rules

- A window declaration is written inside `GROUP BY`.
- At most one window declaration is allowed in a query.
- `GROUP BY` may also include additional non-window key expressions.

## Tumbling window

```sql
... GROUP BY tumblingwindow('ss', <length>)
```

- Fixed-size, non-overlapping time buckets.
- Time unit is currently `'ss'` (seconds).

Example:

```sql
SELECT * FROM s GROUP BY tumblingwindow('ss', 10)
```

## Sliding window

```sql
... GROUP BY slidingwindow('ss', <lookback> [, <lookahead>])
```

- Overlapping moving time ranges.
- When `<lookahead>` is omitted, the window is emitted immediately at the trigger time.

Example:

```sql
SELECT * FROM s GROUP BY slidingwindow('ss', 10, 15)
```

## Count window

```sql
... GROUP BY countwindow(<count>)
```

- Groups by record count rather than time.

Example:

```sql
SELECT * FROM s GROUP BY countwindow(100)
```

## State window

```sql
... GROUP BY statewindow(<open_expr>, <emit_expr>)
... GROUP BY statewindow(<open_expr>, <emit_expr>) OVER (PARTITION BY <expr> [, <expr> ...])
```

- Dynamic segments driven by two boolean expressions:
  - `<open_expr>` decides when a segment starts collecting state.
  - `<emit_expr>` decides when to emit/close the segment.
- `OVER` only supports `PARTITION BY` (no `ORDER BY`, no window frame).

Examples:

```sql
SELECT * FROM s GROUP BY statewindow(a > 0, b = 1)
SELECT * FROM s GROUP BY statewindow(a > 0, b = 1) OVER (PARTITION BY device_id)
```

