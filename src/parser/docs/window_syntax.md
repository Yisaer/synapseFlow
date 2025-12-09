# StreamDialect Window Syntax

StreamDialect introduces windowed aggregation as special functions that appear inside `GROUP BY` only. The parser recognizes these functions and records the single allowed window on `SelectStmt.window` so downstream planning can reason about time- or count-based accumulation.

## Supported window functions
- `tumblingwindow(<time_unit>, <length>)` — fixed, non-overlapping time windows.
- `countwindow(<count>)` — fixed windows measured by number of rows.

Function names are case-insensitive. Arguments must be literals.

## Parameters
- `time_unit`: string literal such as `'ss'`, `'ms'`, `'m'`, etc. Both single- and double-quoted strings are accepted.
- `length`: unsigned integer literal describing how many `time_unit` units form one tumbling window.
- `count`: unsigned integer literal describing how many rows form one count window.

## Cardinality rule
Only one window function is allowed per `GROUP BY`. If multiple window functions are present, parsing fails. Regular grouping expressions may still appear alongside the single window; the parser stores that window separately (`SelectStmt.window`) and keeps only the non-window group keys in `SelectStmt.group_by_exprs`.

## Syntax and examples
```sql
-- Time-based tumbling window of 10 seconds
SELECT * FROM stream GROUP BY tumblingwindow('ss', 10);

-- Count-based window over every 500 rows
SELECT avg(price) FROM stream GROUP BY countwindow(500);

-- Mixing regular group keys with the single window
SELECT user_id, sum(amount)
FROM payments
GROUP BY user_id, tumblingwindow('m', 5);

-- After parsing:
--   window == Some(Tumbling { time_unit: "m", length: 5 })
--   group_by_exprs == ["user_id"]
```

Only `GROUP BY` may contain window functions; they are not treated as regular scalar functions in projections or filters. The recognized window is surfaced in `SelectStmt.window` alongside the raw `group_by_exprs` for consumers that need both the parsed expressions and the structured window definition.
