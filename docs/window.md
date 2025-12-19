# Windows in StreamDialect

StreamDialect expresses stream windows as window functions that appear in the `GROUP BY` clause.

## Parsing Contract (parser crate)

- Window functions are only allowed in `GROUP BY`.
- At most one window function is allowed per statement.
- When a window function is present, the parser extracts it into `SelectStmt.window`.
- All other `GROUP BY` expressions remain in `SelectStmt.group_by_exprs`.

Example:

```sql
SELECT * FROM stream GROUP BY tumblingwindow('ss', 10), b, c
```

Parses into:

- `SelectStmt.window = Some(Window::Tumbling { ... })`
- `SelectStmt.group_by_exprs = [b, c]`

## Supported Window Functions

### `tumblingwindow(time_unit, length)`

Example:

```sql
SELECT * FROM stream GROUP BY tumblingwindow('ss', 10)
```

### `slidingwindow(time_unit, lookback [, lookahead])`

Example:

```sql
SELECT * FROM stream GROUP BY slidingwindow('ss', 10, 15)
```

### `countwindow(count)`

Example:

```sql
SELECT * FROM stream GROUP BY countwindow(3)
```

### `statewindow(open, emit)`

`open` and `emit` are general SQL expressions (typically boolean conditions):

- `open`: condition that opens (starts) the window
- `emit`: condition that triggers emission (sending) of the window state

Example:

```sql
SELECT * FROM stream GROUP BY statewindow(a > 0, b = 1)
```

