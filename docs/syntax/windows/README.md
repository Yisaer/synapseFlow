# StreamDialect Windows

StreamDialect expresses stream windows and bounded table-scan windows as window functions that appear
in the `GROUP BY` clause.

This directory documents:
- Syntax rules and parser behavior: `docs/syntax/windows/syntax.md`
- Window partitioning semantics: `docs/syntax/windows/window_partitioning.md`
- SQL-visible window metadata semantics: `docs/syntax/windows/window_metadata.md`
- Window semantics: `docs/syntax/windows/tumbling_window.md`, `docs/syntax/windows/sliding_window.md`,
  `docs/syntax/windows/count_window.md`, `docs/syntax/windows/state_window.md`
- Watermark-driven execution model: `docs/syntax/windows/watermarks.md`
- Sliding window RFC / implementation status: `docs/syntax/windows/sliding_window_rfc.md`

## Parser Contract

- Window functions are only allowed in `GROUP BY`.
- At most one window function is allowed per statement.
- When present, the parser extracts the window into `SelectStmt.window`.
- All other `GROUP BY` expressions remain in `SelectStmt.group_by_exprs`.
- Window-level `OVER (PARTITION BY ...)` expressions belong to the window specification. They are
  distinct from regular `GROUP BY` expressions because they affect window formation before
  aggregation.
- `eoswindow()` has no arguments and marks a bounded table scan window that closes at
  end-of-stream. Planner validation decides whether the source is eligible.

Example:

```sql
SELECT * FROM stream GROUP BY tumblingwindow('ss', 10), b, c
```

After parsing:
- `SelectStmt.window = Some(Window::Tumbling { ... })`
- `SelectStmt.group_by_exprs = [b, c]`

```sql
SELECT count(*) FROM stream GROUP BY statewindow(a > 0, b = 1) OVER (PARTITION BY device_id)
```

After parsing:
- `SelectStmt.window = Some(Window::State { partition_by: [device_id], ... })`
- `SelectStmt.group_by_exprs = []`

```sql
SELECT sum(a) FROM history_table GROUP BY eoswindow()
```

After parsing:
- `SelectStmt.window = Some(Window::Eos { ... })`
- `SelectStmt.group_by_exprs = []`
