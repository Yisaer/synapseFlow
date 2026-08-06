# StateWindow in StreamDialect

This document describes `statewindow` semantics and syntax extensions.

See also: `docs/syntax/windows/README.md`, `docs/syntax/windows/syntax.md`,
`docs/syntax/windows/window_partitioning.md`, and `docs/syntax/windows/window_metadata.md`.

## Syntax

`statewindow` is a window function with two general SQL expressions (typically booleans):

```sql
statewindow(open_expr, emit_expr)
```

To maintain independent state per logical key, `statewindow` may include an `OVER` clause:

```sql
statewindow(open_expr, emit_expr) OVER (PARTITION BY key_expr1, key_expr2, ...)
```

### Restrictions

For `statewindow`, the `OVER` clause supports **only**:

- `PARTITION BY <expr> [, <expr> ...]`

`ORDER BY`, window frames, named windows, and other `OVER` features are not supported for
`statewindow` in StreamDialect.

## Semantics

`statewindow` buffers input rows between an "open" condition and an "emit" condition.

For a given partition (see below), the state machine is:

- When inactive and `open_expr == true`, start buffering (do not emit even if `emit_expr == true`).
- When active, buffer every incoming row. If `emit_expr == true`, emit the buffered batch and close (become inactive).
- When inactive and `emit_expr == true`, ignore.
- `window_start()` returns the processor wall-clock time when `open_expr` opens the state window.
- `window_end()` returns the processor wall-clock time when `emit_expr` closes and emits the state
  window.
- In event-time mode, these metadata values remain processor wall-clock lifecycle boundaries because
  `statewindow` is not an event-time interval.

### Partitioned Semantics (`OVER (PARTITION BY ...)`)

When `OVER (PARTITION BY ...)` is present:

- A **partition key** is computed per incoming row by evaluating all `key_expr*`.
- The operator maintains an independent `statewindow` state machine **per partition key**.
- `open_expr` / `emit_expr` are evaluated against the current row and apply only to that row’s partition.
- Rows from different partition keys never share buffered state.
- This partitioning happens before the state window decides whether the row opens, extends, or emits
  a window.

When `OVER` is absent:

- All rows belong to a single implicit partition (the entire stream shares one state machine).

### Relationship To `GROUP BY`

`OVER (PARTITION BY ...)` and regular `GROUP BY` keys are different stages.

- `statewindow(...) OVER (PARTITION BY key)` maintains independent state machines before a window is
  emitted.
- `GROUP BY key` groups rows after the state window emits a collection and aggregation starts.

For example:

```sql
SELECT device_id, count(*)
FROM users
GROUP BY statewindow(a > 0, b = 1), device_id
```

uses one global state machine, then groups the emitted state-window rows by `device_id`.

```sql
SELECT device_id, count(*)
FROM users
GROUP BY statewindow(a > 0, b = 1) OVER (PARTITION BY device_id)
```

uses one state machine per `device_id`. Rows from different devices cannot open, extend, or close
each other's state windows.

### Control / End-of-Stream

On graceful termination, if a partition is active and has buffered rows, the current buffered batch may be flushed before shutdown (matching the current processor behavior).

## Examples

### Global (single partition)

```sql
SELECT *
FROM users
GROUP BY statewindow(a > 0, b = 1)
```

### Partitioned by keys

```sql
SELECT *
FROM users
GROUP BY statewindow(a > 0, b = 1) OVER (PARTITION BY user_id)
```

```sql
SELECT *
FROM users
GROUP BY statewindow(a > 0, b = 1) OVER (PARTITION BY region, device_type)
```

## Error Cases (Expected)

- Multiple window functions in `GROUP BY` (e.g. `statewindow(...)` + `tumblingwindow(...)`) are rejected.
- Unsupported `OVER` features for `statewindow` are rejected (e.g. `OVER (ORDER BY ...)`, frames).
- `OVER ()` with no `PARTITION BY` expressions is rejected.
