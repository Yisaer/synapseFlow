# tumblingwindow

`tumblingwindow(time_unit, length)` defines fixed, non-overlapping time windows.

See also: `docs/syntax/windows/syntax.md`, `docs/syntax/windows/window_metadata.md`, and
`docs/syntax/windows/watermarks.md`.

## Semantics

- Let `length` be a duration in `time_unit` (currently only `time_unit = 'ss'` is supported).
- Each tuple has a `timestamp` which acts as the time coordinate.
- Tuples are assigned to exactly one tumbling window by their timestamps.
- Window closure and emission are driven by incoming watermarks:
  - When a watermark advances past a window end boundary, that window is eligible to flush.
- `window_start()` returns the fixed logical bucket start.
- `window_end()` returns the fixed logical bucket end.
- In event-time mode, the logical bucket is computed from the tuple event timestamp.

## Example

```sql
SELECT user_id, sum(amount)
FROM payments
GROUP BY user_id, tumblingwindow('ss', 10);
```

In a watermark-driven execution, a 10-second window `[00:00:00, 00:00:10)` is flushed when the
upstream watermark reaches `00:00:10` (or later).
