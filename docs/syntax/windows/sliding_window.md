# slidingwindow

`slidingwindow(time_unit, lookback [, lookahead])` defines per-row triggered time ranges.

See also: `docs/syntax/windows/syntax.md`, `docs/syntax/windows/window_metadata.md`,
`docs/syntax/windows/watermarks.md`, and `docs/syntax/windows/sliding_window_rfc.md`.

## Parameters

- `time_unit`: string literal. Supported units are `'ms'` for milliseconds and `'ss'` for seconds.
- `lookback`: unsigned integer literal (duration in `time_unit`).
- `lookahead`: optional unsigned integer literal (duration in `time_unit`).

## Syntax

`slidingwindow(...)` can be followed by `OVER (WHEN <expr>)` to restrict which
input tuples create emission requests:

```sql
GROUP BY slidingwindow('ss', 10, 5) OVER (WHEN flag > 0)
```

`WHEN` can be combined with `PARTITION BY` in the same `OVER` clause:

```sql
GROUP BY slidingwindow('ss', 10, 5) OVER (WHEN flag > 0 PARTITION BY vehicle_id)
```

## Semantics

Each incoming tuple is a trigger point with timestamp `t`. `lookback` and `lookahead` are measured
in `time_unit` (`ms` for milliseconds, `ss` for seconds):

- `slidingwindow(time_unit, lookback)`:
  - range: `[t - lookback, t]`
  - emission: immediate (on receiving the trigger tuple)
  - `window_start()`: `t - lookback`
  - `window_end()`: `t`
- `slidingwindow(time_unit, lookback, lookahead)`:
  - range: `[t - lookback, t + lookahead]`
  - emission: delayed until the operator observes a watermark `>= t + lookahead`
  - `window_start()`: `t - lookback`
  - `window_end()`: `t + lookahead`

A millisecond window uses the same boundaries and watermark rules:

```sql
GROUP BY slidingwindow('ms', 100, 50) OVER (WHEN flag > 0)
```

In event-time mode, `t` is the trigger tuple event timestamp.

### Trigger condition

When `OVER (WHEN <expr>)` is present, `<expr>` is evaluated for each input tuple.
Only tuples for which the expression evaluates to `true` create a sliding window
emission request. Tuples for which the expression evaluates to `false` or `null`
remain in the row buffer and can be included by later triggered windows.

The trigger condition is orthogonal to `PARTITION BY`: each partition evaluates
the condition on its own input tuples and maintains its own pending sliding
windows.

### Watermark contract for lookahead

Delayed emission is entirely watermark-driven. The upstream pipeline must eventually produce
watermarks that reach each trigger's deadline (`t + lookahead`):

- In processing-time mode, `SlidingWatermarkProcessor` emits periodic processing-time watermarks
  (tick interval is `1s`) and, when `lookahead` is present, generates per-tuple deadline
  processing-time watermarks.
- In event-time mode, the deadline watermark behavior is controlled by the upstream event-time
  watermark stream.
