# slidingwindow

`slidingwindow(time_unit, lookback [, lookahead])` defines per-row triggered time ranges.

See also: `docs/syntax/windows/syntax.md`, `docs/syntax/windows/window_metadata.md`,
`docs/syntax/windows/watermarks.md`, and `docs/syntax/windows/sliding_window_rfc.md`.

## Parameters

- `time_unit`: string literal (currently only `'ss'` is supported).
- `lookback`: unsigned integer literal (duration).
- `lookahead`: optional unsigned integer literal (duration).

## Semantics

Each incoming tuple is a trigger point with timestamp `t`:

- `slidingwindow('ss', lookback)`:
  - range: `[t - lookback, t]`
  - emission: immediate (on receiving the trigger tuple)
  - `window_start()`: `t - lookback`
  - `window_end()`: `t`
- `slidingwindow('ss', lookback, lookahead)`:
  - range: `[t - lookback, t + lookahead]`
  - emission: delayed until the operator observes a watermark `>= t + lookahead`
  - `window_start()`: `t - lookback`
  - `window_end()`: `t + lookahead`

In event-time mode, `t` is the trigger tuple event timestamp.

### Watermark contract for lookahead

Delayed emission is entirely watermark-driven. The upstream pipeline must eventually produce
watermarks that reach each trigger's deadline (`t + lookahead`):

- In processing-time mode, `SlidingWatermarkProcessor` emits periodic processing-time watermarks
  (tick interval is `1s`) and, when `lookahead` is present, generates per-tuple deadline
  processing-time watermarks.
- In event-time mode, the deadline watermark behavior is controlled by the upstream event-time
  watermark stream.
