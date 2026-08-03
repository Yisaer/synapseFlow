# countwindow

`countwindow(count)` defines fixed windows measured by number of rows (tuples), not by time.

See also: `docs/syntax/windows/syntax.md` and `docs/syntax/windows/window_metadata.md`.

## Semantics

- Let `count` be a positive integer.
- The operator groups the input stream into successive batches of `count` tuples.
- Emission is triggered by data arrival (every `count` tuples), not by watermarks.
- `window_start()` returns the processor wall-clock time when the count window instance opens.
- `window_end()` returns the processor wall-clock time when the `count`th row completes the window
  and triggers emission.
- In event-time mode, these metadata values remain processor wall-clock lifecycle boundaries because
  `countwindow` is not an event-time interval.

## Example

```sql
SELECT avg(price)
FROM quotes
GROUP BY countwindow(500);
```
