# Window Partitioning

This document defines the target semantics for window-level `OVER (PARTITION BY ...)`
support and how it differs from regular `GROUP BY` keys.

Implementation status:

- Current runtime support exists for `statewindow(...) OVER (PARTITION BY ...)`.
- General window partitioning for `tumblingwindow`, `countwindow`, and `slidingwindow` is planned
  and should be implemented in three steps: parser, planner, processor.
- `eoswindow()` remains unpartitioned by `OVER` unless a separate bounded-scan requirement is
  introduced.

## Two Independent Key Stages

Window partition keys and aggregation group keys run at different stages:

- `window(...) OVER (PARTITION BY key)` partitions the input stream before window formation.
- `GROUP BY key` partitions rows after a window has already been formed, during aggregation.

These two key lists must not be merged. They may use the same key-evaluation utility, but they
belong to different operators and different lifecycle stages.

## Example

For this input stream:

```text
(k=A, v=1)
(k=B, v=1)
(k=A, v=1)
(k=A, v=1)
```

Regular grouping:

```sql
SELECT k, count(*)
FROM stream
GROUP BY countwindow(3), k;
```

The count window is formed over the whole input stream first:

```text
window#1 = [A, B, A]
```

Then aggregation groups rows inside that already-formed window:

```text
A -> 2
B -> 1
```

Window partitioning:

```sql
SELECT k, count(*)
FROM stream
GROUP BY countwindow(3) OVER (PARTITION BY k);
```

The stream is partitioned before the count window state is updated:

```text
partition A: [A, A, A] -> emits one count window
partition B: [B]       -> does not emit yet
```

The output is therefore different:

```text
A -> 3
```

## Parser Step

The parser should extract window partition keys from the window function's `OVER` clause and keep
them on the window specification.

Target parser contract:

- The single window function still lives in `SelectStmt.window`.
- Non-window `GROUP BY` expressions still live in `SelectStmt.group_by_exprs`.
- `OVER (PARTITION BY ...)` expressions live on the parsed window spec, not in
  `SelectStmt.group_by_exprs`.
- `ORDER BY`, window frames, named windows, and empty `OVER ()` remain unsupported for window
  partitioning.

This separation preserves the semantic distinction:

```text
SelectStmt.window.partition_by_exprs  -> pre-window partitioning
SelectStmt.group_by_exprs             -> post-window aggregation grouping
```

## Planner Step

Logical and physical plans should keep separate fields for the two key stages.

Logical shape:

```text
Source -> Window(partition_by=...) -> Aggregation(group_by=...) -> Project
```

Physical shape without fusion:

```text
PhysicalSource
  -> PhysicalWindow(partition_by=...)
  -> PhysicalAggregation(group_by=...)
```

Physical shape with streaming aggregation fusion:

```text
PhysicalSource
  -> PhysicalStreamingAggregation(
       window_partition_by=...,
       group_by=...
     )
```

The fusion rule may remove the explicit physical window node, but it must not merge
`window_partition_by` into `group_by`. The fused processor must still evaluate window partition keys
before updating window state, and aggregation group keys only inside the selected partition-window.

## Processor Step

Processors should model the two stages as nested state, not as one combined group key.

For unfused execution:

```text
row
  -> evaluate window partition key
  -> update that partition's window state
  -> emit one collection for the closed partition-window
  -> aggregation evaluates GROUP BY keys inside that collection
```

For fused streaming aggregation:

```text
row
  -> evaluate window partition key
  -> locate/create that partition's window state
  -> update the selected window
  -> aggregation worker evaluates GROUP BY keys inside that window
```

The key evaluation and hash-key representation should be shared by `GROUP BY` and
`OVER (PARTITION BY ...)`, but the call sites remain separate. Prefer a structured key such as a
`Vec<Value>` wrapper over formatted debug strings so hot paths avoid extra allocation and formatting
overhead.

## Validation Notes

Test coverage should include both incremental and non-incremental aggregation paths:

- Incremental/fused path: supported aggregate functions such as `sum`, `count`, `avg`.
- Non-incremental/unfused path: aggregate functions that force `Window -> Aggregation` to remain
  separate.

Planner explain output should show both key lists when both are present, for example:

```text
Window: partition_by=[k]
Aggregation: group_by=[region]
```

or, after fusion:

```text
PhysicalStreamingAggregation: window=count, partition_by=[k], group_by=[region]
```
