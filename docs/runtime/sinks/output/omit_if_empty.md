# Omit If Empty

## Background

Some sink branches should not publish anything when the final query result for that branch is
empty.

This is a sink-side delivery concern, not a SQL concern:

- the query still computes its normal relational result
- one sink branch may want empty results suppressed
- another sink branch of the same pipeline may still want normal delivery behavior

For this reason, `omit_if_empty` should not be modeled inside SQL expressions or connector-local
transport code.

## Decision

`omit_if_empty` should be modeled as a sink-side output policy under the existing sink `output`
block.

It should not be modeled as:

- a SQL clause
- an encoder option
- a connector option
- a rule folded into `RowDiff` itself

## Proposed API Shape

`omit_if_empty` lives under `sink.output`, alongside other sink output semantics such as
`output.mode=full|delta`.

Example without row diff:

```json
{
  "sinks": [
    {
      "id": "mqtt_out",
      "type": "mqtt",
      "props": {
        "broker_url": "tcp://127.0.0.1:1883",
        "topic": "result/demo"
      },
      "output": {
        "mode": "full",
        "omit_if_empty": true
      },
      "encoder": {
        "type": "json",
        "props": {}
      }
    }
  ]
}
```

Example with row diff:

```json
{
  "sinks": [
    {
      "id": "mqtt_delta",
      "type": "mqtt",
      "props": {
        "broker_url": "tcp://127.0.0.1:1883",
        "topic": "result/delta"
      },
      "output": {
        "mode": "delta",
        "delta": {
          "columns": ["speed", "rpm"]
        },
        "omit_if_empty": true
      },
      "encoder": {
        "type": "json",
        "props": {}
      }
    }
  ]
}
```

Minimal shape:

- `output.mode`
  - `full` (default)
  - `delta`
- `output.omit_if_empty`
  - `false` (default)
  - `true`

## Semantics

`omit_if_empty` checks whether the sink branch's final `Collection` is effectively empty.

When `output.omit_if_empty = true`:

- if the branch input collection has zero rows, the branch publishes nothing
- if the branch input collection has one or more rows but every tuple carries an `output_mask`
  whose bits are all `false`, the branch also publishes nothing
- otherwise, the branch continues normally

When `output.omit_if_empty = false`:

- the branch continues normally regardless of whether the collection is empty

This policy is intentionally collection-level. It may inspect row-diff runtime metadata that is
already present on the final branch output, but it does not inspect connector payload bytes and
does not depend on encoder-specific formatting behavior.

## Placement In The Sink Branch

The recommended sink-branch suffix is:

```text
Project
  -> RowDiff?
  -> EmptySuppress(omit_if_empty)
  -> Batch?
  -> Encoder(template)?
  -> DataSink
```

Why this order:

1. `RowDiff` should remain the first consumer of final projected values on delta branches.
2. `omit_if_empty` should observe the branch result after sink-side row shaping has completed,
   including any `output_mask` metadata produced by `RowDiff`.
3. `Batch`, `Encoder`, and `DataSink` should not be asked to reason about empty-result suppression.

## Why It Should Be After Row Diff

For `output.mode=delta`, row diff is already the sink-side consumer that owns the final
row-comparison semantics.

Keeping the order as:

```text
Project -> RowDiff -> EmptySuppress -> ...
```

has two benefits:

- it preserves the existing delayed-materialization endpoint on row-diff branches
- it keeps empty suppression aligned with the actual sink-facing branch output, including row-diff
  no-op batches where every `output_mask` bit is `false`

Placing `EmptySuppress` before `RowDiff` would make it an intermediate consumer between `Project`
and `RowDiff`, which is the wrong boundary for delta branches.

## Why It Should Be A Separate Plan Node

`omit_if_empty` should be a separate physical plan node, not merged into `PhysicalRowDiff`.

Reason:

1. `omit_if_empty` applies to both `output.mode=full` and `output.mode=delta`.
2. `RowDiff` remains a row-content transformation; `omit_if_empty` remains a branch delivery
   policy that may consume the resulting row-diff metadata.
3. Keeping them separate makes explain output and planner responsibilities clearer.

Recommended physical shape:

```text
Encoded-byte path:
PhysicalProject
  -> PhysicalRowDiff?
  -> PhysicalEmptySuppress
  -> PhysicalSinkEncoder
  -> PhysicalSinkConnector

Direct collection path:
PhysicalProject
  -> PhysicalRowDiff?
  -> PhysicalEmptySuppress
  -> PhysicalBatch?
  -> PhysicalDataSink
```

Recommended logical shape:

- keep `omit_if_empty` as sink output metadata on the logical sink node
- materialize it as `PhysicalEmptySuppress` during physical sink-branch construction

This avoids polluting the shared relational DAG with a sink-only policy.

## Relationship To Delayed Materialization

`omit_if_empty` should not change the delayed-materialization endpoint on row-diff branches.

The existing row-diff optimization target is:

```text
Project -> RowDiff -> ...
```

If `PhysicalEmptySuppress` is inserted after `PhysicalRowDiff`, that optimization boundary remains
unchanged:

```text
Project -> RowDiff -> EmptySuppress -> ...
```

Therefore:

- `omit_if_empty` should not block `ByIndexProjectionIntoRowDiffRewrite`
- `omit_if_empty` should not become the delayed-materialization endpoint

## Relationship To Encoder Templates

`omit_if_empty` is independent from `encoder.transform=template`.

Template rendering remains an encoder-local concern:

```text
... -> EmptySuppress -> Encoder(template) -> DataSink
```

This keeps responsibilities separate:

- `omit_if_empty`: decide whether the branch should continue
- `template`: decide how each row is formatted

`omit_if_empty` should not inspect rendered bytes and should not depend on template-specific
semantics.

## Runtime Behavior

`PhysicalEmptySuppress` should be executed by a dedicated sink-side processor, for example
`EmptySuppressProcessor`.

For each incoming `Collection`:

- if `omit_if_empty = true` and `collection.row_count() == 0`, drop the collection for this branch
- if `omit_if_empty = true`, the collection has one or more rows, and every tuple carries an
  `output_mask` whose bits are all `false`, also drop the collection for this branch
- otherwise, forward it unchanged

The processor should also forward control signals normally.

Suggested minimal stats:

- collections in
- collections forwarded
- collections suppressed

## Performance Notes

As a plan concept, `PhysicalEmptySuppress` should stay separate from `PhysicalRowDiff`.

However, runtime implementation is still free to optimize later:

- planner / explain may keep distinct `RowDiff` and `EmptySuppress` nodes
- the runtime may later fuse them into a single processor implementation if profiling shows the
  extra hop is material on hot sink branches

That optimization should be treated as an implementation detail. The plan semantics should remain
explicit.

## Non-Goals

This document does not define:

- payload-byte-level empty suppression
- connector-specific empty suppression behavior
- template-render-result-based suppression
- row dropping rules inside `RowDiff` itself

In particular, `RowDiff` should continue to mean row-diff transformation only. Empty-result
suppression remains a separate sink output policy.
