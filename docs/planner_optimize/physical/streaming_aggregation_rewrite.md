# StreamingAggregationRewrite

## Overview

`StreamingAggregationRewrite` is a physical-plan rewrite that fuses:

```
Window -> Aggregation
```

into a single streaming operator:

```
StreamingAggregation
```

The goal is to avoid intermediate buffering/materialization between windowing and aggregation
when aggregate updates can be applied incrementally per tuple.

Implementation: `src/flow/src/planner/optimizer.rs` (`StreamingAggregationRewrite`).

## Inputs (What It Recognizes)

This rule matches `PhysicalPlan::Aggregation(agg)` where the aggregation’s (first) child is one of:

- `PhysicalPlan::TumblingWindow`
- `PhysicalPlan::CountWindow`
- `PhysicalPlan::SlidingWindow`
- `PhysicalPlan::StateWindow`

## Preconditions (When It Is Safe)

The rewrite is applied only when **all aggregate calls are incremental**:

- `PhysicalAggregation::all_calls_incremental(&agg.aggregate_calls, &AggregateFunctionRegistry)`

If any call is non-incremental, the plan is left unchanged.

## Outputs (What It Produces)

When the match and preconditions hold, the rule replaces the `Window -> Aggregation` chain with
`PhysicalPlan::StreamingAggregation(PhysicalStreamingAggregation)`:

- `StreamingWindowSpec` is derived from the window node:
  - `Tumbling { time_unit, length }`
  - `Count { count }`
  - `Sliding { time_unit, lookback, lookahead }`
  - `State { open_expr, emit_expr, partition_by_exprs, ... }`
- The fused node inherits aggregation fields:
  - `aggregate_calls`, `aggregate_mappings`
  - `group_by_exprs`, `group_by_scalars`
- The fused node’s child becomes the window’s original upstream child (i.e. the window node is
  removed from the topology).
- The fused node keeps the aggregation node’s plan index (`agg.base.index()`), which stabilizes
  processor IDs and explain output across the rewrite.

## Runtime Mapping

At runtime, `PhysicalStreamingAggregation` is executed by `StreamingAggregationProcessor` and
dispatched by `StreamingWindowSpec`:

- `Count` → `StreamingCountAggregationProcessor`
- `Tumbling` → `StreamingTumblingAggregationProcessor`
- `Sliding` → `StreamingSlidingAggregationProcessor`
- `State` → `StreamingStateAggregationProcessor`

See `src/flow/src/processor/streaming_aggregation_processor.rs` and the concrete processor
implementations under `src/flow/src/processor/`.

## Explain / Tests

The rewrite is covered by table-driven planner explain tests in:

- `src/flow/tests/planner/physical/plan_explain_table_driven.rs`

Example cases include:

- `stateful_before_window_and_aggregation`
- `optimize_rewrites_streaming_agg_for_state_window`
- `explain_ndv_countwindow_non_incremental` (negative case)

## Limitations

- Only rewrites when the aggregation is **directly** on top of a supported window node.
- Only checks the aggregation’s first child (aggregations are expected to be unary in the
  current physical plan).

