# Physical Optimizations

This directory documents **physical-plan optimization rules** applied by the flow planner.

Implementation entrypoint: `src/flow/src/planner/optimizer.rs` (`optimize_physical_plan`).

## How Physical Optimization Works

- The optimizer applies a **fixed sequence** of rules (currently one pass per rule).
- Each rule performs a **bottom-up tree rewrite**: optimize children first, then attempt to
  rewrite the current node.
- Physical rules are allowed to consult registries (e.g. encoder capabilities, aggregate
  function properties) to decide whether a rewrite is semantically safe.

## Rules

- `StreamingAggregationRewrite` (`streaming_aggregation_rewrite`):
  [`streaming_aggregation_rewrite.md`](streaming_aggregation_rewrite.md)
- `ByIndexProjectionAcrossMixedConsumersRewrite`
  (`by_index_projection_across_mixed_consumers_rewrite`):
  [`by_index_projection_across_mixed_consumers_rewrite.md`](by_index_projection_across_mixed_consumers_rewrite.md)
- `PartialByIndexRowDiffAndEncoderRewrite`
  (`partial_by_index_row_diff_and_encoder_rewrite`):
  [`partial_by_index_row_diff_and_encoder_rewrite.md`](partial_by_index_row_diff_and_encoder_rewrite.md)
- `ByIndexProjectionIntoRowDiffRewrite` (`by_index_projection_into_row_diff_rewrite`):
  [`by_index_projection_into_row_diff_rewrite.md`](by_index_projection_into_row_diff_rewrite.md)
- `ByIndexProjectionIntoEncoderRewrite` (`by_index_projection_into_encoder_rewrite`):
  [`by_index_projection_into_encoder_rewrite.md`](by_index_projection_into_encoder_rewrite.md)
- `InsertBarrierForFanIn` (`insert_barrier_for_fan_in`):
  [`../../../runtime/processors/barrier_signal.md`](../../../runtime/processors/barrier_signal.md)
