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

- `StreamingEncoderRewrite` (`streaming_encoder_rewrite`):
  [`streaming_encoder_rewrite.md`](streaming_encoder_rewrite.md)
- `StreamingAggregationRewrite` (`streaming_aggregation_rewrite`):
  [`streaming_aggregation_rewrite.md`](streaming_aggregation_rewrite.md)
- `InsertBarrierForFanIn` (`insert_barrier_for_fan_in`):
  [`../../../runtime/processors/barrier_signal.md`](../../../runtime/processors/barrier_signal.md)

Final sink value addressing is derived as an `OutputLayout` during physical
planning rather than implemented as an optimization rule. See
[`../../performance/plan_fixed_output_slots.md`](../../performance/plan_fixed_output_slots.md).
