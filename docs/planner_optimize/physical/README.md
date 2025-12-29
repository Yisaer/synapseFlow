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
- `StreamingEncoderRewrite` (`streaming_encoder_rewrite`):
  [`streaming_encoder_rewrite.md`](streaming_encoder_rewrite.md)
